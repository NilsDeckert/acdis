use crate::db_actor::message::DBRequest;
use crate::db_actor::AHasher;
use crate::hash_slot::hash_slot::HashSlot;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::parse_actor::parse_request_message::ParseRequestMessage;
use log::{debug, error, info, warn};
use ractor::{async_trait, call, cast, pg, Actor, ActorProcessingErr, ActorRef};
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::commands::parse::Request;
use redis_protocol_bridge::util::convert::SerializableFrame;
use std::hash::Hasher;

pub struct ParseRequestActor;

#[async_trait]
impl Actor for ParseRequestActor {
    type Msg = ParseRequestMessage;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        debug!("Spawning parsing actor...");
        Ok(())
    }

    /// Given an [`OwnedFrame`], parse it to a [`Request`] and forward that request to a
    /// [`actor::DBActor`]
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Handling parse request");
        let query = redis_protocol_bridge::parse_owned_frame(message.frame.0);
        let request = redis_protocol_bridge::commands::parse::parse(query);

        let reply_to_vec = pg::get_members(&message.reply_to);
        assert_eq!(
            reply_to_vec.len(),
            1,
            "Found less than or more than one actor for {}",
            message.reply_to
        );
        let reply_to = reply_to_vec.into_iter().next().unwrap();

        match request {
            Err(err) => {
                error!("{}", err.details().to_string());
                let err = OwnedFrame::SimpleError {
                    data: err.details().to_string(),
                    attributes: None,
                };

                Ok(reply_to.send_message(SerializableFrame(err))?)
            }

            Ok(request) => {
                // let responsible = self.find_responsible(&request).await?;
                // TODO: Ensure that we send requests to our own node manager
                let resp = pg::get_local_members(&String::from("acdis_node_managers"))
                    .into_iter()
                    .next()
                    .unwrap();
                let responsible = ActorRef::from(resp);
                info!("Request: {:?}", request);

                if let Request::SET { key, .. } = &request {
                    let hash = ParseRequestActor::hash(&key);
                    info!("{:#018x}", hash);
                }

                cast!(
                    responsible,
                    NodeManagerMessage::Forward(DBRequest {
                        request,
                        reply_to: message.reply_to
                    })
                )?;
                Ok(())
            }
        }
    }
}

impl ParseRequestActor {
    /// Return the* responsible node manager for a given request.
    ///
    /// \*For Requests that concern a specific key, the responsible actor is found by hashing the key.
    /// Requests like PING can be handled by multiple actors, thus any actor might be returned.
    ///
    /// # Arguments
    ///
    /// * `request`: [`Request`] that needs to be handled.
    ///
    /// returns: Result<ActorRef<NodeManagerMessage>, Box<dyn Error+Send+Sync, Global>>
    ///
    /// # Implementation
    ///
    /// Currently, this is done somewhat inefficiently by querying every node manager and asking whether it is
    /// responsible for hash of the given key.
    /// By querying node managers instead of DBActors we restrict the effort a bit.
    ///
    /// Possible ways to improve this include:
    ///  - Keep a some sort of data structure that maps keys to responsible actors.
    ///     Subscribe to process group to update that list whenever actors join / leave.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use ractor::cast;
    /// # use acdis::db_actor::message::{DBMessage, DBRequest};
    /// # use acdis::parse_actor::parse_request_message::ParseRequestMessage;
    /// # use acdis::parse_actor::parse_request_actor::ParseRequestActor;
    /// # use redis_protocol_bridge::commands::parse::Request;
    /// # use redis_protocol_bridge::util::convert::SerializableFrame;
    /// # use redis_protocol::resp3::types::OwnedFrame;
    /// # let message = ParseRequestMessage{frame: SerializableFrame(OwnedFrame::Null), reply_to: "actor".into()};
    ///
    /// let actor = ParseRequestActor;
    /// let request = Request::GET{ key: String::from("my_key") };
    /// let responsible = actor.find_responsible(&request).await.unwrap();
    ///
    ///
    /// cast!(
    ///    responsible,
    ///    NodeManagerMessage::Forward(
    ///        DBRequest{request, reply_to: message.reply_to}
    ///    )
    ///)?;
    /// ```
    async fn find_responsible(
        &self,
        request: &Request,
    ) -> Result<ActorRef<NodeManagerMessage>, ActorProcessingErr> {
        let group_name = "acdis_node_managers".to_string();
        let members = pg::get_members(&group_name);

        match request {
            Request::GET { key } | Request::SET { key, .. } => {
                let hashslot = HashSlot::new(key);
                debug!("{:#?}", hashslot);

                for member in members {
                    let actor_ref = ActorRef::<NodeManagerMessage>::from(member);

                    match call!(actor_ref, NodeManagerMessage::Responsible, hashslot) {
                        Ok(responsible) => {
                            if responsible {
                                return Ok(actor_ref);
                            }
                        }
                        Err(e) => {
                            warn!("Error calling node manager: {}", e);
                        }
                    }
                }
            }
            _ => {}
        }

        if let Some(member) = pg::get_members(&group_name).first() {
            debug!("Responsible actor: {}", member.get_id().pid());
            Ok(ActorRef::<NodeManagerMessage>::from(member.clone()))
        } else {
            error!("Couldn't find responsible actor");
            Err(ActorProcessingErr::from(
                "No actor responsible for this key",
            ))
        }
    }

    /// Hash the given key using [`AHasher`]
    ///
    /// # Arguments
    ///
    /// * `key`: The key to hash
    ///
    /// returns: u64 - The hash
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use acdis::parse_actor::parse_request_actor::ParseRequestActor;
    /// # let actor = ParseRequestActor;
    /// let hash = actor.hash("my_key");
    /// if (0..0xffaa).contains(hash) {
    ///     println!("We are responsible for this key.")
    /// }
    /// ```
    pub(crate) fn hash(key: &String) -> u64 {
        let mut hasher = AHasher::default();
        hasher.write(key.as_bytes());
        hasher.finish()
    }

    /// Return the crc16 of the given key.
    ///
    /// In redis cluster, the crc16 is used to determine the associated hash slot.
    /// Each cluster node is responsible for a subset of the 16384 hash slots.
    pub(crate) fn crc16(key: &String) -> u16 {
        crc16::State::<crc16::XMODEM>::calculate(key.as_bytes())
    }
}
