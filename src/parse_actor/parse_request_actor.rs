use crate::db_actor::message::DBRequest;
use crate::hash_slot::hash_slot::HashSlot;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::parse_actor::parse_request_message::ParseRequestMessage;
use crate::parse_actor::parse_request_message::ParseRequestMessage::*;
use crate::parse_actor::subscription::Subscription;
use log::{debug, error, info, warn};
use ractor::{async_trait, call, cast, pg, registry, Actor, ActorCell, ActorProcessingErr, ActorRef};
use ractor_cluster::NodeServerMessage;
use ractor_cluster::NodeServerMessage::SubscribeToEvents;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::commands::parse::Request;
use redis_protocol_bridge::util::convert::SerializableFrame;

pub struct ParseRequestActor;

pub struct ParseRequestActorState {
    writer: ActorCell,
    node_managers: Vec<ActorRef<NodeManagerMessage>>
}

impl ParseRequestActorState {
    /// Given the name of the tcp writer actor, create a state with a reference to the writer actor
    /// and all node managers.
    pub fn new(writer: String) -> Self {
        let reply_to_vec = pg::get_members(&writer);
        assert_eq!(
            reply_to_vec.len(),
            1,
            "Found less than or more than one actor for {}",
            writer
        );

        let writer = reply_to_vec.into_iter().next().unwrap();

        let group_name = "acdis_node_managers".to_string();
        let manager_cells = pg::get_members(&group_name);
        let mut node_managers = Vec::with_capacity(manager_cells.len());
        for cell in manager_cells {
            let actor_ref = ActorRef::<NodeManagerMessage>::from(cell);
            node_managers.push(actor_ref);
        }

        ParseRequestActorState{
            writer,
            node_managers
        }
    }
    
    pub fn update_index(&mut self) {

        let group_name = "acdis_node_managers".to_string();
        let manager_cells = pg::get_members(&group_name);
        let mut node_managers = Vec::with_capacity(manager_cells.len());
        for cell in manager_cells {
            let actor_ref = ActorRef::<NodeManagerMessage>::from(cell);
            node_managers.push(actor_ref);
        }
        
        self.node_managers = node_managers;
    }
}

#[async_trait]
impl Actor for ParseRequestActor {
    type Msg = ParseRequestMessage;
    type State = ParseRequestActorState;
    type Arguments = String;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        writer: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        debug!("Spawning parsing actor...");
        
        self.subscribe_to_events(myself).await;

        Ok(ParseRequestActorState::new(writer))
    }

    /// Given an [`OwnedFrame`], parse it to a [`Request`] and forward that request to a
    /// [`actor::DBActor`]
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        own: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            Frame(frame) => {
                debug!("Handling parse request");
                let query = redis_protocol_bridge::parse_owned_frame(frame.frame.0);
                let request = redis_protocol_bridge::commands::parse::parse(query);

                match request {
                    Err(err) => {
                        error!("{}", err.details().to_string());
                        let err = OwnedFrame::SimpleError {
                            data: err.details().to_string(),
                            attributes: None,
                        };

                        Ok(own.writer.send_message(SerializableFrame(err))?)
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

                        cast!(
                    responsible,
                    NodeManagerMessage::Forward(DBRequest {
                        request,
                        reply_to: frame.reply_to
                    })
                )?;
                        Ok(())
                    }
                }
                
            },
            UpdateIndex => { Ok(own.update_index()) }
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

                for member in members.clone() {
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

        if let Some(member) = members.first() {
            debug!("Responsible actor: {}", member.get_id().pid());
            Ok(ActorRef::<NodeManagerMessage>::from(member.clone()))
        } else {
            error!("Couldn't find responsible actor");
            Err(ActorProcessingErr::from(
                "No actor responsible for this key",
            ))
        }
    }

    /// Subscribe to new nodes joining so we can update our index
    pub(crate) async fn subscribe_to_events(
        &self,
        myself: ActorRef<ParseRequestMessage>,
    ) {
        let node_server = registry::where_is(String::from("NodeServer")).expect("Failed to find NodeSever");
        let node_server_ref = ActorRef::<NodeServerMessage>::from(node_server);

        // Trigger methods when other nodes connect / disconnect
        cast!(
            node_server_ref,
            SubscribeToEvents {
                id: format!("Subscription {}", myself.get_id()),
                subscription: Box::new(Subscription(myself))
            }
        ).expect("Failed to send Subscription msg")
    }
}
