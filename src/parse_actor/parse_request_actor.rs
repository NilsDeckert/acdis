use std::hash::Hasher;
use log::{debug, error, warn};
use ractor::{async_trait, call, cast, pg, Actor, ActorProcessingErr, ActorRef};
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::commands::parse::Request;
use redis_protocol_bridge::util::convert::SerializableFrame;
use crate::db_actor::AHasher;
use crate::db_actor::message::{DBMessage, DBRequest};
use crate::parse_actor::parse_request_message::ParseRequestMessage;

pub struct ParseRequestActor;

#[async_trait]
impl Actor for ParseRequestActor {
    type Msg = ParseRequestMessage;
    type State = ();
    type Arguments = ();

    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, _args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        debug!("Spawning parsing actor...");
        Ok(())
    }

    /// Given an [`OwnedFrame`], parse it to a [`Request`] and forward that request to a
    /// [`actor::DBActor`]
    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        debug!("Handling parse request");
        let query = redis_protocol_bridge::parse_owned_frame(message.frame.0);
        let request = redis_protocol_bridge::commands::parse::parse(query);
        
        let reply_to_vec = ractor::pg::get_members(&message.reply_to);
        assert_eq!(reply_to_vec.len(), 1,
                   "Found less than or more than one actor for {}", message.reply_to);
        let reply_to = reply_to_vec.into_iter().next().unwrap();

        match request {
            Err(err) => {
                error!("{}", err.details().to_string());
                let err = OwnedFrame::SimpleError {
                    data: err.details().to_string(),
                    attributes: None
                };

                Ok(reply_to.send_message(SerializableFrame(err))?)
            }

            Ok(request) => {
                let responsible = self.find_responsible(&request).await?;
                debug!("Request: {:?}", request);
                
                cast!(
                    responsible,
                    DBMessage::Request(
                        DBRequest{request, reply_to: message.reply_to}
                    )
                )?;
                Ok(())
            }
        }
    }
}

impl ParseRequestActor {
    /// Return the* responsible actor for a given request.
    /// 
    /// \*For Requests that concern a specific key, the responsible actor is found by hashing the key.
    /// Requests like PING can be handled by multiple actors, thus any actor might be return.
    /// 
    /// # Arguments 
    /// 
    /// * `request`: [`Request`] that needs to be handled. 
    /// 
    /// returns: Result<ActorRef<DBMessage>, Box<dyn Error+Send+Sync, Global>> 
    /// 
    /// # Implementation
    /// 
    /// Currently, this is done very inefficiently by querying every actor and asking whether it is
    /// responsible for hash of the given key.
    /// 
    /// Possible ways to improve this include:
    ///  - Keep a some sort of data structure that maps keys to responsible actors.
    ///     Subscribe to process group to update that list whenever actors join / leave.
    /// 
    /// # Examples 
    /// 
    /// ```rust
    /// let request = Request::GET("my_key");
    /// let responsible = self.find_responsible(&request).await?;
    /// 
    /// cast!(
    ///    responsible,
    ///    DBMessage::Request(
    ///        DBRequest{request, caller: message.caller}
    ///    )
    ///)?;
    /// ```
    async fn find_responsible(&self, request: &Request) -> Result<ActorRef<DBMessage>, ActorProcessingErr> {
        let group_name = "acdis".to_string();
        let members = ractor::pg::get_members(&group_name);
        
        match request {
            Request::GET { key } |
            Request::SET { key, .. } => {
                let hash = self.hash(key);
                for member in members {
                    let actor_ref = ActorRef::<DBMessage>::from(member);
                    match call!(actor_ref, DBMessage::Responsible, hash) {
                        Ok(responsible) => {
                            if responsible {
                                return Ok(actor_ref);
                            }
                        }
                        Err(e) => {
                            warn!("Error calling db_actor: {}", e);
                        }
                    }
                }
            }
            _ => {}
        }
        
        if let Some(member) = pg::get_members(&group_name).first() {
            debug!("Responsible actor: {}", member.get_id().pid());
            Ok(ActorRef::<DBMessage>::from(member.clone()))
        } else {
            error!("Couldn't find responsible actor");
            Err(ActorProcessingErr::from("No actor responsible for this key"))
        }
        
    }
    
    /// Hash the given key using [`crate::db_actor::actor::AHasher`]
    /// 
    /// # Arguments 
    /// 
    /// * `key`: The key to hash
    /// 
    /// returns: u64 - The hash
    /// 
    /// # Examples 
    /// 
    /// ```rust
    /// let hash = self.hash("my_key");
    /// if (0..0xffaa).contains(hash) {
    ///     println!("We are responsible for this key.")
    /// }
    /// ```
    fn hash(&self, key: &String) -> u64 {
        let mut hasher = AHasher::default();
        hasher.write(key.as_bytes());
        hasher.finish()
    }
}