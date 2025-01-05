use log::error;
use ractor::{async_trait, cast, Actor, ActorErr, ActorProcessingErr, ActorRef};
use redis_protocol::resp3::types::OwnedFrame;
use crate::db_actor;
use crate::db_actor::actor;
use crate::db_actor::message::DBMessage;
use crate::parse_actor::parse_request_message::ParseRequestMessage;

pub struct ParseRequestActor;

#[async_trait]
impl Actor for ParseRequestActor {
    type Msg = ParseRequestMessage;
    type State = ();
    type Arguments = ();

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    /// Given an [`OwnedFrame`], parse it to a [`redis_protocol_bridge::commands::parse::Request`] and forward that request to a
    /// [`actor::DBActor`]
    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let query = redis_protocol_bridge::parse_owned_frame(message.frame);
        let request = redis_protocol_bridge::commands::parse::parse(query);

        match request {
            Err(err) => {
                error!("{}", err.details().to_string());
                let err = OwnedFrame::SimpleError {
                    data: err.details().to_string(),
                    attributes: None
                };

                if message.caller.is_closed() {
                    error!("Channel already closed");
                    Err(ActorProcessingErr::from("Attempt to reply but channel is closed"))
                } else {
                    message.caller.send(err)?;
                    Ok(())
                }

            }

            Ok(request) => {
                let group_name = "acdis".to_string();
                let members = ractor::pg::get_members(&group_name);

                if let Some(member) = members.first() {
                    // Forward the request to the db actor, who will reply on our behalf.
                    let actor_ref = ActorRef::<DBMessage>::from(member.clone());
                    cast!(
                        actor_ref,
                        DBMessage{request, caller: message.caller}
                    )?;
                    Ok(())
                } else {
                    Err(ActorErr::Cancelled.into())
                }

            }
        }
    }
}