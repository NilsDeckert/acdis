use crate::db_actor::message::DBRequest;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::parse_actor::parse_request_message::ParseRequestMessage;
use crate::parse_actor::parse_request_message::ParseRequestMessage::*;
use crate::parse_actor::subscription::Subscription;
use log::{debug, error, info};
use ractor::{
    async_trait, cast, pg, registry, Actor, ActorCell, ActorProcessingErr, ActorRef,
};
use ractor_cluster::NodeServerMessage;
use ractor_cluster::NodeServerMessage::SubscribeToEvents;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::util::convert::SerializableFrame;

pub struct ParseRequestActor;

pub struct ParseRequestActorState {
    writer: ActorCell,
    node_managers: Vec<ActorRef<NodeManagerMessage>>,
    this_node_manager: ActorRef<NodeManagerMessage>,
}

impl ParseRequestActorState {
    /// Given the name of the tcp writer actor, create a state with a reference to the writer actor
    /// and all node managers.
    pub fn new(writer: String) -> Self {
        let reply_to_vec = pg::get_members(&writer);
        assert_eq!(
            reply_to_vec.len(),
            1,
            "Found less than or more than one actor for {writer}"
        );

        let writer = reply_to_vec.into_iter().next().unwrap();

        let group_name = "acdis_node_managers".to_string();
        let manager_cells = pg::get_members(&group_name);
        let mut node_managers = Vec::with_capacity(manager_cells.len());
        for cell in manager_cells {
            let actor_ref = ActorRef::<NodeManagerMessage>::from(cell);
            node_managers.push(actor_ref);
        }

        let mut local_managers = pg::get_local_members(&group_name);
        while local_managers.len() < 1 {
            local_managers = pg::get_local_members(&group_name);
        }
        let this_manager_cell = local_managers.into_iter().next().unwrap();
        let this_node_manager = ActorRef::<NodeManagerMessage>::from(this_manager_cell);

        ParseRequestActorState {
            writer,
            node_managers,
            this_node_manager,
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
                        error!("{}", err.details());
                        let err = OwnedFrame::SimpleError {
                            data: err.details().to_string(),
                            attributes: None,
                        };

                        Ok(own.writer.send_message(SerializableFrame(err))?)
                    }

                    Ok(request) => {
                        let responsible = &own.this_node_manager;
                        info!("Request: {request:?}");

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
            }
            UpdateIndex => {
                own.update_index();
                Ok(())
            },
        }
    }
}

impl ParseRequestActor {

    /// Subscribe to new nodes joining so we can update our index
    pub(crate) async fn subscribe_to_events(&self, myself: ActorRef<ParseRequestMessage>) {
        let node_server =
            registry::where_is(String::from("NodeServer")).expect("Failed to find NodeSever");
        let node_server_ref = ActorRef::<NodeServerMessage>::from(node_server);

        // Trigger methods when other nodes connect / disconnect
        cast!(
            node_server_ref,
            SubscribeToEvents {
                id: format!("Subscription {}", myself.get_id()),
                subscription: Box::new(Subscription(myself))
            }
        )
        .expect("Failed to send Subscription msg")
    }
}
