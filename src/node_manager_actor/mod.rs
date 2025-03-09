use crate::node_manager_actor::message::NodeManagerMessage;
use ractor::ActorRef;

pub mod actor;
pub mod behaviour;
mod keyspace;
pub mod message;
mod state;
mod subscription;

#[derive(Clone)]
pub struct NodeManagerRef {
    pub actor: ActorRef<NodeManagerMessage>,
    pub host: String,
}
