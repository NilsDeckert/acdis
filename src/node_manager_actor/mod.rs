use crate::node_manager_actor::message::NodeManagerMessage;
use ractor::ActorRef;
use std::fmt::{Display, Formatter};

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

impl Display for NodeManagerRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.actor.get_id(), self.host)
    }
}
