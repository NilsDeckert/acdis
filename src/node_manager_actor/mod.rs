use crate::node_manager_actor::message::NodeManagerMessage;
use ractor::ActorRef;
use std::fmt::{Display, Formatter};

pub mod actor;
pub mod behaviour;
mod keyspace;
pub mod message;
mod state;
mod subscription;

#[derive(Debug, Clone)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct NodeManagerRef {
    //pub actor: ActorRef<NodeManagerMessage>,
    pub host: String,
}
