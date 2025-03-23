use std::fmt::Display;

pub mod actor;
pub mod behaviour;
mod keyspace;
pub mod message;
mod state;
mod subscription;

/// This module contains handlers for requests that should be processed on a node level instead of
/// being handled by the individual [`actor::DBActor`]s
mod command_handlers;

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeManagerRef {
    //pub actor: ActorRef<NodeManagerMessage>,
    pub host_ip: String,
    pub host_port: u16
}

impl Display for NodeManagerRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host_ip, self.host_port)
    }
}
