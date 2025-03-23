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
    pub host: String,
}
