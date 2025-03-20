pub mod actor;
pub mod behaviour;
mod keyspace;
pub mod message;
mod state;
mod subscription;

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeManagerRef {
    //pub actor: ActorRef<NodeManagerMessage>,
    pub host: String,
}
