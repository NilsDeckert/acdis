use ractor::ActorRef;
use crate::node_manager_actor::actor::NodeManagerActor;

pub mod actor;
mod actor_behaviour;
mod keyspace;
pub mod message;
mod subscription;

#[derive(Clone)]
pub struct NodeManagerRef {
    pub actor: ActorRef<NodeManagerActor>,
    pub host: String,
}

