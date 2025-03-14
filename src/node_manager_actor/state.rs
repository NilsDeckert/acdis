use crate::db_actor::message::{DBMessage, DBRequest};
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::NodeManagerRef;
use crate::parse_actor::parse_request_actor::ParseRequestActor;
use log::error;
use ractor::{ActorProcessingErr, ActorRef};
use ractor_cluster::NodeServerMessage;
use redis_protocol_bridge::commands::parse::Request;
use std::collections::HashMap;
use std::ops::Range;

#[derive(Clone)]
pub struct NodeManageActorState {
    /// The keyspace that is managed by this node
    pub keyspace: Range<u64>,
    /// This node's actors, identified by their part of the keyspace
    pub db_actors: HashMap<Range<u64>, ActorRef<DBMessage>>,
    /// The [`NodeServer`] that manages external connections for this node
    pub node_server: ActorRef<NodeServerMessage>,
    /// The other NodeManagers in this cluster, identified by their keyspace
    pub other_nodes: HashMap<Range<u64>, NodeManagerRef>,
    /// The port that accepts redis requests
    pub redis_host: String,
}

impl NodeManageActorState {
    /// Given a list of (ActorRef, Keyspace) Tuples, add them to HashMap of other NodeManagers
    pub(crate) fn update_index(
        &mut self,
        actors: Vec<(&ActorRef<NodeManagerMessage>, Range<u64>, String)>,
    ) {
        for (actor, keyspace, address) in actors {
            self.other_nodes.insert(
                keyspace,
                NodeManagerRef {
                    actor: actor.clone(),
                    host: address,
                },
            );
        }
    }

    pub(crate) fn merge_vec<'a>(
        &mut self,
        keyspaces: Vec<(&'a ActorRef<NodeManagerMessage>, Range<u64>)>,
        addresses: Vec<(&ActorRef<NodeManagerMessage>, String)>,
    ) -> Vec<(&'a ActorRef<NodeManagerMessage>, Range<u64>, String)> {
        let zip = keyspaces.into_iter().zip(addresses);
        zip.map(|t| {
            let (k, a) = t;
            let actor_ref = k.0;
            let keyspace = k.1;
            let addr = a.1;
            (actor_ref, keyspace, addr)
        })
        .collect()
    }

    /// Find the Actor that is responsible for the given hash.
    ///
    /// # Arguments
    ///
    /// * `hash`: Hash of the key
    ///
    /// returns: Option<ActorRef<DBMessage>>
    ///  - `Some(ActorRef<DBMessage>)` if a responsible actor was found
    ///  - `None` otherwise
    ///
    pub(crate) fn find_responsible_by_hash(&self, hash: &u64) -> Option<ActorRef<DBMessage>> {
        if !self.keyspace.contains(&hash) {
            error!(
                "Tried to find actor for hash {:#018x}, but we only manage {:#018x}..{:#018x}",
                &hash, self.keyspace.start, self.keyspace.end
            );
            return None;
        }

        for keyspace in self.db_actors.keys() {
            if keyspace.contains(hash) {
                return Some(self.db_actors[keyspace].clone());
            }
        }

        error!("No actor responsible for {:#018x}", &hash);
        for keyspace in self.db_actors.keys() {
            error!(" - {:#018x}..{:#018x}", keyspace.start, keyspace.end)
        }
        None
    }

    pub(crate) fn find_responsible_by_request(
        &self,
        request: &Request,
    ) -> Option<ActorRef<DBMessage>> {
        match request {
            Request::GET { key } | Request::SET { key, .. } => {
                let hash = ParseRequestActor::hash(&key);
                self.find_responsible_by_hash(&hash)
            }
            _ => self.db_actors.values().into_iter().next().cloned(),
        }
    }
}
