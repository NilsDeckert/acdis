use crate::db_actor::message::{DBMessage, DBRequest};
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::NodeManagerRef;
use crate::parse_actor::parse_request_actor::ParseRequestActor;
use log::{error, info};
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
    /// Given a list of (ActorRef, Keyspace, Address) Tuples, add them to HashMap of other NodeManagers
    pub(crate) fn update_index(
        &mut self,
        actors: Vec<(Range<u64>, NodeManagerRef)>,
    ) {
        for (keyspace, node_manager_ref) in actors {
            self.other_nodes.insert(
                keyspace,
                node_manager_ref
            );
        }
    }

    pub(crate) fn merge_vec(
        &mut self,
        keyspaces: &Vec<(&ActorRef<NodeManagerMessage>, Range<u64>)>,
        addresses: &Vec<(&ActorRef<NodeManagerMessage>, String)>,
    ) -> Vec<(Range<u64>, NodeManagerRef)> {
        let zip = keyspaces.into_iter().zip(addresses);
        zip.map(|t| {
            let (k, a) = t;
            let keyspace = k.1.clone();
            let node_manager_ref = NodeManagerRef{
                host: a.1.clone()
            };
            (keyspace, node_manager_ref)
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
    
    /// Given a request find the responsible [`DBActor`] on this node.
    /// 
    /// For requests with key, this is just a wrapper around [`self.find_responsible_by_hash`].
    /// For others, it just returns the first actor in its list.
    /// 
    /// ## Return
    ///  - `Some(ActorRef<DBMessage>)`
    ///  - `None` if no actor on this node is responsible
    pub(crate) fn find_responsible_by_request(
        &self,
        request: &Request,
    ) -> Option<ActorRef<DBMessage>> {
        match request {
            // Requests with key need hashing to find responsible
            Request::GET { key } | Request::SET { key, .. } => {
                let hash = ParseRequestActor::hash(&key);
                self.find_responsible_by_hash(&hash)
            }
            // Doesn't matter who handles this, take first in list
            _ => self.db_actors.values().into_iter().next().cloned(),
        }
    }
    
    pub(crate) fn find_responsible_node_by_hash(
        &self,
        hash: &u64,
    ) -> Option<NodeManagerRef> {
        for (keyspace, actor) in &self.other_nodes {
            if keyspace.contains(hash) {
                return Some(actor.clone());
            }
        }
        None
    }
    
    pub(crate) fn find_responsible_node_by_request(
        &self,
        request: &Request,
    ) -> Option<NodeManagerRef> {
        match request {
            Request::GET { key } | Request::SET { key, .. } => {
                let hash = ParseRequestActor::hash(&key);
                self.find_responsible_node_by_hash(&hash)
            }
            _ => self.other_nodes.values().into_iter().next().cloned(),
        }
    }
    
    pub(crate) fn moved_error(
        &mut self,
        request: &Request,
    ) -> Result<String, ActorProcessingErr> {
        match request {
            Request::GET { key } | Request::SET { key, .. } => {
                let hash = ParseRequestActor::hash(&key);
                let responsible = self.find_responsible_node_by_hash(&hash);
                if let Some(responsible) = responsible {
                    let slot = ParseRequestActor::crc16(&key) % 16384;
                    info!("MOVED {slot} {}", responsible.host);
                    Ok(format!("MOVED {slot} {}", responsible.host))
                } else {
                    Err(ActorProcessingErr::from("Unable to find responsible node"))
                }
            }
            _ => {
                let next = self.other_nodes.values().into_iter().next();
                if let Some(next) = next {
                    Ok(format!("MOVED 0 {}", next.host))
                } else {
                    Err(ActorProcessingErr::from("No other node in cluster"))
                }
            }
        }
    }
}
