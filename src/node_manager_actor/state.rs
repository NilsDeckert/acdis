use crate::db_actor::message::DBMessage;
use crate::db_actor::HashMap;
use crate::hash_slot::hash_slot::HashSlot;
use crate::hash_slot::hash_slot_range::HashSlotRange;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::NodeManagerRef;
use log::{error, info};
use ractor::{ActorProcessingErr, ActorRef};
use ractor_cluster::NodeServerMessage;
use redis_protocol_bridge::commands::parse::Request;

#[derive(Clone)]
pub struct NodeManagerActorState {
    /// The keyspace that is managed by this node
    pub keyspace: HashSlotRange,
    /// This node's actors, identified by their part of the keyspace
    pub db_actors: HashMap<HashSlotRange, ActorRef<DBMessage>>,
    /// The [`NodeServer`] that manages external connections for this node
    pub node_server: ActorRef<NodeServerMessage>,
    /// The other NodeManagers in this cluster, identified by their keyspace
    pub other_nodes: HashMap<HashSlotRange, NodeManagerRef>,
    /// The port that accepts redis requests
    pub redis_host: (String, u16),
}

impl NodeManagerActorState {
    /// Given a list of (ActorRef, Keyspace, Address) Tuples, add them to HashMap of other NodeManagers
    pub(crate) fn update_index(&mut self, actors: Vec<(HashSlotRange, NodeManagerRef)>) {
        // Delete all entries of nodes in the passed `actors`
        let refs: Vec<NodeManagerRef> = actors.clone().into_iter().map(|(_range, n)| n).collect();
        self.other_nodes.retain(|_, value| !refs.contains(value));

        for (keyspace, node_manager_ref) in actors {
            self.other_nodes.insert(keyspace, node_manager_ref);
        }
    }

    pub(crate) fn merge_vec(
        &mut self,
        keyspaces: &Vec<(&ActorRef<NodeManagerMessage>, HashSlotRange)>,
        addresses: &Vec<(&ActorRef<NodeManagerMessage>, (String, u16))>,
    ) -> Vec<(HashSlotRange, NodeManagerRef)> {
        let mut merged: Vec<(HashSlotRange, NodeManagerRef)> = Vec::with_capacity(keyspaces.len());

        for keyspace in keyspaces {
            for addr in addresses {
                if keyspace.0.get_id() == addr.0.get_id() {
                    merged.push((
                        keyspace.1,
                        NodeManagerRef {
                            host_ip: addr.1 .0.clone(),
                            host_port: addr.1 .1,
                        },
                    ));
                    continue;
                }
            }
        }
        merged
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
    pub(crate) fn find_responsible_by_hashslot(
        &self,
        hashslot: &HashSlot,
    ) -> Option<ActorRef<DBMessage>> {
        if !self.keyspace.contains(hashslot) {
            info!(
                "Tried to find actor for hash {:?}, but we only manage {}",
                &hashslot, self.keyspace
            );
            return None;
        }

        for keyspace in self.db_actors.keys() {
            if keyspace.contains(hashslot) {
                return Some(self.db_actors[keyspace].clone());
            }
        }

        error!("No actor responsible for {:?}", &hashslot);
        for keyspace in self.db_actors.keys() {
            error!(" - {keyspace}")
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
                let hashslot = HashSlot::new(key);
                self.find_responsible_by_hashslot(&hashslot)
            }
            Request::DEL { keys } => {
                // TODO: This only checks the first key, but should check all
                let hashslot = HashSlot::new(keys[0].as_str());
                self.find_responsible_by_hashslot(&hashslot)
            }
            // Doesn't matter who handles this, take first in list
            _ => self.db_actors.values().next().cloned(),
        }
    }

    pub(crate) fn find_responsible_node_by_hashslot(
        &self,
        hashslot: &HashSlot,
    ) -> Option<NodeManagerRef> {
        for (keyspace, actor) in &self.other_nodes {
            if keyspace.contains(hashslot) {
                return Some(actor.clone());
            }
            info!("{actor}: {hashslot:#?} not in {keyspace}");
        }
        None
    }

    pub(crate) fn moved_error(&mut self, request: &Request) -> Result<String, ActorProcessingErr> {
        match request {
            Request::GET { key } | Request::SET { key, .. } => {
                let hashslot = HashSlot::new(key);
                let responsible = self.find_responsible_node_by_hashslot(&hashslot);
                if let Some(responsible) = responsible {
                    let slot = HashSlot::new(key);
                    info!("MOVED {slot} {responsible}");
                    Ok(format!("MOVED {slot} {responsible}"))
                } else {
                    Err(ActorProcessingErr::from("Unable to find responsible node"))
                }
            }
            _ => {
                let next = self.other_nodes.values().next();
                if let Some(next) = next {
                    Ok(format!("MOVED 0 {next}"))
                } else {
                    Err(ActorProcessingErr::from("No other node in cluster"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ractor::Actor;
    use ractor_cluster::node::NodeConnectionMode;
    use ractor_cluster::{IncomingEncryptionMode, NodeServer};

    #[tokio::test]
    async fn test_update_index() {
        // Setup NodeServer just for the ActorRef
        let pmd = NodeServer::new(
            0,
            String::from("cookie"),
            String::from("TestNodeServer"),
            String::from("localhost"),
            Some(IncomingEncryptionMode::Raw),
            Some(NodeConnectionMode::Transitive),
        );

        let (pmd_ref, _pmd_handler) = Actor::spawn(None, pmd, ())
            .await
            .expect("Failed to spawn port mapper daemon");

        let mut state = NodeManagerActorState {
            keyspace: HashSlotRange::from(0..1), // Not relevant
            db_actors: HashMap::default(),
            node_server: pmd_ref,
            other_nodes: HashMap::default(),
            redis_host: ("127.0.0.1".to_string(), 6379),
        };

        let node_ref_1 = NodeManagerRef {
            host_ip: "localhost".into(),
            host_port: 6380,
        };
        let node_ref_2 = NodeManagerRef {
            host_ip: "Also doesnt matter".into(),
            host_port: 6381,
        };

        state.update_index(vec![
            (HashSlotRange::from(0..50), node_ref_1.clone()),
            (HashSlotRange::from(50..100), node_ref_2.clone()),
        ]);

        assert_eq!(state.other_nodes.len(), 2);
        assert_eq!(
            state.other_nodes.get(&HashSlotRange::from(0..50)),
            Some(&node_ref_1)
        );
        assert_eq!(
            state.other_nodes.get(&HashSlotRange::from(50..100)),
            Some(&node_ref_2)
        );

        state.update_index(vec![(HashSlotRange::from(50..150), node_ref_2.clone())]);

        assert_eq!(state.other_nodes.len(), 2);
        assert_eq!(
            state.other_nodes.get(&HashSlotRange::from(50..150)),
            Some(&node_ref_2)
        );
    }
}
