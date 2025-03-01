use log::info;
use std::collections::HashMap;
use std::ops::Range;

use ractor::{cast, Actor, ActorCell, ActorRef};
use ractor_cluster::node::NodeConnectionMode;
use ractor_cluster::NodeServerMessage::SubscribeToEvents;
use ractor_cluster::{IncomingEncryptionMode, NodeServer, NodeServerMessage};

use crate::db_actor::actor::DBActor;
use crate::db_actor::actor::DBActorArgs;
use crate::db_actor::actor::PartitionedHashMap;
use crate::db_actor::message::DBMessage;
use crate::node_manager_actor::message::NodeManagerMessage;

pub struct NodeManagerActor;

pub struct NodeManageActorState {
    pub keyspace: Range<u64>,
    pub db_actors: HashMap<Range<u64>, ActorRef<DBMessage>>,
    pub node_server: ActorRef<NodeServerMessage>,
}

#[allow(dead_code)]
pub enum NodeType {
    Server,
    Client,
}

impl NodeManagerActor {
    /// Initialize and spawn a [`NodeServer`], thus accepting connections for communication across
    /// nodes.
    pub(crate) async fn spawn_pmd(port: u16, name: String) -> ActorRef<NodeServerMessage> {
        // Init port mapper daemon that handles internode communication
        let pmd = NodeServer::new(
            port,
            std::env::var("CLUSTER_COOKIE").unwrap_or(String::from("cookie")),
            name,
            String::from("localhost"), // TODO: This is the String used by other nodes connecting to us. Use IP so it works across the network
            Some(IncomingEncryptionMode::Raw),
            Some(NodeConnectionMode::Transitive),
        );

        // Spawn pmd, thus starting server/client
        let (pmd_ref, _pmd_handler) = Actor::spawn(None, pmd, ())
            .await
            .expect("Failed to spawn port mapper daemon");

        pmd_ref
    }

    /// Spawn and link DB actors
    ///
    /// # Arguments
    ///  * args:
    ///     * args.range: Keyspace that shall be managed by the created actors
    ///     * args.map: (Optional) [`PartitionedHashMap`] containing initial values
    ///  * actors_to_join: Number of [`DBActor`] that will be spawned
    ///  * supervisor: [`NodeManagerActor`] that the db_actors will be linked to. Usually the caller.
    ///
    /// # Return
    /// Returns a HashMap that maps a Range (Keyspace) to a responsible actor
    pub(crate) async fn spawn_db_actors(
        args: DBActorArgs,
        actors_to_join: u64,
        supervisor: ActorRef<NodeManagerMessage>,
    ) -> HashMap<Range<u64>, ActorRef<DBMessage>> {
        let mut ret_map: HashMap<Range<u64>, ActorRef<DBMessage>> = HashMap::new();
        info!(
            "Spawning {} DB actors for range {:#018x}..{:#018x}",
            actors_to_join, args.range.start, args.range.end
        );

        let mut initial_maps = vec![];
        let ranges = NodeManagerActor::chunk_ranges(args.range.clone(), actors_to_join);

        if args.map.is_some() {
            for range in ranges {
                initial_maps.push(PartitionedHashMap {
                    map: HashMap::new(),
                    range,
                })
            }

            let mut argsmap = args.map.unwrap();
            for (key, value) in argsmap.map.drain() {
                for map in &mut initial_maps {
                    if map.in_range(&key) {
                        map.map.insert(key, value);
                        break;
                    }
                }
            }

            for map in initial_maps {
                let range = map.range.clone();
                let (actor_ref, _handle) = Actor::spawn_linked(
                    Some(format!(
                        "DBActor {:#018x}..{:#018x}",
                        range.start, range.end
                    )),
                    DBActor,
                    DBActorArgs {
                        map: Some(map),
                        range: range.clone(),
                    },
                    supervisor.get_cell(),
                )
                .await
                .expect("Failed to spawn DBActor");

                ret_map.insert(range, actor_ref);
            }
        } else {
            for range in ranges {
                let (actor_ref, _handle) = Actor::spawn_linked(
                    Some(format!(
                        "DBActor {:#018x}..{:#018x}",
                        range.start, range.end
                    )),
                    DBActor,
                    DBActorArgs {
                        map: None,
                        range: range.clone(),
                    },
                    supervisor.get_cell(),
                )
                .await
                .expect("Failed to spawn DBActor");

                ret_map.insert(range, actor_ref);
            }
        }

        ret_map
    }

    /// Return address and port of cluster master.
    ///
    /// Uses environment values CLUSTER_HOST & CLUSTER_PORT or default
    /// values `127.0.0.1` & `6381`
    ///
    /// # Returns
    ///  - Address to connect to
    ///  - Port to connect to
    pub(crate) fn get_host_address() -> (String, u16) {
        let cluster_host_address = std::env::var("CLUSTER_HOST").unwrap_or("127.0.0.1".to_string());
        let cluster_host_port = std::env::var("CLUSTER_PORT")
            .unwrap_or(String::from("6381"))
            .parse()
            .unwrap();
        (cluster_host_address, cluster_host_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_halve_range() {
        assert_eq!(NodeManagerActor::halve_range(0..11), (0..6, 6..11));
        assert_eq!(NodeManagerActor::halve_range(0..10), (0..6, 6..10));
    }

    #[test]
    fn test_chunk_range_halve() {
        let chunked_ranges = NodeManagerActor::chunk_ranges(0..11, 2);
        let halved_ranges = NodeManagerActor::halve_range(0..11);
        assert_eq!(chunked_ranges[0], halved_ranges.0);
        assert_eq!(chunked_ranges[1], halved_ranges.1);
    }
}
