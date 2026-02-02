use crate::db_actor::actor::DBActor;
use crate::db_actor::actor::DBActorArgs;
use crate::db_actor::message::DBMessage;
use crate::db_actor::state::PartitionedHashMap;
use crate::db_actor::HashMap;
use crate::hash_slot::hash_slot_range::HashSlotRange;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::NodeManagerRef;
use crate::tcp_listener_actor::tcp_listener::{TcpConnectionMessage, TcpListenerActor};
use log::info;
use ractor::{call, pg, Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::node::NodeConnectionMode;
use ractor_cluster::{IncomingEncryptionMode, NodeServer, NodeServerMessage};
use redis_protocol_bridge::util::convert::SerializableFrame;

pub struct NodeManagerActor;

#[allow(dead_code)]
pub enum NodeType {
    Server,
    Client,
}

impl NodeManagerActor {
    /// Initialize and spawn a [`NodeServer`], thus accepting connections for communication across
    /// nodes.
    pub(crate) async fn spawn_pmd(addr: String, port: u16, name: String) -> ActorRef<NodeServerMessage> {
        // Init port mapper daemon that handles internode communication
        let pmd = NodeServer::new(
            port,
            std::env::var("CLUSTER_COOKIE").unwrap_or(String::from("cookie")),
            name,
            addr.clone(),
            Some(IncomingEncryptionMode::Raw),
            Some(NodeConnectionMode::Transitive),
        );

        // Spawn pmd, thus starting server/client
        let (pmd_ref, _pmd_handler) = Actor::spawn(Some(String::from("NodeServer")), pmd, ())
            .await
            .expect("Failed to spawn port mapper daemon");

        info!("Listening on {addr}:{port}");

        pmd_ref
    }

    /// Open a TCP Port to accept redis requests by spawning a [`TcpListenerActor`].
    ///
    /// # Environment Variables
    ///  * REDIS_HOST: Defaults to `0.0.0.0`
    ///  * REDIS_PORT: For [`NodeType::Server`] this defaults to `6379`, otherwise to `0`.
    ///
    /// # Returns
    ///  The ip and port that the TCP port was assigned.
    ///  In most cases this is `REDIS_HOST:REDIS_PORT` but differs if `REDIS_PORT` is `0`
    pub(crate) async fn spawn_redis_access_point() -> Result<(String, u16), ActorProcessingErr> {
        let host = std::env::var("REDIS_HOST").unwrap_or(String::from("0.0.0.0"));
        let port = std::env::var("REDIS_PORT").unwrap_or(String::from("0"));

        let (tcp_actor, _tcp_handler) = Actor::spawn(
            Some(String::from("TcpListenerActor")),
            TcpListenerActor,
            format!("{host}:{port}"),
        )
        .await
        .expect("Failed to spawn tcp listener actor");

        let address = call!(tcp_actor, TcpConnectionMessage::QueryAddress);
        Ok(address?)
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
    /// # Returns
    /// A HashMap that maps a Range (Keyspace) to a responsible actor
    pub(crate) async fn spawn_db_actors(
        args: DBActorArgs,
        actors_to_join: u16,
        supervisor: ActorRef<NodeManagerMessage>,
    ) -> HashMap<HashSlotRange, ActorRef<DBMessage>> {
        let mut ret_map: HashMap<HashSlotRange, ActorRef<DBMessage>> = HashMap::default();
        info!(
            "Spawning {} DB actors for range {}",
            actors_to_join, args.range
        );

        let mut initial_maps = Vec::with_capacity(actors_to_join.into());
        let ranges = NodeManagerActor::chunk_ranges(args.range, actors_to_join);

        if args.map.is_some() {
            for range in ranges {
                initial_maps.push(PartitionedHashMap {
                    map: HashMap::default(),
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
                let range = map.range;
                let (actor_ref, _handle) = Actor::spawn_linked(
                    Some(format!("DBActor {range}")),
                    DBActor,
                    DBActorArgs {
                        map: Some(map),
                        range
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
                    Some(format!("DBActor {range}")),
                    DBActor,
                    DBActorArgs {
                        map: None,
                        range
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

    pub(crate) fn send_index_update(
        &self,
        myself: ActorRef<NodeManagerMessage>,
        keyspace: HashSlotRange,
        info: NodeManagerRef,
    ) -> Result<(), ActorProcessingErr> {
        let others = pg::get_members(&String::from("acdis_node_managers"));
        for node in others {
            if myself.get_id() == node.get_id() {
                continue;
            }
            node.send_message(NodeManagerMessage::IndexUpdate(
                keyspace,
                info.clone(),
            ))?;
        }

        Ok(())
    }

    /// Attempts to find a tcp_writer with name `reply_to` and sends `reply`
    ///
    /// # Returns
    ///  - `Ok()` if sending was successful
    ///  - `Err()` if no tcp_writer was found or if sending failed
    pub(crate) fn reply_to(
        reply_to: &str,
        reply: SerializableFrame,
    ) -> Result<(), ActorProcessingErr> {
        let tcp_writer = pg::get_members(&reply_to.into());
        if let Some(tcp_writer) = tcp_writer.first() {
            Ok(tcp_writer.send_message(reply)?)
        } else {
            Err(ActorProcessingErr::from(format!(
                "Could not find tcp_writer for address {reply_to}"
            )))
        }
    }
}
