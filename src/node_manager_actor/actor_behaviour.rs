use async_trait::async_trait;
use log::{debug, error, info, warn};
use rand::Rng;
use std::collections::HashMap;

use ractor::SupervisionEvent::*;
use ractor::{call, pg, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::NodeServerMessage::GetSessions;

use crate::db_actor::actor::{DBActorArgs, PartitionedHashMap};
use crate::db_actor::message::DBMessage;
use crate::node_manager_actor::actor::{NodeManageActorState, NodeManagerActor, NodeType};
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::message::NodeManagerMessage::*;

#[async_trait]
impl Actor for NodeManagerActor {
    type Msg = NodeManagerMessage;
    type State = NodeManageActorState;
    type Arguments = NodeType;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        /* Address of cluster master */
        let (cluster_host_address, cluster_host_port) = Self::get_host_address();

        let client_port = std::env::var("CLIENT_PORT")
            .unwrap_or(String::from("0")) // Let OS choose port
            .parse()
            .unwrap();

        let port;
        let name;

        // Set arguments that differ between server and client
        match args {
            NodeType::Server => {
                port = cluster_host_port;
                name = String::from("host_node");
            }
            NodeType::Client => {
                let mut rng = rand::thread_rng();
                port = client_port;
                name = myself
                    .get_name()
                    .unwrap_or(format!("Node {}", rng.gen::<u8>()));
            }
        }

        let pmd_ref = NodeManagerActor::spawn_pmd(port, name).await;
        NodeManagerActor::subscribe_to_events(myself.clone(), pmd_ref.clone()).await;

        // If NodeType is Client, we assume there is already another NodeServer accepting connections
        if let NodeType::Client = args {
            loop {
                match ractor_cluster::client_connect(
                    &pmd_ref,
                    format!("{}:{}", cluster_host_address, cluster_host_port),
                )
                .await
                {
                    Ok(_) => {
                        // Wait to establish connection
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        break;
                    }
                    Err(e) => {
                        error!("Failed to connect to node server: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    }
                }
            }
        }

        myself.send_message(Init)?;

        Ok(NodeManageActorState {
            keyspace: 0u64..u64::MAX,
            db_actors: HashMap::new(),
            node_server: pmd_ref,
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        own: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            Init => {
                debug!("Received init message");
                let pg_name = String::from("acdis_node_managers");

                let mut nodes = pg::get_members(&pg_name);
                info!("Found {} other nodes.", nodes.len());

                let keyspaces = Self::sort_actors_by_keyspace(&mut nodes).await;

                // Adopt half of the largest keyspace managed by another node
                if let Some((actor_ref, keyspace)) = keyspaces.into_iter().next() {
                    // Link to other node
                    myself.get_cell().link(actor_ref.get_cell());

                    info!("Other NodeManagers present, attempting to adopt keyspace");
                    let (_, r2) = Self::halve_range(keyspace);

                    // Inherit keys&values in that keyspace
                    // TODO: if inheriting fails, maybe try next node
                    let map =
                        call!(actor_ref, AdoptKeyspace, r2).expect("Failed to adopt keyspace");
                    let keyspace = map.range.clone();
                    own.keyspace = keyspace.clone();

                    // Only for testing. TODO: remove
                    debug!("We now manage the following key-value-pairs:");
                    for (key, value) in map.map.clone() {
                        debug!(" - {}:{}", key, value);
                    }

                    own.db_actors = Self::spawn_db_actors(
                        DBActorArgs {
                            map: Some(map),
                            range: keyspace,
                        },
                        8,
                        myself.clone(),
                    )
                    .await;
                } else {
                    info!("Could not find any other NodeManager");
                    own.db_actors = Self::spawn_db_actors(
                        DBActorArgs {
                            map: None,
                            range: 0u64..u64::MAX,
                        },
                        8,
                        myself.clone(),
                    )
                    .await;
                }

                // Join later to avoid sending messages to ourselves
                pg::join(pg_name.clone(), vec![myself.get_cell()]);
            }
            QueryKeyspace(reply) => {
                debug!("Received QueryKeyspace");
                reply.send(own.keyspace.clone())?;
            }
            SetKeyspace(range) => {
                // TODO
                info!("Setting keyspace to {:#?}", range);
                own.keyspace = range;
                if myself.get_children().is_empty() {
                    own.db_actors = NodeManagerActor::spawn_db_actors(
                        DBActorArgs {
                            map: None,
                            range: own.keyspace.clone(),
                        },
                        8,
                        myself.clone(),
                    )
                    .await;
                }
            }
            AdoptKeyspace(keyspace, reply) => {
                info!(
                    "{} Giving away keyspace {:#018x}..{:#018x}",
                    myself.get_name().unwrap_or(String::from("node_manager")),
                    keyspace.start,
                    keyspace.end
                );

                assert_ne!(keyspace, own.keyspace); // Don't give up whole keyspace.
                                                    // Keyspace must be at one end of our keyspace
                assert!(
                    (keyspace.start == own.keyspace.start && keyspace.end < own.keyspace.end)
                        || (keyspace.start > own.keyspace.start
                            && keyspace.end == own.keyspace.end)
                );

                let mut return_map = PartitionedHashMap {
                    map: HashMap::new(),
                    range: keyspace.clone(),
                };

                let mut to_remove = vec![];

                // Assumption: keyspace >= actor_keyspace
                for (actor_keyspace, actor) in &own.db_actors {
                    // Keyspace of this actor is completely inside
                    // the requested keyspace
                    if actor_keyspace.start >= keyspace.start && actor_keyspace.end <= keyspace.end
                    {
                        // ""Kill"" actor and fetch HashMap
                        // TODO: Currently the actor is not stopped
                        info!(
                            "'Killing' actor {:?} for keyspace {:#?}",
                            actor, actor_keyspace
                        );
                        let actor_hashmap = call!(actor, DBMessage::Drain);
                        return_map.map.extend(actor_hashmap.unwrap());

                        to_remove.push(actor_keyspace.clone());
                    } else if actor_keyspace.end <= keyspace.start
                        || actor_keyspace.start >= keyspace.end
                    {
                        // Actor does not overlap with requested keyspace
                        // Ignore
                    } else {
                        warn!(
                            "Did not cover this case:\n\
                        The keyspace of this actor ({:#018x}..{:#018x}) is not fully inside the \
                        requested keyspace ({:#018x}..{:#018x})",
                            actor_keyspace.start, actor_keyspace.end, keyspace.start, keyspace.end
                        )
                    }
                }

                // Forget actors whose keyspace we gave away
                for ks in to_remove {
                    own.db_actors.remove(&ks);
                }

                reply.send(return_map)?;
            }
            QueryNodes(reply) => {
                debug!("Received QueryNodes");
                let sessions = call!(own.node_server, GetSessions)?;
                let mut ret = vec![];
                for session in sessions.values() {
                    let addr = &session.peer_addr;
                    info!("Connected to: {}", addr);
                    ret.push(String::from(addr))
                }

                reply.send(ret)?;
            }
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        event: SupervisionEvent,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match event {
            ActorStarted(_actor) => {}
            ActorTerminated(_actor, _opt_state, _opt_reason) => {
                warn!(" Actor got terminated");
                if let Some(_state) = _opt_state {
                    warn!("  Received state")
                }
                if let Some(reason) = _opt_reason {
                    warn!("  Reason: {}", reason)
                }
            }
            ActorFailed(_actor, _err) => {
                error!(" Supervised actor failed");
            }
            ProcessGroupChanged(_actor) => {}
            PidLifecycleEvent(_actor) => {}
        }
        Ok(())
    }
}
