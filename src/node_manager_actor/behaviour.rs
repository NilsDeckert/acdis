use crate::db_actor::message::DBMessage;
use crate::db_actor::HashMap;
use crate::db_actor::{actor::DBActorArgs, state::PartitionedHashMap};
use crate::hash_slot::hash_slot_range::HashSlotRange;
use crate::hash_slot::{MAX, MIN};
use crate::node_manager_actor::actor::{NodeManagerActor, NodeType};
use crate::node_manager_actor::command_handlers::{node_handle, node_handles};
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::message::NodeManagerMessage::*;
use crate::node_manager_actor::state::NodeManagerActorState;
use crate::node_manager_actor::NodeManagerRef;
use async_trait::async_trait;
use log::{debug, error, info, warn};
use ractor::SupervisionEvent::*;
use ractor::{call, pg, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::NodeServerMessage::GetSessions;
use rand::Rng;
use redis_protocol::resp3::types::OwnedFrame;

const INITIAL_DB_ACTORS: u16 = 16;

#[async_trait]
impl Actor for NodeManagerActor {
    type Msg = NodeManagerMessage;
    type State = NodeManagerActorState;
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

                // Use default redis port if nothing else was specified via env variables
                if std::env::var("REDIS_PORT").is_err() {
                    std::env::set_var("REDIS_PORT", "6379")
                }
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

        let redis_host = Self::spawn_redis_access_point().await?;

        myself.send_message(Init)?;

        Ok(NodeManagerActorState {
            keyspace: HashSlotRange::from(MIN..MAX),
            db_actors: HashMap::default(),
            node_server: pmd_ref,
            other_nodes: HashMap::default(),
            redis_host,
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

                let nodes = pg::get_members(&pg_name);
                info!("Found {} other nodes.", nodes.len());
                let actor_refs: Vec<ActorRef<NodeManagerMessage>> = nodes
                    .into_iter()
                    .map(ActorRef::<NodeManagerMessage>::from)
                    .collect();

                let mut keyspaces = Self::query_keyspaces(&actor_refs).await?;
                keyspaces = Self::sort_actors_by_keyspace(keyspaces).await;

                info!("Sorted actor list: {:#?}", keyspaces);

                let addresses = Self::query(&actor_refs, QueryAddress, None);

                // Adopt half of the largest keyspace managed by another node
                if let Some((donor, keyspace)) = keyspaces.clone().into_iter().next() {
                    // Link to other node
                    myself.get_cell().link(donor.get_cell());

                    info!("Other NodeManagers present, attempting to adopt keyspace");
                    let (remaining, requesting) = Self::halve_range(keyspace);

                    // Inherit keys&values in that keyspace
                    // TODO: if inheriting fails, maybe try next node
                    let map =
                        call!(donor, AdoptKeyspace, requesting).expect("Failed to adopt keyspace");
                    let keyspace = map.range;
                    own.keyspace = keyspace;

                    own.db_actors = Self::spawn_db_actors(
                        DBActorArgs {
                            map: Some(map),
                            range: keyspace,
                        },
                        INITIAL_DB_ACTORS,
                        myself.clone(),
                    )
                    .await;

                    // Update keyspace of donor
                    for (actor, keyspace) in &mut keyspaces {
                        if actor.get_id() == donor.get_id() {
                            keyspace.start = remaining.start;
                            keyspace.end = remaining.end;
                            break;
                        }
                    }
                } else {
                    info!("Could not find any other NodeManager");
                    own.db_actors = Self::spawn_db_actors(
                        DBActorArgs {
                            map: None,
                            range: HashSlotRange::from(MIN..MAX),
                        },
                        INITIAL_DB_ACTORS,
                        myself.clone(),
                    )
                    .await;
                }

                let merged = own.merge_vec(&keyspaces, &addresses.await?);
                own.update_index(merged);

                // TODO: Remove
                for (keyspace, info) in &own.other_nodes {
                    println!("- {keyspace}: {:#?}", info)
                }

                // Inform other nodes about our initial keyspace
                self.send_index_update(
                    myself.clone(),
                    own.keyspace,
                    NodeManagerRef {
                        host_ip: own.redis_host.0.clone(),
                        host_port: own.redis_host.1,
                    },
                )?;

                // Join later to avoid sending messages to ourselves
                pg::join(pg_name.clone(), vec![myself.get_cell()]);
            }
            QueryKeyspace(reply) => {
                debug!("Received QueryKeyspace");
                reply.send(own.keyspace)?;
            }
            SetKeyspace(range) => {
                // TODO
                info!("Setting keyspace to {range}");
                own.keyspace = range;
                if myself.get_children().is_empty() {
                    own.db_actors = NodeManagerActor::spawn_db_actors(
                        DBActorArgs {
                            map: None,
                            range: own.keyspace,
                        },
                        8,
                        myself.clone(),
                    )
                    .await;
                }
            }
            AdoptKeyspace(requested, reply) => {
                if own.db_actors.len() < 2 {
                    panic!("Asked to give away keyspace, but less than 2 actors remaining")
                }

                info!(
                    "{} Giving away keyspace {requested}",
                    myself.get_name().unwrap_or(String::from("node_manager"))
                );

                assert_ne!(requested, own.keyspace); // Don't give up whole keyspace.

                // Keyspace must be at one end of our keyspace
                assert!(
                    (requested.start == own.keyspace.start && requested.end < own.keyspace.end)
                        || (requested.start > own.keyspace.start
                            && requested.end == own.keyspace.end)
                );

                let mut return_map = PartitionedHashMap {
                    map: HashMap::default(),
                    range: requested,
                };

                let mut to_remove = vec![];

                // Assumption: keyspace >= actor_keyspace
                for (actor_keyspace, actor) in &own.db_actors {
                    // Keyspace of this actor is completely inside
                    // the requested keyspace
                    if actor_keyspace.start >= requested.start
                        && actor_keyspace.end <= requested.end
                    {
                        // ""Kill"" actor and fetch HashMap
                        info!("'Killing' actor {:?} for keyspace {actor_keyspace}", actor);
                        let actor_hashmap = call!(actor, DBMessage::Drain);
                        return_map.map.extend(actor_hashmap.unwrap());

                        to_remove.push(*actor_keyspace);
                    } else if actor_keyspace.end <= requested.start
                        || actor_keyspace.start >= requested.end
                    {
                        // Actor does not overlap with requested keyspace
                        // Ignore
                    } else {
                        warn!(
                            "Did not cover this case:\n\
                        The keyspace of this actor ({actor_keyspace}) is not fully inside the \
                        requested keyspace ({requested})"
                        )
                    }
                }

                // Forget actors whose keyspace we gave away
                for ks in to_remove {
                    own.db_actors.remove(&ks);
                }

                if requested.start == own.keyspace.start && requested.end < own.keyspace.end {
                    // [==requested==|=remaining=]
                    //               ^ New start of our keyspace
                    own.keyspace.start = requested.end + 1;
                } else if requested.start > own.keyspace.start && requested.end == own.keyspace.end
                {
                    // [=remaining=|==requested==]
                    //             ^ New end of our keyspace
                    own.keyspace.end = requested.start - 1;
                } else {
                    // [=remaining=|==requested==|=remaining=]
                    panic!("Requested keyspace is not at one end of our keyspace.")
                }

                info!("Our new keyspace: {}", own.keyspace);

                // Inform other nodes that our keyspace has changed
                self.send_index_update(
                    myself,
                    own.keyspace,
                    NodeManagerRef {
                        host_ip: own.redis_host.0.clone(),
                        host_port: own.redis_host.1,
                    },
                )?;

                reply.send(return_map)?;
            }
            QueryNodes(reply) => {
                debug!("Received QueryNodes");
                let sessions = call!(own.node_server, GetSessions)?;
                let mut ret = Vec::with_capacity(sessions.len());
                for session in sessions.values() {
                    let addr = &session.peer_addr;
                    info!("Connected to: {}", addr);
                    ret.push(String::from(addr))
                }

                reply.send(ret)?;
            }
            Responsible(hash, reply) => reply.send(own.keyspace.contains(&hash))?,
            Forward(request) => {
                // Some request are handled by the node
                if node_handles(&request) {
                    if let Err(e) = node_handle(&request, own) {
                        NodeManagerActor::reply_to(
                            &request.reply_to,
                            OwnedFrame::SimpleError {
                                data: e.to_string(),
                                attributes: None,
                            }
                            .into(),
                        )?
                    }

                    return Ok(());
                }

                if let Some(responsible) = own.find_responsible_by_request(&request.request) {
                    responsible.send_message(DBMessage::Request(request))?
                } else {
                    let moved_error = OwnedFrame::SimpleError {
                        data: own.moved_error(&request.request)?,
                        attributes: None,
                    };

                    NodeManagerActor::reply_to(&request.reply_to, moved_error.into())?;
                }
            }
            QueryAddress(reply) => reply.send(own.redis_host.clone())?,
            IndexUpdate(keyspace, node_manager_ref) => {
                info!(
                    "Actor {}:{} updated their keyspace to {keyspace}",
                    node_manager_ref.host_ip, node_manager_ref.host_port
                );
                own.update_index(vec![(keyspace, node_manager_ref)])
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
