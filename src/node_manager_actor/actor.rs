use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Range;
use async_trait::async_trait;
use futures::future::join_all;
use log::{debug, info, warn};
use ractor::{call, cast, pg, Actor, ActorCell, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor::SupervisionEvent::*;
use ractor_cluster::node::{NodeConnectionMode, NodeServerSessionInformation};
use ractor_cluster::{NodeEventSubscription, NodeServer};
use ractor_cluster::NodeServerMessage::SubscribeToEvents;
use rand::Rng;
use crate::db_actor::actor::DBActorArgs;
use crate::db_actor::actor::PartitionedHashMap;
use crate::db_actor::actor::DBActor;
use crate::db_actor::message::DBMessage;
use crate::node_manager_actor::message::NodeManagerMessage;
use crate::node_manager_actor::message::NodeManagerMessage::{*};


pub struct NodeManagerActor;

pub struct NodeManageActorState {
    keyspace: Range<u64>,
    db_actors: HashMap<Range<u64>, ActorRef<DBMessage>>,
}

#[allow(dead_code)]
pub enum NodeType {
    Server,
    Client
}

#[async_trait]
impl Actor for NodeManagerActor {
    type Msg = NodeManagerMessage;
    type State = NodeManageActorState;
    type Arguments = NodeType;
    
    async fn pre_start(&self, myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ractor::ActorProcessingErr> {
        let cluster_host_address    = std::env::var("CLUSTER_HOST")
                                        .unwrap_or("127.0.0.1".to_string());
        let cluster_host_port       = std::env::var("CLUSTER_PORT")
                                        .unwrap_or(String::from("6381")).parse().unwrap();
        
        let port;
        let name;
        
        // Set arguments that differ between server and client
        match args {
            NodeType::Server => {
                port = cluster_host_port;
                name = String::from("host_node");
            },
            NodeType::Client => {
                let mut rng = rand::thread_rng();
                port = 0; // Let OS choose a port
                name = myself.get_name().unwrap_or(format!("Node {}", rng.gen::<u8>()));
            }
        }
        
        let pmd_ref = NodeManagerActor::spawn_pmd(port, name).await;
        NodeManagerActor::subscribe_to_events(myself.clone(), pmd_ref.clone()).await;
        
        // If NodeType is Client, we assume there is already another NodeServer accepting connections
        if let NodeType::Client = args {
            ractor_cluster::client_connect(
                &pmd_ref,
                format!("{}:{}",
                        cluster_host_address,
                        cluster_host_port)
            ).await.expect("Failed to connect to node server");
        }
        
        // Wait to establish connection
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        myself.send_message(Init)?;
            
        Ok(NodeManageActorState {
            keyspace: 0u64..u64::MAX,
            db_actors: HashMap::new()
        })
        
    }

    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn handle(&self, myself: ActorRef<Self::Msg>, message: Self::Msg, own: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match message {
            Init => {
                let pg_name = String::from("acdis_node_managers");

                // TODO: Transitive node connection doesn't seem to work,
                //       we only find directly connected nodes here.
                let mut nodes = pg::get_members(&pg_name);
                Self::sort_actors_by_keyspace(&mut nodes);
                
                if !nodes.is_empty() {
                    info!("Other NodeManagers present, attempting to adopt keyspace");
                    let node = nodes[0].clone(); // Node with the largest keyspace
                    let actor_ref = ActorRef::<NodeManagerMessage>::from(node);
                    
                    let keyspace = call!(actor_ref.clone(), QueryKeyspace).unwrap();
                    let (_, r2) = Self::halve_range(keyspace);
                    
                    // Inherit keys&values in that keyspace
                    let map = call!(actor_ref, AdoptKeyspace, r2).expect("Failed to adopt keyspace");
                    let keyspace = map.range.clone();
                    own.keyspace = keyspace.clone();
                    
                    // Only for testing. TODO: remove
                    info!("We now manage the following key-value-pairs:");
                    for (key, value) in map.map.clone() {
                        info!(" - {}:{}", key, value);
                    }
                    
                    own.db_actors = Self::spawn_db_actors(DBActorArgs{ map: Some(map), range: keyspace},
                                          8,
                                          myself.clone()).await;
                } else {
                    info!("Could not find any other NodeManager");
                    own.db_actors = Self::spawn_db_actors(DBActorArgs { map: None, range: 0u64..u64::MAX }, 8, myself.clone()).await;
                }
                
                // Join later to avoid sending messages to ourselves
                pg::join(pg_name.clone(), vec![myself.get_cell()]);
                
                // TODO: Remove
                let pg_name2 = String::from("acdis");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                debug!("Summary: DB Actors in pg:");
                for a in pg::get_members(&pg_name2) {
                    debug!("- {}", a.get_name().unwrap_or(String::from("Unnamed Actor")));
                    debug!("  - Type DBMessage? {:?}", a.is_message_type_of::<DBMessage>().unwrap());
                }
            },
            QueryKeyspace(reply) => {
                reply.send(own.keyspace.clone())?;
            },
            SetKeyspace(range) => {
                // TODO
                info!("Setting keyspace to {:#?}", range);
                own.keyspace = range;
                if myself.get_children().is_empty() {
                    own.db_actors = NodeManagerActor::spawn_db_actors(DBActorArgs { map: None, range: own.keyspace.clone() }, 8, myself.clone()).await;
                }
            },
            AdoptKeyspace(keyspace, reply) => {
                info!("{} Giving away keyspace {:#?}", 
                    myself.get_name().unwrap_or(String::from("node_manager")),
                    keyspace);
                
                assert_ne!(keyspace, own.keyspace); // Don't give up whole keyspace.
                // Keyspace must be at one end of our keyspace
                assert!((keyspace.start == own.keyspace.start && keyspace.end < own.keyspace.end) 
                     || (keyspace.start > own.keyspace.start  && keyspace.end == own.keyspace.end));

                let mut return_map = PartitionedHashMap{
                    map: HashMap::new(),
                    range: keyspace.clone()
                };
                
                info!("I manage {} db_actors", own.db_actors.len());
                
                // Assumption: keyspace >= actor_keyspace
                for (actor_keyspace, actor) in &own.db_actors {
                    
                    // Keyspace of this actor is completely inside
                    // the requested keyspace
                    if actor_keyspace.start >= keyspace.start 
                        && actor_keyspace.end <= keyspace.end {
                        // Kill actor and fetch HashMap
                        info!("'Killing' actor {:?} for keyspace {:#?}", actor, actor_keyspace);
                        let actor_hashmap = call!(actor, DBMessage::Drain);
                        return_map.map.extend(actor_hashmap.unwrap());
                    } else {
                        warn!("Did not cover this case:\n\
                        The keyspace of this actor ({:#018x}..{:#018x}) is not fully inside the \
                        requested keyspace ({:#018x}..{:#018x})", 
                            actor_keyspace.start, actor_keyspace.end, keyspace.start, keyspace.end)
                    }
                }
                
                reply.send(return_map)?;
            }
        }
        Ok(())
    }
    
    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, event: SupervisionEvent, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match event {
            ActorStarted(_cell) => {},
            ActorFailed(_cell, _error) => {},
            ActorTerminated(_cell, _last_state, _reason) => {},
            ProcessGroupChanged(_message) => {},
            _ => {}
        }
        Ok(())
    }
}

impl NodeManagerActor {
    /// Initialize and spawn a [`NodeServer`], thus accepting connections for communication across
    /// nodes.
    async fn spawn_pmd(port: u16, name: String) -> ActorRef<ractor_cluster::node::NodeServerMessage>{
        // Init port mapper daemon that handles internode communication
        let pmd = NodeServer::new(
            port,
            std::env::var("CLUSTER_COOKIE").unwrap_or(String::from("cookie")),
            name,
            gethostname::gethostname().into_string().unwrap(),
            None,
            Some(NodeConnectionMode::Transitive)
        );

        // Spawn pmd, thus starting server/client
        let (pmd_ref, _pmd_handler) = Actor::spawn(None, pmd,()).await
            .expect("Failed to spawn port mapper daemon");
        
        pmd_ref
    }
    
    async fn subscribe_to_events(myself: ActorRef<NodeManagerMessage>, pmd_ref: ActorRef<ractor_cluster::node::NodeServerMessage>) {
        // Trigger methods when other nodes connect / disconnect
        cast!(pmd_ref, SubscribeToEvents{
                    id: String::from("Subscription"),
                    subscription: Box::new(Subscription(myself))}
                ).expect("Failed to send Subscription msg")
    }

    /// Divide a given [`Range`] into equally sized parts.
    ///
    /// # Arguments 
    /// * `range`: Keyspace to split
    /// * `chunks`: Number of chunks to return.
    ///
    /// returns: Vec<(u64, u64), Global> 
    fn chunk_ranges(range: Range<u64>, chunks: u64) -> Vec<Range<u64>> {
        let size = range.end - range.start;

        let values_per_chunk = size / chunks;
        let mut ranges: Vec<Range<u64>> = Vec::new();

        let mut start = range.start;
        
        for i in 0..chunks {
            let mut end = start + values_per_chunk;
            if i == chunks - 1 {
                end += size%chunks;
            }
            ranges.push(start..end);
            start = end;
        }

        ranges
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
    async fn spawn_db_actors(args: DBActorArgs, actors_to_join: u64, supervisor: ActorRef<NodeManagerMessage>) -> HashMap<Range<u64>, ActorRef<DBMessage>> {
        let mut ret_map: HashMap<Range<u64>, ActorRef<DBMessage>> = HashMap::new();
        info!("Spawning {} DB actors for range {:#?}", actors_to_join, args.range);
        
        let mut initial_maps = vec!();
        let ranges = NodeManagerActor::chunk_ranges(args.range.clone(), actors_to_join);

        if args.map.is_some() {
            for range in ranges {
                initial_maps.push(
                    PartitionedHashMap{map: HashMap::new(), range}
                )
            }

            if let Some(mut argsmap) = args.map {
                for (key, value) in argsmap.map.drain() {
                    for map in &mut initial_maps {
                        if map.in_range(&key) {
                            map.map.insert(key, value);
                            break;
                        }
                    }
                }
            }
            
            for map in initial_maps {
                let range = map.range.clone();
                let (actor_ref, _handle) = Actor::spawn_linked(
                    Some(format!("DBActor {:#018x}..{:#018x}", range.start, range.end)),
                    DBActor,
                    DBActorArgs{map: Some(map), range: range.clone()},
                    supervisor.get_cell()
                ).await.expect("Failed to spawn DBActor");
                
                info!("Is newly spawned db_actor of type DBMessage? {:?}", actor_ref.is_message_type_of::<DBMessage>());

                ret_map.insert(range, actor_ref);
            }
        } else {
            for range in ranges {
                let (actor_ref, _handle) = Actor::spawn_linked(
                    Some(format!("DBActor {:#018x}..{:#018x}", range.start, range.end)),
                    DBActor,
                    DBActorArgs{map: None, range: range.clone()},
                    supervisor.get_cell()
                ).await.expect("Failed to spawn DBActor");

                ret_map.insert(range, actor_ref);
            }
        }
        
        ret_map
    }
    
    /// Given a range, split it and return both halves
    fn halve_range(range: Range<u64>) -> (Range<u64>, Range<u64>) {
        let mid = range.start + ((range.end - range.start) / 2);
        (range.start .. mid, mid .. range.end)
    }

    fn sort_actors_by_keyspace(actors: &mut Vec<ActorCell>) {
        actors.sort_by(|actor_a, actor_b| {
            let ref_actor_a = ActorRef::<NodeManagerMessage>::from(actor_a.clone());
            let ref_actor_b = ActorRef::<NodeManagerMessage>::from(actor_b.clone());

            futures::executor::block_on(async {
                let keyspace_a: Range<u64> = call!(ref_actor_a, QueryKeyspace).unwrap();
                let keyspace_b: Range<u64> = call!(ref_actor_b, QueryKeyspace).unwrap();

                return if (keyspace_a.end - keyspace_a.start) > (keyspace_b.end - keyspace_b.start) {
                    Ordering::Greater
                } else if (keyspace_a.end - keyspace_a.start) < (keyspace_b.end - keyspace_b.start) {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
        });
    }
    
}
struct Subscription(ActorRef<NodeManagerMessage>);

impl NodeEventSubscription for Subscription {
    fn node_session_opened(&self, _ses: NodeServerSessionInformation) {
        // info!("Session opened: \n\
        //     node_id:    {:#?} \n\
        //     peer_addr:  {:#?} \n\
        //     peer_name:  {:#?} \n\
        //     is_server:  {:#?}",
        //     ses.node_id, ses.peer_addr, ses.peer_name, ses.is_server);
        // 
        // let registered = ractor::registry::registered();
        // info!("Registered: {:#?}", registered);
        // 
        // let pids = ractor::registry::get_all_pids();
        // info!("Pids: {:#?}", pids);
        // info!("\n\n\n\n\n\n")
    }

    fn node_session_disconnected(&self, ses: NodeServerSessionInformation) {
        info!("Session disconnected: {:#?}", ses.node_id);
    }

    fn node_session_authenicated(&self, _ses: NodeServerSessionInformation) {
        // info!("Session authenticated: {:#?}", ses.node_id);
    }

    fn node_session_ready(&self, ses: NodeServerSessionInformation) {
        info!("Session ready: {:?}", ses.peer_name);
        // info!("Session ready: \n\
        //     node_id:    {:#?} \n\
        //     peer_addr:  {:#?} \n\
        //     peer_name:  {:#?} \n\
        //     is_server:  {:#?}",
        //     ses.node_id, ses.peer_addr, ses.peer_name, ses.is_server);
        // 
        // let registered = ractor::registry::registered();
        // info!("Registered: {:#?}", registered);
        // 
        // let pids = ractor::registry::get_all_pids();
        // info!("Pids: {:#?}", pids);
    }
}
