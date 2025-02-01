use std::ops::Range;
use async_trait::async_trait;
use futures::future::join_all;
use log::info;
use ractor::{cast, pg, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor::SupervisionEvent::*;
use ractor_cluster::node::{NodeConnectionMode, NodeServerSessionInformation};
use ractor_cluster::{NodeEventSubscription, NodeServer};
use ractor_cluster::NodeServerMessage::SubscribeToEvents;
use rand::Rng;
use crate::db_actor::actor::DBActor;

pub struct NodeManagerActor;

#[allow(dead_code)]
pub enum NodeType {
    Server,
    Client
}

#[async_trait]
impl Actor for NodeManagerActor {
    type Msg = ();
    type State = Range<u64>;
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
        NodeManagerActor::subscribe_to_events(myself, pmd_ref.clone()).await;
        
        // Connect to "Server"
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
        
        Ok(0u64..u64::MAX)
    }

    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let pg_name = String::from("acdis_node_managers");
        
        // TODO: Transitive node connection doesn't seem to work,
        //       we only find directly connected nodes here.
        let nodes = pg::get_members(&pg_name);
        info!("Found {} nodes", nodes.len());
        for node in nodes {
            let actor_ref = ActorRef::<()>::from(node);
            let name = actor_ref.get_name().unwrap_or(actor_ref.get_id().to_string());
            info!(" - {name}")
        }

        // Join later to avoid sending messages to ourselves
        pg::join(pg_name.clone(), vec![myself.get_cell()]);
        
        // TODO: Move this into handle()
        NodeManagerActor::spawn_db_actors(state.clone(), 8, myself.clone()).await;
        
        // TODO: Send message to ourselves to spawn initial actors
        
        Ok(())
    }

    async fn handle(&self, _myself: ActorRef<Self::Msg>, _message: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
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
    
    async fn subscribe_to_events(myself: ActorRef<()>, pmd_ref: ActorRef<ractor_cluster::node::NodeServerMessage>) {
        // Trigger methods when other nodes connect / disconnect
        cast!(pmd_ref, SubscribeToEvents{
                    id: String::from("Subscription"),
                    subscription: Box::new(Subscription(myself))}
                ).expect("Failed to send Subscription msg")
    }

    /// Divide the value range 0..[`u64::MAX`] into equally sized parts.
    ///
    /// # Arguments 
    ///
    /// * `chunks`: Number of chunks to return. MUST be power of two.
    ///
    /// returns: Vec<(u64, u64), Global> 
    /// TODO: Accept range to split
    fn chunk_ranges(chunks: u64) -> Vec<Range<u64>> {

        assert!(chunks.is_power_of_two());

        let values_per_chunk = u64::MAX / chunks;
        let mut ranges: Vec<Range<u64>> = Vec::new();

        (0..chunks).for_each(|i| {
            let start = i * values_per_chunk;
            let end = if i == chunks - 1 { u64::MAX } else { start + values_per_chunk - 1 };

            ranges.push(start .. end);
        });

        ranges
    }
    
    /// Spawn and link DB actors
    async fn spawn_db_actors(_range: Range<u64>, actors_to_join: u64, supervisor: ActorRef<()>) {
        let mut handles = Vec::new();
        for range in NodeManagerActor::chunk_ranges(actors_to_join) {
            handles.push(tokio::spawn({
                Actor::spawn_linked(
                    Some(format!("DBActor {:#018x}..{:#018x}", range.start, range.end)),
                    DBActor,
                    range,
                    supervisor.get_cell()
                )
            }));
        }
        join_all(handles).await;
    }
}
struct Subscription(ActorRef<()>);

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

    fn node_session_disconnected(&self, _ses: NodeServerSessionInformation) {
        // info!("Session disconnected: {:#?}", ses.node_id);
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
