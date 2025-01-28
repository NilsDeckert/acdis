extern crate core;

use ractor::{cast, Actor};

use log::{debug, info, Level, LevelFilter};
use ractor_cluster::node::{NodeConnectionMode, NodeServerSessionInformation};
use ractor_cluster::{NodeEventSubscription, NodeServer};
use ractor_cluster::NodeServerMessage::SubscribeToEvents;
use simplelog::{Color, ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode};

use crate::tcp_listener_actor::tcp_listener::TcpListenerActor;

mod db_actor;
mod parse_actor;
mod tcp_listener_actor;
mod tcp_reader_actor;
mod tcp_writer_actor;

fn setup_logging() {
    let logconfig = ConfigBuilder::new()
        .set_level_color(Level::Error, Some(Color::Red))
        .set_level_color(Level::Warn, Some(Color::Yellow))
        .set_level_color(Level::Info, Some(Color::Green))
        .set_level_color(Level::Debug, Some(Color::Blue))
        .set_target_level(LevelFilter::Debug)
        .build();

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Debug,  logconfig.clone(), TerminalMode::Mixed, ColorChoice::Auto),
        ]
    ).unwrap();
}

struct subscription;

impl NodeEventSubscription for subscription {
    fn node_session_opened(&self, ses: NodeServerSessionInformation) {
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
        // info!("Session disconnected: {:#?}", ses.node_id);
    }

    fn node_session_authenicated(&self, ses: NodeServerSessionInformation) {
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

#[tokio::main]
async fn main() {
    setup_logging();
    debug!("HALLO");

    let server = NodeServer::new(
        std::env::var("CLUSTER_PORT").unwrap_or(String::from("6381")).parse().unwrap(),
        std::env::var("CLUSTER_COOKIE").unwrap_or(String::from("cookie")),
        String::from("host_node"),
        gethostname::gethostname().into_string().unwrap(),
        None,
        Some(NodeConnectionMode::Transitive)
    );

    let (pmd_ref, _pmd_handler) = Actor::spawn(
        None,
        server,
        ()
    ).await.expect("Failed to spawn port mapper daemon");

    cast!(pmd_ref, SubscribeToEvents{
        id: String::from("subscription"),
        subscription: Box::new(subscription)}
    ).expect("Failed to send subscription msg");

    let (_tcp_actor, tcp_handler) = Actor::spawn(
        Some(String::from("TcpListenerActor")),
        TcpListenerActor,
        String::from("0.0.0.0:6379")).await.expect("Failed to spawn tcp listener actor");

    tcp_handler.await.unwrap();
}
