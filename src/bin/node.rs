#![allow(unused_imports)]

use futures::future::join_all;
use log::{info, warn, Level, LevelFilter};
use ractor::{call, pg, Actor, ActorCell, ActorRef};
use ractor_cluster::node::NodeConnectionMode;
use simplelog::{Color, ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode};
use std::cmp::Ordering;
use std::ops::Range;

use acdis::db_actor::actor::DBActor;
use acdis::db_actor::message::DBMessage;
use acdis::node_manager_actor::actor::{NodeManagerActor, NodeType};

use rand::Rng;

// CLI argument parsing
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {

    /// Address of the cluster manager
    #[arg(long, default_value_t=String::from("localhost"))]
    host: String,

    /// Port for internode communication
    #[arg(short, long, default_value_t=16379)]
    port: u16,
}

fn setup_logging() {
    let logconfig = ConfigBuilder::new()
        .set_level_color(Level::Error, Some(Color::Red))
        .set_level_color(Level::Warn, Some(Color::Yellow))
        .set_level_color(Level::Info, Some(Color::Green))
        .set_level_color(Level::Debug, Some(Color::Blue))
        .set_target_level(LevelFilter::Info)
        .build();

    // For release builds, only print warnings and errors
    let level_filter = if cfg!(debug_assertions) {
        LevelFilter::Info
    } else {
        LevelFilter::Warn
    };

    CombinedLogger::init(vec![TermLogger::new(
        level_filter,
        logconfig.clone(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();
}

#[tokio::main]
async fn main() {
    setup_logging();

    let args = Args::parse();

    let (manager_ref, manager_handler) = Actor::spawn(
        //Some(String::from("ClusterNodeManager")), // Same name leads to problems
        None,
        NodeManagerActor,
        (NodeType::Client, args.host, args.port),
    )
    .await
    .expect("Failed to spawn node manager");

    tokio::signal::ctrl_c()
        .await
        .expect("Failed waiting for ctrl c");

    warn!("Received ctrl+c. Terminating....");
    manager_ref.kill();
    // manager_ref.stop(Some(String::from("Node shutting down")));
    manager_handler.await.unwrap();
}
