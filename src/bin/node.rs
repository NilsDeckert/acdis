#![allow(unused_imports)]
use futures::future::join_all;
use log::{info, warn, Level, LevelFilter};
use ractor::{call, pg, Actor, ActorCell, ActorRef};
use ractor_cluster;
use ractor_cluster::node::NodeConnectionMode;
use simplelog::{Color, ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode};
use std::cmp::Ordering;
use std::ops::Range;

use acdis::db_actor::actor::DBActor;
use acdis::db_actor::message::DBMessage;
use acdis::node_manager_actor::actor::{NodeManagerActor, NodeType};

use rand::Rng;

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

    let (manager_ref, manager_handler) = Actor::spawn(
        //Some(String::from("ClusterNodeManager")), // Same name leads to problems
        None,
        NodeManagerActor,
        NodeType::Client,
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
