extern crate core;

use ractor::Actor;

use crate::node_manager_actor::actor::{NodeManagerActor, NodeType};
use crate::tcp_listener_actor::tcp_listener::TcpListenerActor;
use log::{Level, LevelFilter};
use simplelog::{Color, ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode};

mod db_actor;
mod node_manager_actor;
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

    let (_manager_ref, manager_handler) = Actor::spawn(
        Some(String::from("NodeManager")),
        NodeManagerActor,
        NodeType::Server,
    )
    .await
    .expect("Failed to spawn node manager");

    manager_handler.await.expect("Failed to join node manager");
}
