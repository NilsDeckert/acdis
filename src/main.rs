extern crate core;

use ractor::Actor;

use crate::node_manager_actor::actor::{NodeManagerActor, NodeType};
use crate::tcp_listener_actor::tcp_listener::TcpListenerActor;
#[allow(unused_imports)]
use log::{debug, info, Level, LevelFilter};
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

    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Info,
        logconfig.clone(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();
}

#[tokio::main]
async fn main() {
    setup_logging();

    let (_manager_ref, _manager_handler) = Actor::spawn(None, NodeManagerActor, NodeType::Server)
        .await
        .expect("Failed to spawn node manager");

    let (_tcp_actor, tcp_handler) = Actor::spawn(
        Some(String::from("TcpListenerActor")),
        TcpListenerActor,
        String::from("0.0.0.0:6379"),
    )
    .await
    .expect("Failed to spawn tcp listener actor");

    tcp_handler.await.unwrap();
}
