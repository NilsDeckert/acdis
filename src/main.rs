extern crate core;

use ractor::Actor;

use log::{Level, LevelFilter};
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
        .set_target_level(LevelFilter::Error)
        .build();

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Info,  logconfig.clone(), TerminalMode::Mixed, ColorChoice::Auto),
            TermLogger::new(LevelFilter::Warn,  logconfig.clone(), TerminalMode::Mixed, ColorChoice::Auto),
            TermLogger::new(LevelFilter::Error, logconfig.clone(), TerminalMode::Mixed, ColorChoice::Auto),
            // TermLogger::new(LevelFilter::Debug, Config::default(), TerminalMode::Mixed, ColorChoice::Auto),
        ]
    ).unwrap();
}

#[tokio::main]
async fn main() {
    setup_logging();

    let (_tcp_actor, tcp_handler) = Actor::spawn(
        Some(String::from("TcpListenerActor")),
        TcpListenerActor,
        String::from("0.0.0.0:6379")).await.expect("Failed to spawn tcp listener actor");

    tcp_handler.await.unwrap();
}
