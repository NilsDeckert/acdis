extern crate core;

use log::{debug, error, warn, Level, LevelFilter};
use ractor::Actor;
use simplelog::{Color, ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use redis_protocol::resp3::*;
use redis_protocol::resp3::types::*;
use crate::tcp_listener_actor::tcp_listener::TcpListenerActor;

mod db_actor;
mod parse_actor;
mod tcp_listener_actor;
mod tcp_connection_handler_actor;

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

/// Read the TcpStream and write its contents to a buffer.
/// 
/// ## Returns:
/// - True: If successful
/// - False: Otherwise
async fn write_stream_to_buf(stream: &mut TcpStream, buf: &mut [u8; 512]) -> bool {
    match stream.read(buf).await {
        Ok(0) => {
            warn!("Client closed channel");
        }
        Ok(_) => {return true}
        Err(e) => {
            error!("Error reading from socket: {}", e);
        }
    };
    false
}

/// Serialize reply frame and write it to the tcp stream
async fn send_tcp_reply(stream: &mut TcpStream, reply: OwnedFrame) {
    debug!("Sending out reply: {:#?}", reply);

    let mut encoded: Vec<u8> = vec![0u8; reply.encode_len(false)];
    encode::complete::encode(
        &mut encoded,
        &reply,
        false).expect("Failed to encode");

    stream.write_all(&encoded)
        .await
        .expect("Failed to send reply");

    stream.flush().await.unwrap()
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
