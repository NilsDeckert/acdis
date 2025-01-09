use log::{debug, error, info, warn, Level, LevelFilter};
use ractor::Actor;
use simplelog::{Color, ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode};

use std::net::SocketAddr;
use ractor::rpc::CallResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use redis_protocol::resp3::*;
use redis_protocol::resp3::types::*;
use redis_protocol_bridge::util::convert::AsFrame;
use db_actor::actor::DBActor;
use crate::parse_actor::parse_request_actor::ParseRequestActor;
use crate::parse_actor::parse_request_message::ParseRequestMessage;

mod db_actor;
mod parse_actor;

fn setup_logging() {
    let logconfig = ConfigBuilder::new()
        .set_level_color(Level::Error, Some(Color::Red))
        .set_level_color(Level::Warn, Some(Color::Yellow))
        .set_level_color(Level::Info, Some(Color::Green))
        .set_target_level(LevelFilter::Info)
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

async fn handle_client(mut stream: TcpStream, addr: SocketAddr) {
    info!("Incoming connection from: {}", addr);
    loop {
        stream.readable().await.unwrap();
        let mut buf = [0; 512];
        if !write_stream_to_buf(&mut stream, &mut buf).await { break; }

        let res_op = decode::complete::decode(&mut buf);
        match res_op {
            Ok(
                Some((frame, _size))
            ) => {
                let (parse_ref, _parse_handle) = Actor::spawn(
                    None, ParseRequestActor, ()
                ).await.expect("Error spawning ParseRequestActor");

                let response = parse_ref.call(
                    |r| ParseRequestMessage{frame, caller: r},
                    None
                ).await.expect("Error handing off request to ParseRequestActor");

                let reply = match response {
                    CallResult::Success(response) => response,
                    CallResult::Timeout => "Request timed out".as_frame(),
                    CallResult::SenderError => "Error handling request".as_frame()
                };

                send_tcp_reply(&mut stream, reply).await;
            }
            Ok(None) => warn!("Received empty command"),
            Err(e) => error!("Error: {}", e),
        }
    }
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

/// Divide the value range 0..[`u64::MAX`] into equally sized parts.
/// 
/// # Arguments 
/// 
/// * `chunks`: Number of chunks to return. MUST be power of two.
/// 
/// returns: Vec<(u64, u64), Global> 
fn chunk_ranges(chunks: u64) -> Vec<(u64, u64)> {

    assert!(chunks.is_power_of_two());

    let values_per_chunk = u64::MAX / chunks;
    let mut ranges: Vec<(u64, u64)> = Vec::new();

    (0..chunks).for_each(|i| {
        let start = i * values_per_chunk;
        let end = if i == chunks - 1 { u64::MAX } else { start + values_per_chunk - 1 };

        ranges.push((start, end));
    });
    
    ranges
}

#[tokio::main]
async fn main() {
    setup_logging();

    /* Key space partitioning */
    let chunks = 2_u64.pow(3);

    info!("Spawning initial actors");
    for (start, end) in chunk_ranges(chunks) {
       let (_actor, _handler) = Actor::spawn(
           Some(format!("DB Actor ({:#x}, {:#x})", start, end)),
           DBActor,
           (start, end)
       ).await.expect("Failed to spawn db actor");
    }

    /* Setup redis port */
    // TODO: Do this using an actor, spawning other actors per session
    let address = "0.0.0.0:6379";
    let listener = TcpListener::bind(address)
        .await
        .expect("Failed to open TCP Listener");

    info!("Listening on {}", address);

    loop {
        let (tcp_stream, socket_addr) = listener.accept()
            .await
            .expect("Failed to accept connection");

        // TODO: Use actor here instead of tokio call
        // - Call actor::spawn instead of tokio::spawn()
        //  i.e. use actor that takes in a stream
        tokio::spawn(async move {
            handle_client(tcp_stream, socket_addr).await;
        });
    }
    
}
