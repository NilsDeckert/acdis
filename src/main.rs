use db_actor::MapEntry;
use log::{debug, info, warn, error, Level, LevelFilter};
use ractor::{cast, call, Actor, ActorRef};
use redis_protocol::error::{RedisProtocolError, RedisProtocolErrorKind};
use simplelog::{Color, ColorChoice, CombinedLogger, Config, ConfigBuilder, TermLogger, TerminalMode};
use crate::db_actor::{DBActor, DBMessage};

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use redis_protocol::resp3::{*};
use redis_protocol::resp3::types::{*};
use redis_protocol_bridge::util::convert::AsFrame;
use redis_protocol_bridge::commands::{*};
use redis_protocol_bridge::commands::parse::Request;

mod db_actor;

fn setup_logging() {
    let logconfig = ConfigBuilder::new()
        .set_level_color(Level::Error, Some(Color::Red))
        .set_level_color(Level::Warn, Some(Color::Yellow))
        .set_level_color(Level::Info, Some(Color::Green))
        .set_target_level(LevelFilter::Info)
        .build();

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Warn, Config::default(), TerminalMode::Mixed, ColorChoice::Auto),
            TermLogger::new(LevelFilter::Info, logconfig, TerminalMode::Mixed, ColorChoice::Auto),
        ]
    ).unwrap();
}

// TODO: This should be handled by the actor
async fn get(key: String) -> Result<OwnedFrame, RedisProtocolError> {

    /* Pick one actor that could handle our request */
    let group_name = String::from("acdis");

    match ractor::pg::get_members(&group_name).first() {
        None => {
            return Err(
                RedisProtocolError::new(
                    RedisProtocolErrorKind::Unknown,
                    "No actor to process request"
                )
            )
        }
        Some(cell) => {
            /* Send request to actor */
            let call_result = call!(
                ActorRef::<DBMessage>::from(cell.clone()),
                |r| DBMessage::GET(key, r)
            );

            match call_result {
                Ok(res) => {
                    info!("Received reply: {}", res);
                    Ok(res.into())
                },
                Err(e) => {
                    Err(
                        RedisProtocolError::new(
                            RedisProtocolErrorKind::Unknown,
                            e.to_string()
                        )
                    )
                }
            }
        }
    }
}

// TODO: This should be handled by the actor
async fn set(key: String, value: String) -> Result<OwnedFrame, RedisProtocolError> {

    /* Pick one actor that could handle our request */
    let group_name = String::from("acdis");

    match ractor::pg::get_members(&group_name).first() {
        None => {
            return Err(
                RedisProtocolError::new(
                    RedisProtocolErrorKind::Unknown,
                    "No actor to process request"
                )
            )
        }
        Some(cell) => {
            /* Send request to actor */
            let call_result = cast!(
                ActorRef::<DBMessage>::from(cell.clone()),
                DBMessage::INSERT(key, MapEntry::STRING(value))
            );

            match call_result {
                Ok(()) => {
                    Ok("Ok".as_frame())
                },
                Err(e) => {
                    Err(
                        RedisProtocolError::new(
                            RedisProtocolErrorKind::Unknown,
                            e.to_string()
                        )
                    )
                }
            }
        }
    }
}

/// Dispatch command handlers.
/// 
/// For redis documentation on commands see [Commands](https://redis.io/docs/latest/commands/)
async fn handle_command(query: Vec<String>) -> Vec<u8> {
    let reply: OwnedFrame = match parse::parse(query) {
        Ok(request) => {
            debug!("{:?}", request);

            // TODO: Actors should be able to handle Requests enum instead of DBMessage
            let r = match request {
                Request::HELLO { .. } => hello::default_handle(request),
                Request::GET {key} => get(key).await,
                Request::SET {key, value} => set(key, value).await,
                Request::COMMAND { .. } => command::default_handle(request),
                Request::INFO { .. } => info::default_handle(request),
                Request::PING { .. } => ping::default_handle(request),
                Request::SELECT { .. } => select::default_handle(request)
            };

            r.unwrap_or_else(|err| {
                error!("{}", err.details().to_string());
                OwnedFrame::SimpleError {
                    data: err.details().to_string(),
                    attributes: None
                }
            })
        }
        
        Err(err) => {
            OwnedFrame::SimpleError {
                data: err.details().to_string(),
                attributes: None
            }
        }
    };

    debug!("Reply: {:#?}", reply);

    let mut buf: Vec<u8> = vec![0u8; reply.encode_len(false)];
    encode::complete::encode(
        &mut buf,
        &reply,
        false).expect("Failed to encode");
    buf

}

async fn handle_client(mut stream: TcpStream, addr: SocketAddr) {
    info!("Incoming connection from: {}", addr);
    loop {
        stream.readable().await.unwrap();
        let mut buf = [0; 512];

        match stream.read(&mut buf).await {
            Ok(0) => {
                warn!("Client closed channel");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!("Error reading from socket: {}", e);
                break;
            }
        };

        let res_op = decode::complete::decode(&mut buf);
        match res_op {
            Ok(Some((frame, _size))) => {
                let query = redis_protocol_bridge::parse_owned_frame(frame);
                info!("{:?}", query);
                let reply = handle_command(query).await;
                stream
                    .write_all(&reply)
                    .await
                    .expect("Failed to send reply");
                stream.flush().await.unwrap()
            }
            Ok(None) => warn!("Received empty command"),
            Err(e) => error!("Error: {}", e),
        }
    }
}

#[tokio::main]
async fn main() {
    setup_logging();
    //
    info!("Spawning initial actor");
    let (_dbactor, _handler) = Actor::spawn(
        Some("DB Actor".to_string()),
        DBActor,
        ()
    ).await.expect("Failed to spawn DBActor");
    //
    //dbactor.cast(
    //    DBMessage::INSERT(
    //        "key".to_string(),
    //        MapEntry::STRING("value".to_string())
    //    )
    //).expect("Failed to send message");
    //
    //
    //dbactor.cast(
    //    DBMessage::INSERT(
    //        "number".to_string(),
    //        MapEntry::USIZE(7usize)
    //    )
    //).expect("Failed to send message");
    //
    //get(&dbactor, "key".to_string()).await;
    //get(&dbactor, "number".to_string()).await;

    let address = "0.0.0.0:6379";
    let listener = TcpListener::bind(address)
        .await
        .expect("Failed to open TCP Listener");

    info!("Listening on {}", address);

    loop {
        let (tcp_stream, socket_addr) = listener.accept()
            .await
            .expect("Failed to accept connection");

        tokio::spawn(async move {
            handle_client(tcp_stream, socket_addr).await;
        });
    }
    
}
