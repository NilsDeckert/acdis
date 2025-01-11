use std::net::SocketAddr;
use log::{error, warn};
use tokio::net::TcpStream;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use ractor::rpc::CallResult;
use redis_protocol::resp3::decode;
use redis_protocol_bridge::util::convert::AsFrame;
use crate::parse_actor::parse_request_actor::ParseRequestActor;
use crate::parse_actor::parse_request_message::ParseRequestMessage;
use crate::{send_tcp_reply, write_stream_to_buf};

pub struct TcpConnectionHandler;

#[async_trait]
impl Actor for TcpConnectionHandler {
    type Msg = (TcpStream, SocketAddr);
    type State = ();
    type Arguments = ();

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let (mut stream, _addr) = message;
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
        Ok(())
    }
}