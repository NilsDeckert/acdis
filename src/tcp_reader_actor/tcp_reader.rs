use log::{error, info, warn};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, Message};
use redis_protocol::resp3::decode;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::util::convert::SerializableFrame;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use crate::parse_actor::parse_request_actor::ParseRequestActor;
use crate::parse_actor::parse_request_message::ParseRequestMessage;

/// This actor handles the reading part of a [`tokio::net::TcpStream`].
///
/// It is spawned with a [`OwnedReadHalf`] and the [`crate::tcp_writer_actor`] handling the writing
/// part of that stream. It constantly queries the stream for RESP3 frames and decodes them upon arrival.
/// The decoded OwnedFrames are sent to a [`ParseRequestActor`] for further processing and handling
/// of the request.
///
/// The result of the request is sent to the [`crate::tcp_writer_actor`] directly without going
/// through this actor.
pub struct TcpReaderActor;

/// Dummy message that prompts us to start working
pub struct TcpReaderMessage;
impl Message for TcpReaderMessage {}

#[async_trait]
impl Actor for TcpReaderActor {
    type Msg = TcpReaderMessage;
    type State = (OwnedReadHalf, ActorRef<SerializableFrame>);
    type Arguments = (OwnedReadHalf, ActorRef<SerializableFrame>);

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        myself.cast(TcpReaderMessage)?;
        Ok(args)
    }

    async fn post_stop(&self, myself: ActorRef<Self::Msg>, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        myself.get_children().iter().for_each(|child| {
            info!("Stopping {}...", child.get_name().unwrap_or(String::from("child")));
            child.stop(Some("TCP Reader stopped.".into()));
        });
        Ok(())
    }

    // TODO: Because of this loop{}, we only query the mailbox once, thus cannot handle supervisor messages
    async fn handle(&self, myself: ActorRef<Self::Msg>, _message: Self::Msg, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let (stream, writer) = state;

        let (parse_ref, _parse_handle) = Actor::spawn(
            None, ParseRequestActor, ()
        ).await.expect("Error spawning ParseRequestActor");
        
        loop {

            stream.readable().await.unwrap();
            
            // TODO: Check what happens if we receive != 1 frame
            // Shift this by amount of bytes received
            let mut buf = [0; 512];
            if !write_stream_to_buf(stream, &mut buf).await {
                break;
            }

            let res_op = decode::complete::decode(&mut buf);
            match res_op {
                Ok(
                    Some((frame, _size))
                ) => {

                    // This sends the request to the ParseRequestActor.
                    // The reply will be received and written onto the stream by `writer`
                    // TODO: parse_ref.cast(ParseRequestMessage{frame, reply_to: writer.clone() })?;
                }
                Ok(None) => warn!("Received empty command"),
                Err(e) => error!("Error: {}", e),
            }
        }
        Ok(myself.stop(Some("Channel was closed.".into())))
    }



}

/// Read the TcpStream and write its contents to a buffer.
///
/// ## Returns:
/// - True: If successful
/// - False: Otherwise
async fn write_stream_to_buf(stream: &mut OwnedReadHalf, buf: &mut [u8; 512]) -> bool {
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
