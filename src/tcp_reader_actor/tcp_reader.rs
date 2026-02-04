use crate::parse_actor::parse_request_actor::ParseRequestActor;
use crate::parse_actor::parse_request_message::{ParseRequestFrame, ParseRequestMessage};
use log::{debug, error, info, warn};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use redis_protocol::resp3::decode;
use redis_protocol_bridge::util::convert::SerializableFrame;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;

const MAX_MSG_LEN: usize = 2048;

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
#[derive(RactorMessage)]
pub struct TcpReaderMessage;

#[async_trait]
impl Actor for TcpReaderActor {
    type Msg = TcpReaderMessage;
    type State = (OwnedReadHalf, ActorRef<SerializableFrame>, String);
    type Arguments = (OwnedReadHalf, ActorRef<SerializableFrame>, String);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        myself.cast(TcpReaderMessage)?;
        Ok(args)
    }

    async fn post_stop(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        myself.get_children().iter().for_each(|child| {
            info!(
                "Stopping {}...",
                child.get_name().unwrap_or(String::from("child"))
            );
            child.stop(Some("TCP Reader stopped.".into()));
        });
        Ok(())
    }

    // TODO: Because of this loop{}, we only query the mailbox once, thus cannot handle supervisor messages
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Begin reading from tcp stream...");
        let (stream, writer, identifier) = state;

        let (parse_ref, parse_handle) = Actor::spawn(
            None,
            ParseRequestActor,
            identifier.clone(),
        )
        .await
        .expect("Error spawning ParseRequestActor");

        loop {
            stream.readable().await.unwrap();

            // TODO: Check what happens if we receive != 1 frame
            // Shift this by amount of bytes received
            let mut buf = [0; MAX_MSG_LEN];
            if !write_stream_to_buf(stream, &mut buf).await {
                break;
            }

            let mut start = 0;
            while let Ok(Some((frame, size))) = decode::complete::decode(&buf[start..]) {
                parse_ref.cast(ParseRequestMessage::Frame(ParseRequestFrame {
                    frame: SerializableFrame(frame),
                    reply_to: identifier.to_string(),
                }))?;

                start += size;
            }
        }
        parse_ref.stop(Some("Channel was closed.".into()));
        parse_handle.await?;
        writer.stop(Some("Channel was closed.".into()));
        myself.stop(Some("Channel was closed.".into()));
        Ok(())
    }
}

/// Read the TcpStream and write its contents to a buffer.
///
/// ## Returns:
/// - True: If successful
/// - False: Otherwise
async fn write_stream_to_buf(stream: &mut OwnedReadHalf, buf: &mut [u8; MAX_MSG_LEN]) -> bool {
    match stream.read(buf).await {
        Ok(0) => {
            info!("Client closed channel {}", stream.peer_addr().unwrap());
        }
        Ok(_) => return true,
        Err(e) => {
            error!("Error reading from socket: {e}");
        }
    };
    false
}
