use log::{debug, info};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use redis_protocol::resp3::encode;
use redis_protocol::resp3::types::{OwnedFrame, Resp3Frame};
use redis_protocol_bridge::util::convert::SerializableFrame;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;

/// This actor handles the writing part of a [`tokio::net::TcpStream`].
/// It is spawned with a [`OwnedWriteHalf`], receives [`OwnedFrame`]s, serializes them
/// and writes the result to its stream.
/// 
/// On spawn, it joins a [`ractor::pg`] process group with the name of the [`OwnedWriteHalf`]s peer_addr.
/// This is used to write Resp3 Frames to the client by sending them to this actor.
pub struct TcpWriterActor;

#[async_trait]
impl Actor for TcpWriterActor {
    type Msg = SerializableFrame;
    type State = OwnedWriteHalf;
    type Arguments = OwnedWriteHalf;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        write_half: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // Join process group to facilitate cross-node addressing
        debug!("Joining pg {}", write_half.peer_addr().unwrap().to_string());
        ractor::pg::join(
            write_half.peer_addr().unwrap().to_string(),
            vec![myself.get_cell()],
        );

        Ok(write_half)
    }

    async fn post_stop(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        info!("Stopping...");
        let supervisor = myself.try_get_supervisor();
        if let Some(supervisor) = supervisor {
            info!("Found supervisor");
            supervisor.stop(Some(String::from("Write actor for this connection was stopped.")))
        }
        Ok(())
    }


    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        write_half: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Received frame to write onto stream");
        send_tcp_reply(write_half, message.0).await;
        Ok(())
    }
}

/// Serialize reply frame and write it to the tcp stream
async fn send_tcp_reply(stream: &mut OwnedWriteHalf, reply: OwnedFrame) {
    // debug!("Sending out reply: {:#?}", reply);

    let mut encoded: Vec<u8> = vec![0u8; reply.encode_len(false)];
    encode::complete::encode(&mut encoded, &reply, false).expect("Failed to encode");

    stream
        .write_all(&encoded)
        .await
        .expect("Failed to send reply");

    stream.flush().await.unwrap()
}
