use std::ops::Range;
use futures::future::join_all;
use log::{info};
use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor::SupervisionEvent::*;
use tokio::net::TcpListener;
use crate::db_actor::actor::DBActor;
use crate::tcp_reader_actor::tcp_reader::TcpReaderActor;
use crate::tcp_writer_actor::tcp_writer::TcpWriterActor;

pub struct TcpListenerActor;

struct CompanionActor;
pub struct TcpConnectionMessage {
    pub tcp_stream: tokio::net::TcpStream,
    pub socket_addr: core::net::SocketAddr,
}

#[async_trait]
impl Actor for CompanionActor {
    type Msg = ();
    type State = (TcpListener, ActorRef<TcpConnectionMessage>);
    type Arguments = (TcpListener, ActorRef<TcpConnectionMessage>);

    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    async fn post_start(&self, _myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let (listener, send_to) = state;
        
        loop {
            let (tcp_stream, socket_addr) = listener.accept().await?;
            info!("Accepting connection from: {}", socket_addr);
            cast!(send_to, TcpConnectionMessage{tcp_stream, socket_addr})?;
        }
    }
}

#[async_trait]
impl Actor for TcpListenerActor {
    type Msg = TcpConnectionMessage;
    type State = ();
    type Arguments = String;

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, address: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {

        /* Number of chunk to split key space into */
        let chunks = 2_u64.pow(3);
        let mut handles = Vec::new();

        info!("Spawning initial DB actors");
        for range in chunk_ranges(chunks) {
            handles.push(tokio::spawn({
                Actor::spawn_linked(
                    Some(format!("DBActor {:#018x}..{:#018x}", range.start, range.end)),
                    DBActor,
                    range,
                    myself.get_cell()
                )
            }));
        }
        join_all(handles).await;
        
        /* Open TCP port to accept connections */
        info!("Listening on {}", address);
        let listener = TcpListener::bind(address)
            .await
            .expect("Failed to open TCP Listener");
        
        // Spawn an actor that polls the TcpListener for available connections
        Actor::spawn_linked(
            Some(String::from("TCP Poller")),
            CompanionActor,
            (listener, myself.clone()),
            myself.get_cell()
        ).await.expect("Failed to spawn TCP poller");

        Ok(())
    }

    async fn handle(&self, myself: ActorRef<Self::Msg>, connection: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let TcpConnectionMessage{ tcp_stream, socket_addr} = connection;

        // Split stream into reader and writer to allow for concurrent reading and writing
        let (reader, writer) = tcp_stream.into_split();

        let (write_actor, _write_actor_handle) = Actor::spawn_linked(
            Some(format!("Stream Writer {}", socket_addr)),
            TcpWriterActor,
            writer,
            myself.get_cell()
        ).await?;

        // Clients usually keep their connections open. This means the spawned connection handler
        // is alive and working until the client disconnects. -> It is okay to spawn a fresh actor
        // for each client connection.
        let (read_actor, _read_actor_handle) = Actor::spawn_linked(
            Some(format!("Stream Reader {}", socket_addr)),
            TcpReaderActor,
            (reader, write_actor.clone()),
            myself.get_cell()
        ).await?;
        
        // Let the read_actor supervise the write actor. This simplifies stopping the write_actor
        // when the stream is closed.
        write_actor.link(read_actor.get_cell());
        
        Ok(())
    }

    /// This handles supervision events for both DBActors and TcpConnectionHandler Actors 
    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, event: SupervisionEvent, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match event {
            ActorStarted(cell) => {
                info!("{} started", cell.get_name().unwrap_or(String::from("Actor")))
            },
            ActorTerminated(cell, _last_state, _reason) => {
                info!("{} stopped", cell.get_name().unwrap_or(String::from("Actor")));
            },
            ActorFailed(_cell, _error) => {},
            ProcessGroupChanged(_message) => {}
        }
        Ok(())
    }
}

/// Divide the value range 0..[`u64::MAX`] into equally sized parts.
///
/// # Arguments 
///
/// * `chunks`: Number of chunks to return. MUST be power of two.
///
/// returns: Vec<(u64, u64), Global> 
fn chunk_ranges(chunks: u64) -> Vec<Range<u64>> {

    assert!(chunks.is_power_of_two());

    let values_per_chunk = u64::MAX / chunks;
    let mut ranges: Vec<Range<u64>> = Vec::new();

    (0..chunks).for_each(|i| {
        let start = i * values_per_chunk;
        let end = if i == chunks - 1 { u64::MAX } else { start + values_per_chunk - 1 };

        ranges.push(start .. end);
    });

    ranges
}
