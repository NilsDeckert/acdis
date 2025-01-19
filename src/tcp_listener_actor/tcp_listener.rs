use std::ops::Range;
use log::{info};
use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor::SupervisionEvent::*;
use tokio::net::TcpListener;
use crate::db_actor::actor::DBActor;
use crate::tcp_reader_actor::tcp_reader::{TcpReaderActor, TcpReaderMessage};
use crate::tcp_writer_actor::tcp_writer::TcpWriterActor;

pub struct TcpListenerActor;

#[async_trait]
impl Actor for TcpListenerActor {
    type Msg = TcpListener;
    type State = ();
    type Arguments = String;

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, address: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {

        /* Number of chunk to split key space into */
        let chunks = 2_u64.pow(3);

        info!("Spawning initial DB actors");
        for range in chunk_ranges(chunks) {
            let (_actor, _handler) = Actor::spawn_linked(
                Some(format!("DBActor {:#018x}..{:#018x}", range.start, range.end)),
                DBActor,
                range,
                myself.get_cell()
            ).await.expect("Failed to spawn db actor");
        }
        
        /* Open TCP port to accept connections */
        // TODO: use .into_split to split the TcpStream into listening and writing half
        info!("Listening on {}", address);
        let listener = TcpListener::bind(address)
            .await
            .expect("Failed to open TCP Listener");
        
        cast!(myself, listener)?;
        Ok(())
    }

    // This is only called once
    async fn handle(&self, myself: ActorRef<Self::Msg>, connection: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        // TODO: Move this to post_start?
        // TODO: Because this is in a more or less infinite loop, it does not handle supervisor events.
        loop {
            let (tcp_stream, socket_addr) = connection.accept().await?;
            info!("Accepting connection from: {}", socket_addr);
            
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
            // TODO: Instead of spawning one actor that reads from and write to stream, spawn
            //       one actor that reads and one that writes. This way, the read actor can start reading
            //       the next packet without waiting for a reply.
            //  - The writer has to know the reader but not the other way around
            let (connection_actor, _connection_actor_handle) = Actor::spawn_linked(
                Some(format!("Connection Handler {}", socket_addr)),
                TcpReaderActor,
                (reader, write_actor),
                myself.get_cell()
            ).await?;
            
            cast!(connection_actor, TcpReaderMessage)?;

            // TODO: Send message to yourself?
        }
    }

    /// This handles supervision events for both DBActors and TcpConnectionHandler Actors 
    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, event: SupervisionEvent, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match event {
            ActorStarted(cell) => {
                info!("{} started", cell.get_name().unwrap_or(String::from("Actor")))
            },
            ActorTerminated(cell, _last_state, _reason) => {
                info!("{} stopped", cell.get_name().unwrap_or(String::from("Actor")))
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
