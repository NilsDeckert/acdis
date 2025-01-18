use std::ops::Range;
use log::{info};
use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor::SupervisionEvent::*;
use tokio::net::TcpListener;
use crate::db_actor::actor::DBActor;
use crate::tcp_connection_handler_actor::tcp_connection_handler::TcpConnectionHandler;

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
                Some(format!("DBActor {:#?}", range)),
                DBActor,
                range,
                myself.get_cell()
            ).await.expect("Failed to spawn db actor");
        }
        
        /* Open TCP port to accept connections */
        info!("Listening on {}", address);
        let listener = TcpListener::bind(address)
            .await
            .expect("Failed to open TCP Listener");
        cast!(myself, listener)?;
        Ok(())
    }

    async fn handle(&self, myself: ActorRef<Self::Msg>, connection: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        // TODO: Move this to post_start?
        loop {
            let (tcp_stream, socket_addr) = connection.accept().await?;
            info!("Accepting connection from: {}", socket_addr);
            let (connection_actor, _connection_actor_handle) = Actor::spawn_linked(
                Some(format!("Connection Handler {}", socket_addr)),
                TcpConnectionHandler,
                (),
                myself.get_cell()
            ).await?;
            
            // TODO: Send message to yourself?
            cast!(connection_actor, (tcp_stream, socket_addr))?;
        }
    }

    /// This handles supervision events for both DBActors and TcpConnectionHandler Actors 
    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, event: SupervisionEvent, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match event {
            ActorStarted(cell) => {
                info!("{} started", cell.get_name().unwrap_or(String::from("Actor")))
            },
            ActorTerminated(_cell, _last_state, _reason) => {},
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
