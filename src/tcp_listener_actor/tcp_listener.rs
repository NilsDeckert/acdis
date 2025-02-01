use std::io::ErrorKind::AddrInUse;
use std::process::exit;
use log::{debug, error, info};
use ractor::{cast, Actor, ActorProcessingErr, ActorRef, SupervisionEvent, async_trait};
use ractor::SupervisionEvent::*;
use ractor_cluster::RactorMessage;
use tokio::net::TcpListener;
use crate::tcp_reader_actor::tcp_reader::TcpReaderActor;
use crate::tcp_writer_actor::tcp_writer::TcpWriterActor;

pub struct TcpListenerActor;

struct CompanionActor;

#[derive(RactorMessage)]
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
        
        /* Open TCP port to accept connections */
        info!("Listening on {}", address);
        let res = TcpListener::bind(address)
            .await;
        
        match res {
            Ok(l) => {
                let listener = l;
                
                // Spawn an actor that polls the TcpListener for available connections
                Actor::spawn_linked(
                    Some(String::from("TCP Poller")),
                    CompanionActor,
                    (listener, myself.clone()),
                    myself.get_cell()
                ).await.expect("Failed to spawn TCP poller");
                
                Ok(())
            }
            Err(e) => {
                if e.kind() == AddrInUse {
                    error!("Address already in use! Make sure no other instance is running");
                    exit(-1);
                } else {
                    Err(e.into())
                }
            }
        }
    }

    async fn handle(&self, myself: ActorRef<Self::Msg>, connection: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        debug!("Received Message");
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
            ProcessGroupChanged(_message) => {},
            _ => {/* Just to silence IDE*/}
        }
        Ok(())
    }
}

