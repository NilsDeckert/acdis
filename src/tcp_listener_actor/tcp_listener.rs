use crate::tcp_reader_actor::tcp_reader::TcpReaderActor;
use crate::tcp_writer_actor::tcp_writer::TcpWriterActor;
use log::{debug, error, info};
use ractor::SupervisionEvent::*;
use ractor::{
    async_trait, cast, Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent,
};
use ractor_cluster::RactorMessage;
use std::io::ErrorKind::AddrInUse;
use std::process::exit;
use tokio::net::TcpListener;

pub struct TcpListenerActor;

struct CompanionActor;

#[derive(RactorMessage)]
pub enum TcpConnectionMessage {
    /// Notify the ListenerActor about a new incoming connection
    NewConnection {
        tcp_stream: tokio::net::TcpStream,
        socket_addr: core::net::SocketAddr,
    },
    /// Ask which address the TCP port uses
    QueryAddress(RpcReplyPort<String>),
}

/// Extra actor to avoid blocking the [`TcpListenerActor`]s message handling
/// with an infinite loop.
/// This companion actor will wait for a new incoming connection and then notify the [`TcpListenerActor`]
/// with A [`TcpConnectionMessage::NewConnection`]
#[async_trait]
impl Actor for CompanionActor {
    type Msg = ();
    type State = (TcpListener, ActorRef<TcpConnectionMessage>);
    type Arguments = (TcpListener, ActorRef<TcpConnectionMessage>);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let (listener, send_to) = state;

        loop {
            let (tcp_stream, socket_addr) = listener.accept().await?;
            info!("Accepting connection from: {}", socket_addr);
            cast!(
                send_to,
                TcpConnectionMessage::NewConnection {
                    tcp_stream,
                    socket_addr
                }
            )?;
        }
    }
}

#[async_trait]
impl Actor for TcpListenerActor {
    type Msg = TcpConnectionMessage;
    type State = String;
    type Arguments = String;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        address: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        /* Open TCP port to accept connections */
        let res = TcpListener::bind(address).await;

        match res {
            Ok(l) => {
                let listener = l;
                let address = listener.local_addr()?;

                // Spawn an actor that polls the TcpListener for available connections
                Actor::spawn_linked(
                    Some(String::from("TCP Poller")),
                    CompanionActor,
                    (listener, myself.clone()),
                    myself.get_cell(),
                )
                .await
                .expect("Failed to spawn TCP poller");

                info!("Listening on {}", &address);
                Ok(address.to_string())
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

    async fn post_stop(&self, _myself: ActorRef<Self::Msg>, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        info!("Stopping");
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        address: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Received Message");
        match msg {
            TcpConnectionMessage::NewConnection {
                tcp_stream,
                socket_addr,
            } => {
                // Split stream into reader and writer to allow for concurrent reading and writing
                let (reader, writer) = tcp_stream.into_split();

                let (write_actor, _write_actor_handle) = Actor::spawn_linked(
                    Some(format!("Stream Writer {}", socket_addr)),
                    TcpWriterActor,
                    writer,
                    myself.get_cell(),
                )
                .await?;

                // Clients usually keep their connections open. This means the spawned connection handler
                // is alive and working until the client disconnects. -> It is okay to spawn a fresh actor
                // for each client connection.
                let (read_actor, _read_actor_handle) = Actor::spawn_linked(
                    Some(format!("Stream Reader {}", socket_addr)),
                    TcpReaderActor,
                    (reader, write_actor.clone()),
                    myself.get_cell(),
                )
                .await?;

                // Let the read_actor supervise the write actor. This simplifies stopping the write_actor
                // when the stream is closed.
                write_actor.link(read_actor.get_cell());
            }
            TcpConnectionMessage::QueryAddress(reply) => {
                reply.send(address.clone())?;
            }
        }

        Ok(())
    }

    /// This handles supervision events for both DBActors and TcpConnectionHandler Actors
    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        event: SupervisionEvent,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match event {
            ActorStarted(cell) => {
                info!(
                    "{} started",
                    cell.get_name().unwrap_or(String::from("Actor"))
                )
            }
            ActorTerminated(cell, _last_state, _reason) => {
                info!(
                    "{} stopped",
                    cell.get_name().unwrap_or(String::from("Actor"))
                );
            }
            ActorFailed(_cell, _error) => {}
            ProcessGroupChanged(_message) => {}
            _ => { /* Just to silence IDE*/ }
        }
        Ok(())
    }
}
