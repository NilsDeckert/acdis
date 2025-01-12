use log::{info};
use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef};
use tokio::net::TcpListener;
use crate::tcp_connection_handler_actor::tcp_connection_handler::TcpConnectionHandler;

pub struct TcpListenerActor;

#[async_trait]
impl Actor for TcpListenerActor {
    type Msg = TcpListener;
    type State = ();
    type Arguments = String;

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, address: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        info!("Listening on {}", address);
        let listener = TcpListener::bind(address)
            .await
            .expect("Failed to open TCP Listener");
        cast!(myself, listener)?;
        Ok(())
    }

    async fn handle(&self, _myself: ActorRef<Self::Msg>, connection: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        info!("Received connection");
        loop {
            let (tcp_stream, socket_addr) = connection.accept().await?;
            // TODO spawn_linked
            let (connection_actor, connection_actor_handle) = Actor::spawn(
                None, // TODO
                TcpConnectionHandler,
                ()
            ).await?;
            cast!(connection_actor, (tcp_stream, socket_addr))?;
            connection_actor_handle.await?;
        }
    }
}