use ractor_cluster::RactorClusterMessage;
use redis_protocol_bridge::util::convert::SerializableFrame;

/// The API Endpoint deserializes the incoming messages
/// and creates an OwnedFrame. Using this message type,
/// the api endpoint sends off the request for parsing and
/// later handling.
/// The result of the request encoded in `frame` is sent
/// directly to `reply_to`.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ParseRequestFrame {
    pub frame: SerializableFrame,
    /// Name of the [`ractor::pg`] process group, whose actor should receive the response to this request.
    ///
    /// In our case, this the [`tokio::net::tcp::OwnedWriteHalf::peer_addr`] of the connected
    /// tcp stream, which identifies the [`crate::tcp_writer_actor::tcp_writer::TcpWriterActor`]
    /// responsible for this connection.
    pub reply_to: String,
}

#[derive(RactorClusterMessage)]
pub enum ParseRequestMessage {
    Frame(ParseRequestFrame),

    /// To avoid querying for all NodeManagers for every request, the
    /// [`crate::parse_actor::parse_request_actor::ParseRequestActor`] 'caches' the list of NodeManagers.
    /// Sending this message prompts the ParseRequestActor to update its index.
    UpdateIndex,
}
