use ractor::{ActorRef, Message};
use ractor_cluster::RactorClusterMessage;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::util::convert::SerializableFrame;

/// The API Endpoint deserializes the incoming messages
/// and creates an OwnedFrame. Using this message type,
/// the api endpoint sends off the request for parsing and
/// later handling.
/// The result of the request encoded in `frame` is sent
/// directly to `caller`.
#[derive(RactorClusterMessage)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ParseRequestMessage {
    pub frame: SerializableFrame,
    // pub reply_to: ActorRef<SerializableFrame> // TODO: This causes problems in serialization
}