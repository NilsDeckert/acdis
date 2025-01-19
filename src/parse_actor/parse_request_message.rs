use ractor::ActorRef;
use redis_protocol::resp3::types::OwnedFrame;


/// The API Endpoint deserializes the incoming messages
/// and creates an OwnedFrame. Using this message type,
/// the api endpoint sends off the request for parsing and
/// later handling.
/// The result of the request encoded in `frame` is sent
/// directly to `caller`.
pub struct ParseRequestMessage {
    pub frame: OwnedFrame,
    pub reply_to: ActorRef<OwnedFrame>
}