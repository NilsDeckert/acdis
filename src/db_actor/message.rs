use std::ops::Range;
use redis_protocol_bridge::commands::parse::Request;
use ractor::{ActorRef, BytesConvertable, RpcReplyPort};
use ractor_cluster::RactorClusterMessage;
use redis_protocol_bridge::util::convert::SerializableFrame;

#[derive(RactorClusterMessage)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DBRequest {
    pub request: Request,
    // pub reply_to: ActorRef<SerializableFrame> // TODO: This causes problems when serializing
}

#[derive(RactorClusterMessage)]
pub enum DBMessage {
    #[allow(dead_code)]
    #[rpc]
    QueryKeyspace(RpcReplyPort<Vec<u64>>),
    #[rpc]
    Responsible(u64, RpcReplyPort<bool>),
    Request(DBRequest)
}