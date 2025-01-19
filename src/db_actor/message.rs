use std::ops::Range;
use redis_protocol_bridge::commands::parse::Request;
use ractor::{ActorRef, RpcReplyPort};
use redis_protocol::resp3::types::OwnedFrame;

pub struct DBRequest {
    pub request: Request,
    pub reply_to: ActorRef<OwnedFrame>
}

pub enum DBMessage {
    #[allow(dead_code)]
    QueryKeyspace(RpcReplyPort<Range<u64>>),
    Responsible(u64, RpcReplyPort<bool>),
    Request(DBRequest)
}