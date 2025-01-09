use redis_protocol_bridge::commands::parse::Request;
use ractor::RpcReplyPort;
use redis_protocol::resp3::types::OwnedFrame;

pub struct DBRequest {
    pub request: Request,
    pub caller: RpcReplyPort<OwnedFrame>
}

pub enum DBMessage {
    #[allow(dead_code)]
    QueryKeyspace(RpcReplyPort<(u64, u64)>),
    Responsible(u64, RpcReplyPort<bool>),
    Request(DBRequest)
}