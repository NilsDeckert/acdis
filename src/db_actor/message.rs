use redis_protocol_bridge::commands::parse::Request;
use ractor::RpcReplyPort;
use redis_protocol::resp3::types::OwnedFrame;

pub struct DBMessage {
    pub request: Request,
    pub caller: RpcReplyPort<OwnedFrame>
}