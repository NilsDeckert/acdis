use std::ops::Range;
use redis_protocol_bridge::commands::parse::Request;
use ractor::{ActorRef, BytesConvertable, RpcReplyPort};
use ractor_cluster::RactorClusterMessage;
use redis_protocol_bridge::util::convert::SerializableFrame;

//#[derive(RactorClusterMessage)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DBRequest {
    pub request: Request,
    pub reply_to: String
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

#[cfg(test)]
pub mod tests {
    use super::*;
    
    #[test]
    fn serialize_hello() {
        let hello = Request::HELLO {
            version: Some(String::from("3")),
            clientname: Some(String::from("Client")),
            auth: None,
        };
        
        let db_request = DBRequest{
            request: hello,
            reply_to: String::from("Client"),
        };
        
        let _serialized = db_request.into_bytes();
    }
    
    #[test]
    fn deserialize_hello() {
        let hello = Request::HELLO {
            version: Some(String::from("3")),
            clientname: Some(String::from("Client")),
            auth: None,
        };

        let db_request = DBRequest{
            request: hello,
            reply_to: String::from("Client"),
        };

        let serialized = db_request.into_bytes();
        let deserialized = DBRequest::from_bytes(serialized);
        
        assert_eq!(deserialized.reply_to, String::from("Client"));
    }
    
    #[test]
    fn serialize_dbmessage_hello() {
        let hello = Request::HELLO {
            version: Some(String::from("3")),
            clientname: Some(String::from("Client")),
            auth: None,
        };

        let db_request = DBRequest{
            request: hello,
            reply_to: String::from("Client"),
        };
        
        let db_message = DBMessage::Request(db_request);
        let _serialized = db_message.into_bytes();
    }
}