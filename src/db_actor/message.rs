use crate::db_actor::map_entry::MapEntry;
use ractor::RpcReplyPort;
use ractor_cluster::RactorClusterMessage;
use redis_protocol_bridge::commands::parse::Request;
use std::collections::HashMap;
use std::ops::Range;

// #[derive(serde::Serialize, serde::Deserialize, RactorClusterMessage)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DBRequest {
    pub request: Request,
    pub reply_to: String,
}

#[derive(RactorClusterMessage)]
pub enum DBMessage {
    #[allow(dead_code)]
    #[rpc]
    QueryKeyspace(RpcReplyPort<Range<u64>>),
    #[rpc]
    Responsible(u64, RpcReplyPort<bool>),
    #[rpc]
    Drain(RpcReplyPort<HashMap<String, MapEntry>>),
    Request(DBRequest),
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::db_actor::actor::{DBActor, DBActorArgs};
    use ractor::{call, Actor, BytesConvertable, Message};

    #[derive(RactorClusterMessage)]
    pub enum TestMessageType {
        #[rpc]
        RpcU64(RpcReplyPort<u64>),
    }

    #[test]
    fn serialize_hello() {
        let hello = Request::HELLO {
            version: Some(String::from("3")),
            clientname: Some(String::from("Client")),
            auth: None,
        };

        let db_request = DBRequest {
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

        let db_request = DBRequest {
            request: hello,
            reply_to: String::from("Client"),
        };

        let serialized = db_request.into_bytes();
        let deserialized = DBRequest::from_bytes(serialized);

        assert_eq!(deserialized.reply_to, String::from("Client"));
    }

    #[tokio::test]
    async fn serialize_query_keyspace() {
        let (actor, handle) = Actor::spawn(
            None,
            DBActor,
            DBActorArgs {
                map: None,
                range: 0u64..1u64,
            },
        )
        .await
        .expect("Could not create actor");

        let response = call!(actor, DBMessage::QueryKeyspace);
        if let Ok(response) = response {
            assert!(!response.is_empty());
            assert_eq!(response.start, 0u64);
            assert_eq!(response.end, 1u64);
        }
    }

    #[test]
    fn serialize_dbmessage_hello() {
        let hello = Request::HELLO {
            version: Some(String::from("3")),
            clientname: Some(String::from("Client")),
            auth: None,
        };

        let db_request = DBRequest {
            request: hello,
            reply_to: String::from("Client"),
        };

        let db_message = DBMessage::Request(db_request);
        let _serialized = db_message.serialize();
    }
}
