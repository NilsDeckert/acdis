use crate::db_actor::map_entry::MapEntry;
use crate::db_actor::HashMap;
use crate::hash_slot::hash_slot_range::HashSlotRange;
use ractor::RpcReplyPort;
use ractor_cluster::RactorClusterMessage;
use redis_protocol_bridge::commands::parse::Request;

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
    QueryKeyspace(RpcReplyPort<HashSlotRange>),
    #[rpc]
    Responsible(u16, RpcReplyPort<bool>),
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
        let (actor, _handle) = Actor::spawn(
            None,
            DBActor,
            DBActorArgs {
                map: None,
                range: HashSlotRange::from(0u16..1u16),
            },
        )
        .await
        .expect("Could not create actor");

        let response = call!(actor, DBMessage::QueryKeyspace);
        if let Ok(response) = response {
            assert!(!response.is_empty());
            assert_eq!(u16::from(response.start), 0u16);
            assert_eq!(u16::from(response.end), 1u16);
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
