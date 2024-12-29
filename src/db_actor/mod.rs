use convert::AsFrame;
use ractor::{Actor, async_trait, ActorRef, ActorProcessingErr, RpcReplyPort, Message};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use log::{info, error};
use redis_protocol_bridge::util::{*};

pub struct DBActor;

pub enum MapEntry {
    STRING(String),
    USIZE(usize),
}

impl From<MapEntry> for redis_protocol::resp3::types::OwnedFrame {
    fn from(item: MapEntry) -> Self {
        match item {
            MapEntry::STRING(s) => {s.as_frame()}
            MapEntry::USIZE(u) => {u.as_frame()}
        }
    }
}

impl Clone for MapEntry {
    fn clone(&self) -> Self {
        match self {
            MapEntry::STRING(a) => MapEntry::STRING(a.clone()),
            MapEntry::USIZE(a) => MapEntry::USIZE(a.clone())
        }
    }
}

impl Display for MapEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MapEntry::STRING(a) => write!(f, "{}", a),
            MapEntry::USIZE(a) => write!(f, "{}", a)
        }
    }
}

pub enum DBMessage {
    INSERT( String, MapEntry),
    GET(    String, RpcReplyPort<MapEntry>),
    REMOVE( String, RpcReplyPort<MapEntry>),
}

#[async_trait]
impl Actor for DBActor {
    type Msg = DBMessage;
    type State = HashMap<String, MapEntry>;
    type Arguments = ();

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, _args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        info!("Initializing...");
        let hashmap = HashMap::new();
        let group_name = "acdis".to_string();

        ractor::pg::join(
            group_name.to_owned(),
            vec![myself.get_cell()]
        );

        let members = ractor::pg::get_members(&group_name);
        info!("We're one of {} actors in this cluster", members.len());

        Ok(hashmap)
    }

    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, map: &mut Self::State) -> Result<(), ActorProcessingErr> {
        info!("Received message");
        match message {
            DBMessage::INSERT(key, value) => {
                info!("Inserting: {}", key);
                map.insert(key, value);
                Ok(())
            }

            DBMessage::REMOVE(_, _)|
            DBMessage::GET(_, _) => {
                let value: MapEntry;
                let reply;

                if let DBMessage::REMOVE(key, rpl) = message {
                    info!("Removing: {}", key);
                    reply = rpl;
                    value = map.remove(&key).ok_or(
                        ActorProcessingErr::from("Key not in Map")
                    )?;
                } else if let DBMessage::GET(key, rpl) = message {
                    info!("Fetching: {}", key);
                    reply = rpl;
                    
                    let refvalue = map.get(&key).ok_or(
                        ActorProcessingErr::from("Could not find value for key")
                    )?;
                    
                    value = refvalue.clone();

                } else {
                    error!("Unexpected DBMessage type");
                    return Err(ActorProcessingErr::from(
                        "Unexpected DBMessage type in DBActor::handle"
                    ));
                }

                if !reply.is_closed() {
                    reply.send(value).expect("Failed to send reply");
                    Ok(())
                } else {
                    error!{"Reply channel closed"}
                    Err(ActorProcessingErr::from("Reply channel closed"))
                }
            }
        }
    }
}
