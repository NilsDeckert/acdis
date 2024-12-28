use ractor::{Actor, async_trait, ActorRef, ActorProcessingErr, RpcReplyPort, Message};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::iter::Map;
use std::ops::Deref;
use log::{info, error};

pub struct DBActor;

pub enum MapEntry {
    STRING(String),
    USIZE(usize),
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

    async fn pre_start<'life1>
    (&'life1 self, _myself: ActorRef<Self::Msg>, _args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        let hashmap = HashMap::new();
        info!("Initializing...");
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
