use std::hash::Hasher;
use std::ops::Range;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol::error::RedisProtocolError;
use log::{debug, info, warn};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use redis_protocol_bridge::commands::parse::Request;
use redis_protocol_bridge::commands::{command, hello, info, ping, select};
use redis_protocol_bridge::util::convert::{AsFrame, SerializableFrame};
use serde::{Deserialize, Serialize};
use crate::db_actor::{AHasher, HashMap};
use crate::db_actor::map_entry::MapEntry;
use crate::db_actor::message::DBMessage;

pub struct DBActor;

#[derive(Serialize, Deserialize, Clone)]
pub struct PartitionedHashMap {
    pub map: HashMap<String, MapEntry>,
    pub range: Range<u64>,
}

impl PartitionedHashMap {
    pub fn in_range(&self, key: &String) -> bool {
        //let mut hasher = DefaultHasher::new();
        let mut hasher = AHasher::default();
        hasher.write(key.as_bytes());
        let hash = hasher.finish();
        
        if self.range.contains(&hash) {
            true
        } else {
            debug!("Hash({:#x}) not in range {:#018x}..{:#018x}", hash, self.range.start, self.range.end);
            false
        }
        
    }
}

pub struct DBActorArgs {
    pub(crate) map: Option<PartitionedHashMap>,
    pub(crate) range: Range<u64>
}

#[async_trait]
impl Actor for DBActor {
    type Msg = DBMessage;
    type State = PartitionedHashMap;
    type Arguments = DBActorArgs;

    /// Join group of actors
    async fn pre_start(&self, myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        let map = args.map.unwrap_or(
            PartitionedHashMap { map: HashMap::default(), range: args.range.clone()});
        let group_name = "acdis".to_string();

        ractor::pg::join(
            group_name.to_owned(),
            vec![myself.get_cell()]
        );

        let members = ractor::pg::get_members(&group_name);
        info!("We're one of {} actors in this cluster managing {:#018x}..{:#018x}",
            members.len(), args.range.start, args.range.end);

        Ok(map)
    }

    async fn handle(&self, myself: ActorRef<Self::Msg>, message: Self::Msg, map: &mut Self::State) -> Result<(), ActorProcessingErr> {
        
        match message {
            DBMessage::QueryKeyspace(reply) => {
                debug!("Received keyspace query");
                if !reply.is_closed() {
                    reply.send(map.range.clone())?;
                }
                Ok(())
            }
            DBMessage::Responsible(hash, reply) => {
                debug!("Received responsibility check");
                if !reply.is_closed() {
                    reply.send(map.range.contains(&hash))?
                }
                Ok(())
            }
            DBMessage::Request(req) => {
                debug!("Received request");
                debug!("{:?}", req.request);
                let reply = self.handle_request(req.request, map);
                
                match reply {
                    Ok(frame) => {
                        let reply_to_vec = ractor::pg::get_members(&req.reply_to);
                        assert_eq!(reply_to_vec.len(), 1,
                                   "Found less than or more than one actors for {}", req.reply_to);
                        let reply_to = reply_to_vec.into_iter().next().unwrap();
                        debug!("Replying to: {:?}", reply_to);
                        
                        Ok(reply_to.send_message(SerializableFrame(frame))?)
                    },

                    Err(err) => {
                        Err(ActorProcessingErr::from(err))
                    }
                }
            }
            DBMessage::Drain(reply) => {
                debug!("Received drain request");
                if !reply.is_closed() {
                    reply.send(map.map.clone())?;
                }
                // TODO: Don't accept DB Requests anymore
                myself.stop(Some(String::from("Received Drain request")));
                Ok(())
            }
        }
    }
}

impl DBActor {
    
    fn handle_request(&self, request: Request, map: &mut PartitionedHashMap) -> Result<OwnedFrame, RedisProtocolError> {
        match request {
            /* Handle requests with proper implementations */
            Request::GET {key} => self.get(key, map),
            Request::SET {key, value} => self.set(key, value, map),
            /* Mock reply using default handlers. TODO */
            Request::HELLO { .. } => hello::default_handle(request),
            Request::COMMAND { .. } => command::default_handle(request),
            Request::INFO { .. } => info::default_handle(request),
            Request::PING { .. } => ping::default_handle(request),
            Request::SELECT { .. } => select::default_handle(request)
        }
    }
    
    /// Fetch the value for given `key` from `map`
    fn get(&self, key: String, map: &PartitionedHashMap) -> Result<OwnedFrame, RedisProtocolError> {
        debug!("GET: {}", key);
        
        if !map.in_range(&key) {
            warn!("This actor is not responsible for key {}", key);
            return Ok(OwnedFrame::Null);
        }
        
        let value = map.map.get(&key);
        if let Some(value) = value {
            Ok(value.into())
        } else {
            Ok(OwnedFrame::Null)
        }
    }

    /// Set the `value` of `key` in `map`
    fn set(&self, key: String, value: String, map: &mut PartitionedHashMap) -> Result<OwnedFrame, RedisProtocolError> {
        debug!("SET: ({}, {})", key, value);

        if !map.in_range(&key) {
            warn!("This actor is not responsible for key {}", key);
            Ok("Not responsible".as_frame())
        } else {
            map.map.insert(key, value.into());
            Ok("Ok".as_frame())
        }
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     
//     #[test]
//     fn 
// }