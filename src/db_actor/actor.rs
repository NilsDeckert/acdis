use std::collections::HashMap;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol::error::RedisProtocolError;
use log::{debug, info};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use redis_protocol_bridge::commands::parse::Request;
use redis_protocol_bridge::commands::{command, hello, info, ping, select};
use redis_protocol_bridge::util::convert::AsFrame;
use crate::db_actor::map_entry::MapEntry;
use crate::db_actor::message::DBMessage;

pub struct DBActor;

#[async_trait]
impl Actor for DBActor {
    type Msg = DBMessage;
    type State = HashMap<String, MapEntry>;
    type Arguments = ();

    /// Join group of actors
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
        let request = message.request;

        let reply = match request {
            /* Handle requests with proper implementations */
            Request::GET {key} => self.get(key, map),
            Request::SET {key, value} => self.set(key, value, map),
            /* Mock reply using default handlers. TODO */
            Request::HELLO { .. } => hello::default_handle(request),
            Request::COMMAND { .. } => command::default_handle(request),
            Request::INFO { .. } => info::default_handle(request),
            Request::PING { .. } => ping::default_handle(request),
            Request::SELECT { .. } => select::default_handle(request)
        };

        match reply {
            Ok(frame) => {
                if message.caller.is_closed() {
                    Err(ActorProcessingErr::from("Attempted to send reply but channel is closed."))
                } else {
                    message.caller.send(frame)?;
                    Ok(())
                }
            },

            Err(err) => {
                Err(ActorProcessingErr::from(err))
            }
        }
    }

}

impl DBActor {
    /// Fetch the value for given `key` from `map`
    fn get(&self, key: String, map: &HashMap<String, MapEntry>) -> Result<OwnedFrame, RedisProtocolError> {
        debug!("GET: {}", key);
        let value = map.get(&key);
        if let Some(value) = value {
            Ok(value.into())
        } else {
            Ok(OwnedFrame::Null)
        }
    }

    /// Set the `value` of `key` in `map`
    fn set(&self, key: String, value: String, map: &mut HashMap<String, MapEntry>) -> Result<OwnedFrame, RedisProtocolError> {
        debug!("SET: ({}, {})", key, value);
        map.insert(key, value.into());
        Ok("Ok".as_frame())
    }
}