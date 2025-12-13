use super::state::*;

use crate::db_actor::command_handler::handle_info;
use crate::db_actor::message::DBMessage;
use crate::hash_slot::hash_slot_range::HashSlotRange;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use redis_protocol::error::{RedisProtocolError, RedisProtocolErrorKind};
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::commands::parse::Request;
use redis_protocol_bridge::commands::{client, cluster, command, config, hello, ping, quit, select};
use redis_protocol_bridge::util::convert::{AsFrame, SerializableFrame};

#[allow(unused_imports)]
use log::{debug, warn};

pub struct DBActor;

pub struct DBActorArgs {
    pub(crate) map: Option<PartitionedHashMap>,
    pub(crate) range: HashSlotRange,
}

#[async_trait]
impl Actor for DBActor {
    type Msg = DBMessage;
    type State = DBActorState;
    type Arguments = DBActorArgs;

    /// Join group of actors
    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let map = args
            .map
            .unwrap_or(PartitionedHashMap::new(args.range.start, args.range.end));
        let group_name = "acdis".to_string();

        ractor::pg::join(group_name.to_owned(), vec![myself.get_cell()]);

        #[cfg(debug_assertions)]
        {
            let members = ractor::pg::get_members(&group_name);
            debug!(
                "We're one of {} actors in this cluster managing {}",
                members.len(),
                args.range
            );
        }

        Ok(DBActorState::from_partitioned_hashmap(map))
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("{:?}", state);
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        own: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            DBMessage::QueryKeyspace(reply) => {
                debug!("Received keyspace query");
                if !reply.is_closed() {
                    reply.send(own.map.range)?;
                }
                Ok(())
            }
            DBMessage::Responsible(hash, reply) => {
                debug!("Received responsibility check");
                if !reply.is_closed() {
                    reply.send(own.map.range.contains(&hash.into()))?
                }
                Ok(())
            }
            DBMessage::Request(req) => {
                debug!("Received request");
                debug!("{:?}", req.request);
                let reply = self.handle_request(&req.request, &mut own.map);

                let reply_to = own.get_writer(req.reply_to);
                debug!("Replying to: {:?}", reply_to);

                match reply {
                    Ok(frame) => {
                        if let Request::QUIT = req.request {
                            reply_to.send_message(SerializableFrame(frame))?;
                            let supervisor = reply_to.try_get_supervisor();
                            if let Some(supervisor) = supervisor {
                                supervisor.stop(Some(String::from("Client sent QUIT")));
                            }
                            Ok(())
                        } else {
                            Ok(reply_to.send_message(SerializableFrame(frame))?)
                        }
                    }

                    Err(err) => match err.kind() {
                        RedisProtocolErrorKind::Parse => {
                            Ok(reply_to.send_message(SerializableFrame(err.as_frame()))?)
                        }
                        _ => Err(ActorProcessingErr::from(err)),
                    },
                }
            }
            DBMessage::Drain(reply) => {
                debug!("Received drain request");
                if !reply.is_closed() {
                    reply.send(own.map.map.clone())?;
                }
                // TODO: Don't accept DB Requests anymore
                myself.stop(Some(String::from("Received Drain request")));
                Ok(())
            }
        }
    }
}

impl DBActor {
    fn handle_request(
        &self,
        request: &Request,
        map: &mut PartitionedHashMap,
    ) -> Result<OwnedFrame, RedisProtocolError> {
        match request {
            /* Handle requests with proper implementations */
            Request::GET { key } => self.get(key, map),
            Request::SET { key, value } => self.set(key, value, map),
            Request::CLUSTER { .. } => cluster::default_handle(request), //TODO
            /* Mock reply using default handlers. TODO */
            Request::HELLO { .. } => hello::default_handle(request),
            Request::COMMAND { .. } => command::default_handle(request),
            Request::INFO(info) => handle_info(info),
            Request::PING { .. } => ping::default_handle(request),
            Request::SELECT { .. } => select::default_handle(request),
            Request::QUIT { .. } => quit::default_handle(request),
            Request::CONFIG(_) => config::default_handle(request),
            Request::CLIENT(_) => client::default_handle(request),
        }
    }

    /// Fetch the value for given `key` from `map`
    fn get(&self, key: &str, map: &PartitionedHashMap) -> Result<OwnedFrame, RedisProtocolError> {
        debug!("GET: {}", key);

        #[cfg(debug_assertions)]
        {
            if !map.in_range(key) {
                warn!("This actor is not responsible for key {}", key);
                return Ok(OwnedFrame::Null);
            }
        }

        let value = map.map.get(key);
        if let Some(value) = value {
            Ok(value.into())
        } else {
            Ok(OwnedFrame::Null)
        }
    }

    /// Set the `value` of `key` in `map`
    fn set(
        &self,
        key: &str,
        value: &str,
        map: &mut PartitionedHashMap,
    ) -> Result<OwnedFrame, RedisProtocolError> {
        debug!("SET: ({}, {})", key, value);

        map.map.insert(key.to_string(), value.into());
        Ok("Ok".as_frame())
    }
}
