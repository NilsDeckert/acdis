use std::ops::Add;
use redis_protocol::error::{RedisProtocolError, RedisProtocolErrorKind};
use redis_protocol::resp3::types::{FrameMap, OwnedFrame};
use redis_protocol_bridge::commands::info::Info;
use redis_protocol_bridge::util::convert::{AsFrame, SerializableFrame};

pub(crate) fn handle_info(mut info: Info) -> Result<OwnedFrame, RedisProtocolError> {
    let mut ret = String::new();
    
    if info.default {
        info.server=true;
        info.keyspace=true;
        info.persistence=true;
    }
    
    if info.server {
        ret.push_str("\
        # Server\r\n\
        redis_version:7.0.0\r\n\
        redis_git_sha1:00000000\r\n\
        redis_git_dirty:0\r\n\
        os:Linux\r\n\
        ");
    }
    
    if info.keyspace {
        ret.push_str("\
        # Keyspace\r\n\
        db0:keys=0,expires=0,avg_ttl=0\r\n\
        ");
    }
    
    if info.persistence {
        ret.push_str("\
        # Persistence\r\n\
        loading:0\r\n\
        async_loading:0\r\n\
        current_cow_peak:0\r\n\
        current_cow_size:0\r\n\
        current_cow_size_age:0\r\n\
        current_fork_perc:0.00\r\n\
        current_save_keys_processed:0\r\n\
        current_save_keys_total:0\r\n\
        aof_enabled:0\r\n\
        aof_rewrite_in_progress:0\r\n\
        aof_rewrite_scheduled:0\r\n\
        aof_last_rewrite_time_sec:-1\r\n\
        aof_current_rewrite_time_sec:-1\r\n\
        aof_last_bgrewrite_status:ok\r\n\
        aof_rewrites:0\r\n\
        aof_rewrites_consecutive_failures:0\r\n\
        aof_last_write_status:ok\r\n\
        aof_last_cow_size:0\r\n\
        ")
    }
    
    Ok(ret.as_frame())

    // Ok(ret.as_frame())
}