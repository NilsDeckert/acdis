
use crate::hash_slot::{hash_slot::HashSlot, hash_slot_range::HashSlotRange};
use crate::db_actor::HashMap;
use crate::db_actor::map_entry::MapEntry;

use log::info;
use lru::LruCache;
use std::num::NonZeroUsize;
use serde::{Deserialize, Serialize};
use ractor::ActorRef;
use redis_protocol_bridge::util::convert::SerializableFrame;

pub(crate) static CACHE_CAP: NonZeroUsize = NonZeroUsize::new(64).expect("CACHE_CAP must not be zero");

pub struct DBActorState {
    pub map: PartitionedHashMap,
    writer_cache: LruCache<String, ActorRef<SerializableFrame>>
}

impl DBActorState {
    pub fn new(start: HashSlot, end: HashSlot) -> Self {
        DBActorState {
            map: PartitionedHashMap::new(start, end),
            writer_cache: LruCache::new(CACHE_CAP)
        }
    }

    pub fn from_partitioned_hashmap(map: PartitionedHashMap) -> Self {
        DBActorState {
            map,
            writer_cache: LruCache::new(CACHE_CAP)
        }
    }

    /// Fetches ActorRef from the cache or populates it it's missing.
    pub fn get_writer(&mut self, name: String) -> &ActorRef<SerializableFrame> {
        info!("Looking for {}", name);
        self.debug_cache();
        if let Some(_hit) = self.writer_cache.peek(&name) {
            info!("cache hit")
        }
        let ret = self.writer_cache.get_or_insert(
            name.clone(),
             ||{
                let a = Self::find_writer(&name);
                info!("{:?}", &a);
                a
            });
        ret
    }

    /// Given the name of a tcp writer actor, return the reference to that actor
    ///
    /// ## Panics
    /// Panics if the are less than or more than once actors with the given name.
    fn find_writer(name: &String) -> ActorRef<SerializableFrame> {
        info!("cache miss");

        let reply_to_vec = ractor::pg::get_members(name);
        assert_eq!(
            reply_to_vec.len(),
            1,
            "Found less than or more than one actors for {}",
            name
        );
        reply_to_vec.into_iter().next().unwrap().into()
    }

    pub(crate) fn debug_cache(&self) {
        info!("Cap: {:?}", self.writer_cache.cap());
        info!("Len: {}", self.writer_cache.len());
        info!("{:?}", self.writer_cache.peek_lru());
    } 
    
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PartitionedHashMap {
    pub map: HashMap<String, MapEntry>,
    pub range: HashSlotRange,
}

impl PartitionedHashMap {
    pub fn new(start: HashSlot, end: HashSlot) -> Self {
        PartitionedHashMap {
            map: HashMap::default(),
            range: HashSlotRange::new(start, end)
        }
    }

    pub fn in_range(&self, key: &str) -> bool {
        self.range.contains(&HashSlot::from(key))
    }
}

