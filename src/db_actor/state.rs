use crate::db_actor::map_entry::MapEntry;
use crate::db_actor::HashMap;
use crate::hash_slot::{hash_slot::HashSlot, hash_slot_range::HashSlotRange};

use log::{debug, info};
use lru::LruCache;
use ractor::ActorRef;
use redis_protocol_bridge::util::convert::SerializableFrame;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

pub(crate) static CACHE_CAP: NonZeroUsize =
    NonZeroUsize::new(64).expect("CACHE_CAP must not be zero");

#[derive(Debug)]
pub struct DBActorState {
    pub map: PartitionedHashMap,
    writer_cache: LruCache<String, ActorRef<SerializableFrame>>,
    #[cfg(debug_assertions)]
    writer_req: u32,
    #[cfg(debug_assertions)]
    writer_cache_misses: u32,
}

impl DBActorState {
    #[allow(dead_code)]
    pub fn new(start: HashSlot, end: HashSlot) -> Self {
        DBActorState {
            map: PartitionedHashMap::new(start, end),
            writer_cache: LruCache::new(CACHE_CAP),
            #[cfg(debug_assertions)]
            writer_req: 0,
            #[cfg(debug_assertions)]
            writer_cache_misses: 0,
        }
    }

    pub fn from_partitioned_hashmap(map: PartitionedHashMap) -> Self {
        DBActorState {
            map,
            writer_cache: LruCache::new(CACHE_CAP),
            #[cfg(debug_assertions)]
            writer_req: 0,
            #[cfg(debug_assertions)]
            writer_cache_misses: 0,
        }
    }

    /// Fetches ActorRef from the cache or populates it it's missing.
    pub fn get_writer(&mut self, name: String) -> &ActorRef<SerializableFrame> {
        info!("Looking for {}", name);
        // #[cfg(debug_assertions)]
        // {
        //     self.writer_req += 1;
        //     debug!("Total requests to cache: {}", &self.writer_req);
        //     debug!("Cache misses:            {}", &self.writer_cache_misses);
        //     debug!(
        //         "Cache hits:              {}",
        //         (self.writer_req - self.writer_cache_misses)
        //     );
        // }
        let ret = self.writer_cache.get_or_insert(name.clone(), || {
            #[cfg(debug_assertions)]
            {
                self.writer_cache_misses += 1;
            }
            Self::find_writer(&name)
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

    #[allow(dead_code)]
    pub(crate) fn debug_cache(&self) {
        debug!("Cap: {:?}", self.writer_cache.cap());
        debug!("Len: {}", self.writer_cache.len());
        debug!("{:?}", self.writer_cache.peek_lru());
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PartitionedHashMap {
    pub map: HashMap<String, MapEntry>,
    pub range: HashSlotRange,
}

impl PartitionedHashMap {
    pub fn new(start: HashSlot, end: HashSlot) -> Self {
        PartitionedHashMap {
            map: HashMap::default(),
            range: HashSlotRange::new(start, end),
        }
    }

    pub fn in_range(&self, key: &str) -> bool {
        self.range.contains(&HashSlot::from(key))
    }
}
