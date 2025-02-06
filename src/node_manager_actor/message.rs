use std::ops::Range;
use ractor::RpcReplyPort;
use ractor_cluster::RactorClusterMessage;
use crate::db_actor::actor::PartitionedHashMap;

#[derive(RactorClusterMessage)]
pub enum NodeManagerMessage {
    Init,
    #[rpc]
    QueryKeyspace(RpcReplyPort<Range<u64>>),
    #[rpc]
    AdoptKeyspace(Range<u64>, RpcReplyPort<PartitionedHashMap>),
    SetKeyspace(Range<u64>),
}