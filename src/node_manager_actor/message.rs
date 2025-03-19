use ractor::RpcReplyPort;
use ractor_cluster::RactorClusterMessage;
use std::ops::Range;
use crate::node_manager_actor::NodeManagerRef;
use crate::db_actor::actor::PartitionedHashMap;
use crate::db_actor::message::DBRequest;

#[derive(RactorClusterMessage)]
pub enum NodeManagerMessage {
    Init,
    #[rpc]
    QueryKeyspace(RpcReplyPort<Range<u64>>),
    #[rpc]
    AdoptKeyspace(Range<u64>, RpcReplyPort<PartitionedHashMap>),
    SetKeyspace(Range<u64>),
    #[rpc]
    QueryNodes(RpcReplyPort<Vec<String>>),
    #[rpc]
    Responsible(u64, RpcReplyPort<bool>),
    Forward(DBRequest),
    #[rpc]
    QueryAddress(RpcReplyPort<String>),
    IndexUpdate(Range<u64>, NodeManagerRef)
}
