use crate::db_actor::actor::PartitionedHashMap;
use crate::db_actor::message::DBRequest;
use crate::hash_slot::hash_slot::HashSlot;
use crate::hash_slot::hash_slot_range::HashSlotRange;
use crate::node_manager_actor::NodeManagerRef;
use ractor::RpcReplyPort;
use ractor_cluster::RactorClusterMessage;

#[derive(RactorClusterMessage)]
pub enum NodeManagerMessage {
    Init,
    #[rpc]
    QueryKeyspace(RpcReplyPort<HashSlotRange>),
    #[rpc]
    AdoptKeyspace(HashSlotRange, RpcReplyPort<PartitionedHashMap>),
    SetKeyspace(HashSlotRange),
    #[rpc]
    QueryNodes(RpcReplyPort<Vec<String>>),
    #[rpc]
    Responsible(HashSlot, RpcReplyPort<bool>),
    Forward(DBRequest),
    #[rpc]
    QueryAddress(RpcReplyPort<String>),
    IndexUpdate(HashSlotRange, NodeManagerRef),
}
