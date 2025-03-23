use crate::db_actor::message::DBRequest;
use crate::node_manager_actor::actor::NodeManagerActor;
use crate::node_manager_actor::state::NodeManagerActorState;
use ractor::ActorProcessingErr;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::commands::cluster::{cluster_slots, Cluster};
use redis_protocol_bridge::commands::parse::Request::*;
use redis_protocol_bridge::util::convert::{map_to_array, AsFrame};
use std::collections::HashMap;

/// Returns true if the request should be handled by the node.
/// Returns false if it should be handled by a [`db_actor::actor::DBActor`]
pub(crate) fn node_handles(request: &DBRequest) -> bool {
    match request.request {
        CLUSTER(_) => true,
        _ => false,
    }
}

pub(crate) fn node_handle(
    request: DBRequest,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    assert!(node_handles(&request));

    match request.request {
        CLUSTER(sub) => node_handle_cluster(sub, request.reply_to, state),
        _ => Err(ActorProcessingErr::from(format!(
            "No command handler for {:?}",
            request.request
        ))),
    }
}

fn node_handle_cluster(
    subcommand: Cluster,
    reply_to: String,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    match subcommand {
        Cluster::SHARDS => node_handle_cluster_shards(reply_to, state),
        _ => Err(ActorProcessingErr::from(format!(
            "No command handler for CLUSTER {:?}",
            subcommand
        ))),
    }
}

fn node_handle_cluster_shards(
    reply_to: String,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    let mut reply: Vec<OwnedFrame> = vec![];

    for (hsr, nmr) in &state.other_nodes {
        let mut this_shard: Vec<OwnedFrame> = vec![];
        this_shard.push(cluster_slots(vec![hsr.into()]).as_frame());
        this_shard.push("nodes".as_frame());
        
        let host: String = format!("{nmr}");
        let ip: String = nmr.host_ip.clone();
        let port: String = nmr.host_port.to_string();

        let value_map: HashMap<&str, &str> = HashMap::from([
            ("id", host.as_ref()),
            ("endpoint", ip.as_ref()),
            ("ip", ip.as_ref()),
            ("port", port.as_ref()),
        ]);

        this_shard.push(map_to_array(value_map));
        reply.push(this_shard.as_frame());
    }

    NodeManagerActor::reply_to(reply_to.as_ref(), reply.as_frame().into())
}
