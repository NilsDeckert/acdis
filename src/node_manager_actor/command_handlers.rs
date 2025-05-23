use crate::db_actor::message::DBRequest;
use crate::node_manager_actor::actor::NodeManagerActor;
use crate::node_manager_actor::state::NodeManagerActorState;
use ractor::ActorProcessingErr;
use redis_protocol::resp3::types::OwnedFrame;
use redis_protocol_bridge::commands::cluster::{cluster_slots, Cluster};
use redis_protocol_bridge::commands::config;
use redis_protocol_bridge::commands::config::Config;
use redis_protocol_bridge::commands::parse::Request::*;
use redis_protocol_bridge::util::convert::{map_to_array, AsFrame};
use std::collections::HashMap;

/// Returns true if the request should be handled by the node.
/// Returns false if it should be handled by a [`db_actor::actor::DBActor`]
pub(crate) fn node_handles(request: &DBRequest) -> bool {
    matches!(request.request, CLUSTER(_) | CONFIG(_))
}

pub(crate) fn node_handle(
    request: &DBRequest,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    assert!(node_handles(request));

    match &request.request {
        CLUSTER(sub) => node_handle_cluster(sub, &request.reply_to, state),
        CONFIG(sub) => node_handle_config(sub, &request.reply_to, state),
        _ => Err(ActorProcessingErr::from(format!(
            "No command handler for {:?}",
            request.request
        ))),
    }
}

fn node_handle_cluster(
    subcommand: &Cluster,
    reply_to: &str,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    match subcommand {
        Cluster::SHARDS => node_handle_cluster_shards(reply_to, state),
        Cluster::NODES => node_handle_cluster_nodes(reply_to, state),
        Cluster::SLOTS => node_handle_cluster_slots(reply_to, state),
        _ => Err(ActorProcessingErr::from(format!(
            "No command handler for CLUSTER {:?}",
            subcommand
        ))),
    }
}

fn node_handle_cluster_shards(
    reply_to: &str,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    let mut reply: Vec<OwnedFrame> = vec![];

    let ip: String = state.redis_host.0.clone();
    let port: String = state.redis_host.1.to_string();
    let host: String = format!("{}:{}", ip, port);

    let myself: Vec<OwnedFrame> = vec![
        cluster_slots(vec![(state.keyspace.start.0, state.keyspace.end.0)]).as_frame(),
        "nodes".as_frame(),
        map_to_array(HashMap::from([
            ("id", host.as_ref()),
            ("endpoint", ip.as_ref()),
            ("ip", ip.as_ref()),
            ("port", port.as_ref()),
        ])),
    ];
    reply.push(myself.as_frame());

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

    NodeManagerActor::reply_to(reply_to, reply.as_frame().into())
}

/// Reply to `CLUSTER NODES` command.
/// This sends an overview of the nodes in this cluster to the [`crate::tcp_writer_actor::tcp_writer`]
/// `reply_to`.
///
/// ## See
/// <a href="https://redis.io/docs/latest/commands/cluster-nodes/">CLUSTER NODES</a>
fn node_handle_cluster_nodes(
    reply_to: &str,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    let mut reply: String = String::new();

    // Info for this node
    let own_host = format!("{}:{}", state.redis_host.0, state.redis_host.1);
    let own_port = state.redis_host.1.to_string();
    let own_slots = format!("{}-{}", state.keyspace.start.0, state.keyspace.end.0);
    reply.push_str(
        format!("{own_host} {own_host}@{own_port} myself,master - 0 0 1 connected {own_slots}\r\n")
            .as_ref(),
    );

    // Info for other nodes in cluster
    let mut i = 1;
    for (hsr, nmr) in &state.other_nodes {
        i += 1;
        let host: String = format!("{nmr}");
        let port: String = nmr.host_port.to_string();
        let slot_range = format!("{}-{}", hsr.start.0, hsr.end.0);
        reply.push_str(
            format!("{host} {host}@{port} master - 0 0 {i} connected {slot_range}\r\n").as_ref(),
        );
    }

    NodeManagerActor::reply_to(reply_to, reply.as_frame().into())
}

/// ## See
/// <a href="https://redis.io/docs/latest/commands/cluster-slots/">CLUSTER SLOTS</a>
///
/// ```
/// 1)  1) Start of slot range
///     2) End of slot range
///     3)  1) Ip of the node managing the slot range
///         2) Port of the node
///         3) ID of the node
///         4) Additional Network Info. Empty here.
/// ```
fn node_handle_cluster_slots(
    reply_to: &str,
    state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    let mut reply: Vec<OwnedFrame> = vec![];

    // We don't send any additional network info. Redis-server sends an empty array if nothing is
    // transmitted.
    let empty_array = OwnedFrame::Array {
        data: vec![],
        attributes: None,
    };

    let myself = vec![
        state.keyspace.start.0.as_frame(),
        state.keyspace.end.0.as_frame(),
        vec![
            state.redis_host.0.as_frame(),
            state.redis_host.1.as_frame(),
            format!("{}:{}", state.redis_host.0, state.redis_host.1).as_frame(),
            empty_array.clone(),
        ]
        .as_frame(),
    ];
    reply.push(myself.as_frame());

    for (hsr, nmr) in &state.other_nodes {
        let ip: String = nmr.host_ip.to_string();
        let port: String = nmr.host_port.to_string();
        let host: String = format!("{nmr}");
        let this_node = vec![
            hsr.start.0.as_frame(),
            hsr.end.0.as_frame(),
            vec![ip.as_frame(), port.as_frame(), host.as_frame()].as_frame(),
            empty_array.clone(),
        ];
        reply.push(this_node.as_frame());
    }

    NodeManagerActor::reply_to(reply_to, reply.as_frame().into())
}

pub fn node_handle_config(
    sub: &Config,
    reply_to: &str,
    _state: &NodeManagerActorState,
) -> Result<(), ActorProcessingErr> {
    let reply = match sub {
        Config::Get(get) => config::default_handle_config_get(get)?,
    };

    NodeManagerActor::reply_to(reply_to, reply.into())
}
