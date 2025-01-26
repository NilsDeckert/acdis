use std::ops::Range;
use futures::future::join_all;
use log::info;
use ractor::Actor;
use Acdis::db_actor::actor::DBActor;
use Acdis::tcp_listener_actor::tcp_listener::chunk_ranges;
use ractor_cluster;
use ractor_cluster::node::NodeConnectionMode;

#[tokio::main]
async fn main() {
    let pmd = ractor_cluster::NodeServer::new(
        0,
        std::env::var("CLUSTER_COOKIE").unwrap_or("cookie".to_string()),
        String::from("my_node"),
        gethostname::gethostname().into_string().unwrap(),
        None,
        Some(NodeConnectionMode::Transitive)
    );

    let (pmd_ref, _pmd_handler) = Actor::spawn(None, pmd, ())
        .await.expect("Failed to spawn port mapper daemon");

    ractor_cluster::client_connect(
        &pmd_ref,
        format!("{}:{}",
            std::env::var("CLUSTER_HOST").unwrap_or("127.0.0.1".to_string()),
            std::env::var("CLUSTER_PORT").unwrap_or("6381".to_string()))
    ).await.expect("Failed to connect to node server");
    
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    
    let (_ref, _handler) = Actor::spawn(Some(String::from("my_remote_db_actor")), DBActor, 0u64..64u64).await.expect("");

    tokio::signal::ctrl_c().await.expect("Failed waiting for ctrl c");
}

