#![allow(unused_imports)]
use std::cmp::Ordering;
use std::ops::Range;
use futures::future::join_all;
use log::{info, Level, LevelFilter};
use ractor::{call, pg, Actor, ActorCell, ActorRef};
use ractor_cluster;
use ractor_cluster::node::NodeConnectionMode;
use simplelog::{Color, ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode};

use acdis::db_actor::actor::DBActor;
use acdis::tcp_listener_actor::tcp_listener::chunk_ranges;
use acdis::db_actor::message::DBMessage;

use rand::Rng;

fn setup_logging() {
    let logconfig = ConfigBuilder::new()
        .set_level_color(Level::Error, Some(Color::Red))
        .set_level_color(Level::Warn, Some(Color::Yellow))
        .set_level_color(Level::Info, Some(Color::Green))
        .set_level_color(Level::Debug, Some(Color::Blue))
        .set_target_level(LevelFilter::Debug)
        .build();

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Debug,  logconfig.clone(), TerminalMode::Mixed, ColorChoice::Auto),
        ]
    ).unwrap();
}

fn sort_actors_by_keyspace(actors: &mut Vec<ActorCell>) {
    actors.sort_by(|actor_a, actor_b| {
        let ref_actor_a = ActorRef::<DBMessage>::from(actor_a.clone());
        let ref_actor_b = ActorRef::<DBMessage>::from(actor_b.clone());

        futures::executor::block_on(async {
            let keyspace_a: Range<u64> = call!(ref_actor_a, DBMessage::QueryKeyspace).unwrap();
            let keyspace_b: Range<u64> = call!(ref_actor_b, DBMessage::QueryKeyspace).unwrap();

            return if (keyspace_a.end - keyspace_a.start) > (keyspace_b.end - keyspace_b.start) {
                Ordering::Greater
            } else if (keyspace_a.end - keyspace_a.start) < (keyspace_b.end - keyspace_b.start) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        })
    });
}
#[tokio::main]
async fn main() {
    setup_logging();
    
    let mut rng = rand::thread_rng();
    
    let pmd = ractor_cluster::NodeServer::new(
        0,
        std::env::var("CLUSTER_COOKIE").unwrap_or("cookie".to_string()),
        format!("Node {}", rng.gen::<u8>()),
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
    
    let mut members = pg::get_members(&String::from("acdis"));
    sort_actors_by_keyspace(&mut members);
    
    info!("Found {} db actors", members.len());

    for member in members {
        let actor_ref = ActorRef::<DBMessage>::from(member);
        let keyspace = call!(actor_ref, DBMessage::QueryKeyspace).expect("Failed querying keyspace");
        info!("{} has keyspace {:?}", actor_ref.get_name().unwrap_or(String::from("Actor")), keyspace);
    }
    
    // let (_ref, _handler) = Actor::spawn(Some(String::from("my_remote_db_actor")), DBActor, 0u64..64u64).await.expect("");

    tokio::signal::ctrl_c().await.expect("Failed waiting for ctrl c");
}

