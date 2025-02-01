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
use acdis::db_actor::message::DBMessage;
use acdis::node_manager_actor::actor::{NodeManagerActor, NodeType};

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

    let (_manager_ref, _manager_handler) = Actor::spawn(None, NodeManagerActor, NodeType::Client)
                                            .await.expect("Failed to spawn node manager");
    
    let mut members = pg::get_members(&String::from("acdis"));
    sort_actors_by_keyspace(&mut members);

    info!("Found {} db actors", members.len());

    for member in members {
        let actor_ref = ActorRef::<DBMessage>::from(member);
        let keyspace = call!(actor_ref, DBMessage::QueryKeyspace).expect("Failed querying keyspace");
        info!("{} has keyspace {:?}", actor_ref.get_name().unwrap_or(String::from("Actor")), keyspace);
    }
    
    tokio::signal::ctrl_c().await.expect("Failed waiting for ctrl c");
}

