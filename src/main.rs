use std::ops::Deref;
use log::{error, info, Level, LevelFilter};
use ractor::{Actor, ActorRef};
use simplelog::{Color, ColorChoice, CombinedLogger, Config, ConfigBuilder, TermLogger, TerminalMode};
use crate::db_actor::{DBActor, DBMessage, MapEntry};

mod db_actor;

fn setup_logging() {
    let logconfig = ConfigBuilder::new()
        .set_level_color(Level::Error, Some(Color::Red))
        .set_level_color(Level::Warn, Some(Color::Yellow))
        .set_level_color(Level::Info, Some(Color::Green))
        .set_target_level(LevelFilter::Info)
        .build();

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Warn, Config::default(), TerminalMode::Mixed, ColorChoice::Auto),
            TermLogger::new(LevelFilter::Info, logconfig, TerminalMode::Mixed, ColorChoice::Auto),
        ]
    ).unwrap();
}

async fn get(dbactor: &ActorRef<DBMessage>, key: String) {
    let call_result = ractor::rpc::call(
        &dbactor.get_cell(),
        |reply|
            DBMessage::GET(key, reply),
        None
    ).await.expect("Failed to send message");

    match call_result {
        ractor::rpc::CallResult::Success(res) => {
            info!("Received reply: {}", res);
        }
        ractor::rpc::CallResult::Timeout => error!("Call timed out"),
        ractor::rpc::CallResult::SenderError => error!("Transmission channel dropped before any message")
    }
}

#[tokio::main]
async fn main() {
    setup_logging();

    let (dbactor, _handler) = Actor::spawn(
        Some("DB Actor".to_string()),
        DBActor,
        ()
    ).await.expect("Failed to spawn DBActor");

    dbactor.cast(
        DBMessage::INSERT(
            "key".to_string(),
            MapEntry::STRING("value".to_string())
        )
    ).expect("Failed to send message");


    dbactor.cast(
        DBMessage::INSERT(
            "number".to_string(),
            MapEntry::USIZE(7usize)
        )
    ).expect("Failed to send message");
    
    get(&dbactor, "key".to_string()).await;
    get(&dbactor, "number".to_string()).await;
    
}
