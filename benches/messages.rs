use std::os::linux::raw::stat;
/// This file groups benchmarks concerning message passing.
/// 
/// The following ways of message passing are compared:
///  - [`ractor`] Actors
///  - Rust Channels
///  - Tokio Channels
///  - async_channel


use std::sync::{mpsc};
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};
use ractor::{async_trait, Actor, ActorProcessingErr, RpcReplyPort, ActorRef, cast, Message};
use criterion::{black_box};
use criterion::{criterion_group, criterion_main, Criterion};

use futures::future::join_all;

/////
// This section sets up the necessary structs and enums to pass simple messages using the ractor
// crate.
/////

struct BenchActor;
enum BenchMessage {
    Foo(RpcReplyPort<String>),
    Bar()
}

impl Message for BenchMessage {}

#[async_trait]
impl Actor for BenchActor {
    type Msg = BenchMessage;

    type State = ();

    type Arguments = ();

    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, _args: Self::Arguments) -> Result<Self::State, ActorProcessingErr>{
        Ok(())
    }

    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, _state: &mut Self::State) -> Result<Self::State, ActorProcessingErr> {
        match message {
            BenchMessage::Foo(repl) => {
                if !repl.is_closed() {
                    repl.send("Reply".into()).unwrap();
                }
            }
            BenchMessage::Bar() => {}
        }
        Ok(())
    }

}

/////
// Benchmarking functions start here
////

/// In parallel, send messages to an actor and wait for a reply
async fn call_actor(num: u64) -> Duration{
    // Preparation outside timed frame
    let (actor, handle) = Actor::spawn(None, BenchActor, ()).await.unwrap();
    let mut handles = vec![];
    
    let start = Instant::now();
    
    for _ in 0..num {
        let actor2 = actor.clone();
        handles.push(tokio::spawn(async move { // TODO: Don't use tokio::spawn, put future into vec
            actor2.call(BenchMessage::Foo, None).await.unwrap()
        }));
    }
    
    join_all(handles).await;

    let elapsed = start.elapsed();
    actor.stop(None);
    handle.await.unwrap();

    elapsed
}

/// Send messages to an actor without waiting for a reply
async fn cast_actor(num: u64) -> Duration {
    let (actor, handle) = Actor::spawn(None, BenchActor, ()).await.unwrap();
    let start = Instant::now();
    
    black_box(
        for _ in 0..num {
            cast!(actor, BenchMessage::Bar()).expect("RPC failed");
        }
    );
    
    let elapsed = start.elapsed();
    actor.stop(None);
    handle.await.unwrap();

    elapsed
}

/// Use a rust channel to send and receive messages
fn send_messages_through_channels(num: u64) -> Duration {
    let (tx, rx) = mpsc::channel();

    let start = Instant::now();

    for _ in 0..num {
        tx.send(String::from("Ping")).unwrap();
    }

    for _ in 0..num {
        rx.recv().unwrap();
    }
    start.elapsed()
}

/// In parallel, use a tokio channel to send and receive messages 
async fn tokio_mpsc_parallel(num: u64) -> Duration {
    let (tx, mut rx) = tokio::sync::mpsc::channel(num as usize);

    let start = Instant::now();

    for _ in 0..num {
        let tx2 = tx.clone();
        tokio::spawn(async move {
            tx2.send(String::from("Ping")).await.unwrap();
        });
    }
    
    for _ in 0..num {
        rx.recv().await.unwrap();
    }

    let elapsed = start.elapsed();
    rx.close();

    elapsed
}

async fn tokio_mpsc_blocking(num: u64) -> Duration {
    let (tx, mut rx) = tokio::sync::mpsc::channel(num as usize);

    let start = Instant::now();

    for _ in 0..num {
        tx.send(String::from("Ping")).await.unwrap();
    }

    for _ in 0..num {
        rx.recv().await.unwrap();
    }

    let elapsed = start.elapsed();
    rx.close();
    elapsed
}

async fn async_channels_parallel_A(num: u64) -> Duration {
    let (tx, rx) = async_channel::unbounded();
    let start = Instant::now();

    for _ in 0..num {
        let tx2 = tx.clone();
        tokio::spawn(async move {
            tx2.send(String::from("Ping")).await.unwrap();
        });
    }

    for _ in 0..num {
        rx.recv().await.unwrap();
    }

    let elapsed = start.elapsed();
    rx.close();
    tx.close();

    elapsed
}

async fn async_channels_parallel_B(num: u64) -> Duration {
    let (tx, rx) = async_channel::unbounded();
    let start = Instant::now();

    for _ in 0..num {
        tx.send(String::from("Ping")).await.unwrap();
    }

    for _ in 0..num {
        let rx2 = rx.clone();
        tokio::spawn(async move {
            rx2.recv().await.unwrap();
        });
    }


    let elapsed = start.elapsed();
    tx.close();

    elapsed
}

async fn async_channels_blocking(num: u64) -> Duration {
    let (tx, rx) = async_channel::unbounded();
    let start = Instant::now();

    for _ in 0..num {
        tx.send(String::from("Ping")).await.unwrap();
    }
    
    for _ in 0..num {
        rx.recv().await.unwrap();
    }

    let elapsed = start.elapsed();
    tx.close();
    rx.close();

    elapsed
}

fn criterion_benchmark(c: &mut Criterion) {

    let mut group = c.benchmark_group("messages");

    group.bench_function("Call actors", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|messages| black_box(call_actor(messages)));
    });

    group.bench_function("Cast actors", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|messages| black_box(cast_actor(messages)));
    });
    
    group.bench_function("Message passing rust channels", |b| {
        b.iter_custom(move |messages| {
            black_box(send_messages_through_channels(messages))
        })
    });

    group.bench_function("Message passing async_channels blocking", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|messages| {
            black_box(async_channels_blocking(messages))
        })
    });

    group.bench_function("Message passing async_channels parallel A", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|messages| {
            black_box(async_channels_parallel_A(messages))
        })
    });

    group.bench_function("Message passing async_channels parallel B", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|messages| {
            black_box(async_channels_parallel_A(messages))
        })
    });

    group.bench_function("tokio mpsc", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|messages| black_box(tokio_mpsc_parallel(messages)));
    });

    group.bench_function("tokio mpsc blocking", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(|messages| black_box(tokio_mpsc_blocking(messages)));
    });

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
