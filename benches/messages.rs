use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef, Message, RpcReplyPort};
/// This file groups benchmarks concerning message passing.
///
/// The following ways of message passing are compared:
///  - [`ractor`] Actors
///  - Rust Channels
///  - Tokio Channels
///  - async_channel
use std::thread;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/////
// This section sets up the necessary structs and enums to pass simple messages using the ractor
// crate.
/////

struct BenchActor;
enum BenchMessage {
    Foo(RpcReplyPort<String>),
    Bar(),
}

impl Message for BenchMessage {}

#[async_trait]
impl Actor for BenchActor {
    type Msg = BenchMessage;

    type State = ();

    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<Self::State, ActorProcessingErr> {
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

/// Send messages to an actor without waiting for a reply
async fn cast_actor(num: u64) -> Duration {
    let (actor, handle) = Actor::spawn(None, BenchActor, ()).await.unwrap();
    let start = Instant::now();

    black_box(for _ in 0..num {
        cast!(actor, BenchMessage::Bar()).expect("RPC failed");
    });

    let elapsed = start.elapsed();
    actor.stop(None);
    handle.await.unwrap();

    elapsed
}

/// Use a rust channel to send and receive messages
fn rust_mpsc(num: u64) -> Duration {
    let (tx, rx) = mpsc::channel();

    let start = Instant::now();

    black_box(for _ in 0..num {
        tx.send(String::from("Ping")).unwrap();
    });

    black_box(for _ in 0..num {
        rx.recv().unwrap();
    });

    start.elapsed()
}

async fn tokio_mpsc(num: u64) -> Duration {
    let (tx, mut rx) = tokio::sync::mpsc::channel(num as usize);

    let start = Instant::now();

    black_box(for _ in 0..num {
        tx.send(String::from("Ping")).await.unwrap();
    });

    black_box(for _ in 0..num {
        rx.recv().await.unwrap();
    });

    let elapsed = start.elapsed();
    rx.close();
    elapsed
}

async fn tokio_unbounded_receiver(num: u64) -> Duration {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let start = Instant::now();

    black_box(for _ in 0..num {
        tx.send(String::from("Ping")).unwrap();
    });

    black_box(for _ in 0..num {
        rx.recv().await.unwrap();
    });

    start.elapsed()
}

async fn async_channels_unbounded(num: u64) -> Duration {
    let (tx, rx) = async_channel::unbounded();
    let start = Instant::now();

    // for _ in 0..num {
    //     tx.send(String::from("Ping")).await.unwrap();
    // };
    //
    // for _ in 0..num {
    //     rx.recv().await.unwrap();
    // };

    black_box(for _ in 0..num {
        tx.send(String::from("Ping")).await.unwrap();
    });

    black_box(for _ in 0..num {
        rx.recv().await.unwrap();
    });

    let elapsed = start.elapsed();
    tx.close();
    rx.close();

    elapsed
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("messages");

    group.bench_function("Rust mpsc", |b| {
        b.iter_custom(|messages| black_box(rust_mpsc(messages)))
    });
    //
    // group.bench_function("Cast actors", |b| {
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //     b.to_async(&rt)
    //         .iter_custom(|messages| black_box(cast_actor(messages)));
    // });
    //
    // group.bench_function("async_channels unbounded", |b| {
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //     b.to_async(&rt)
    //         .iter_custom(|messages| black_box(async_channels_unbounded(messages)))
    // });

    // group.bench_function("Tokio mpsc", |b| {
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //     b.to_async(&rt)
    //         .iter_custom(|messages| black_box(tokio_mpsc(messages)));
    // });

    // group.bench_function("Tokio unbounded", |b| {
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //     b.to_async(&rt)
    //         .iter_custom(|messages| black_box(tokio_unbounded_receiver(messages)));
    // });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
