use std::sync::{mpsc, Arc};
use std::sync::mpsc::{Receiver, Sender};
use ractor::{async_trait, Actor, ActorProcessingErr, RpcReplyPort, ActorRef, cast, Message};
use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};

use futures::future::join_all;

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

async fn call_actor(actor: Arc<ActorRef<BenchMessage>>, num: usize) {
    // TODO: Move this out of benchmark, allocate with proper size
    let mut handles = Vec::new();
    
    for _ in 0..num {
        let actor2 = actor.clone();
        handles.push(tokio::spawn(async move { // TODO: Don't use tokio::spawn, put future into vec
            actor2.call(BenchMessage::Foo, None).await.unwrap()
        }));
    }
    
    join_all(handles).await;
    
}

async fn cast_actor(actor: &ActorRef<BenchMessage>, num: usize) {
    for _ in 0..num {
        cast!(actor, BenchMessage::Bar()).expect("RPC failed");
    }
}

fn send_messages_through_channels(tx: &Sender<String>, rx: &Receiver<String>, num: usize) {
    for _ in 0..num {
        tx.send(String::from("Ping")).unwrap();
    }

    for _ in 0..num {
        rx.recv().unwrap();
    }
}

async fn tokio_mpsc(num: usize) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(num);

    for _ in 0..num {
        let tx2 = tx.clone();
        tokio::spawn(async move {
            tx2.send(String::from("Ping")).await.unwrap();
        });
    }
    
    for _ in 0..num {
        rx.recv().await.unwrap();
    }
    
}

fn criterion_benchmark(c: &mut Criterion) {

    let mut group = c.benchmark_group("messages");
    let messages = 1_000_000; // TODO: MORE 100_000_000

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut actor_arc = Arc::<ActorRef<BenchMessage>>::new_uninit();
    
    rt.block_on(async {
        let (actor, _handle) = Actor::spawn(None, BenchActor, ()).await.unwrap();
        //actors.push(actor);
        Arc::get_mut(&mut actor_arc).unwrap().write(actor);
    });
    
    let actor_arc = unsafe { actor_arc.assume_init() };

    group.bench_function("Call actors", |b| {
        b.to_async(&rt).iter(|| black_box(call_actor(actor_arc.clone(), messages)));
    });

    group.bench_function("Cast actors", |b| {
        b.to_async(&rt).iter(|| black_box(cast_actor(&*actor_arc, messages)));
    });
    
    group.bench_function("Message passing rust channels", |b| {
        let (tx, rx) = mpsc::channel();
        b.iter(move || {
            black_box(send_messages_through_channels(&tx, &rx, messages));
        })
    });
    
    group.bench_function("tokio mpsc", |b| {
        b.to_async(&rt).iter(move || black_box(tokio_mpsc(messages)));
    });
    
    // TODO: actor_cell.send_message()

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
