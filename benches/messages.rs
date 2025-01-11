use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use ractor::{async_trait, call, Actor, ActorProcessingErr, RpcReplyPort, ActorRef};
use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};

struct BenchActor;
enum BenchMessage {
    Foo(RpcReplyPort<String>)
}

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
        }
        Ok(())
    }

}

async fn call_actor(actor: &ActorRef<BenchMessage>, num: usize) {
    for _ in 0..num {
        call!(actor, BenchMessage::Foo).expect("RPC failed");
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
fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("messages");
    let messages = 1_000;

    group.bench_function("Message passing actors", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut actors: Vec<ActorRef<BenchMessage>> = Vec::new();
        rt.block_on(async {
            let (actor, _handle) = Actor::spawn(None, BenchActor, ()).await.unwrap();
            actors.push(actor);
        });
        b.to_async(&rt).iter(|| black_box(call_actor(&actors.first().unwrap(), messages)));
    });
    
    group.bench_function("Message passing rust channels", |b| {
        let (tx, rx) = mpsc::channel();
        b.iter(move || {
            black_box(send_messages_through_channels(&tx, &rx, messages));
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
