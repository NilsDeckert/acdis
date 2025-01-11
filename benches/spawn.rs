use std::sync::mpsc;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::mpsc::Sender;

struct BenchActor;
enum BenchMessage {}

#[async_trait]
impl Actor for BenchActor {
    type Msg = BenchMessage;

    type State = Sender<String>;

    type Arguments = Sender<String>;

    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr>{
        Ok(args)
    }

    async fn post_start(&self, _myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        state.send(String::from("Ping")).expect("Error sending to channel");
        Ok(())
    }

    async fn handle(&self, _myself: ActorRef<Self::Msg>, _message: Self::Msg, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}


async fn spawn_actor() {
    let (tx, rx) = mpsc::channel();
    
    let (_actor1, _handle) =
        Actor::spawn(None, BenchActor, tx).await
            .expect("Failed to start actor");

    rx.recv().expect("Failed to receive from channel");
}

async fn spawn_task() {
    let (tx, rx) = mpsc::channel();
    black_box({
        tokio::spawn(async move {
            tx.send(String::from("Ping")).expect("Error sending to channel");
        });
    });
    
    rx.recv().expect("Failed to receive from channel");
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawning");
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    group.bench_function("Spawn actor", |b| {
        b.to_async(&rt).iter(|| black_box(spawn_actor()));
    });
    
    group.bench_function("Spawn Task", |b| {
        b.to_async(&rt).iter(|| black_box(spawn_task()));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
