use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, Message};
use std::thread;

struct BenchActor;
enum BenchMessage {}

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

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

async fn spawn_destroy_actor() {
    let (actor, handle) = Actor::spawn(None, BenchActor, ())
        .await
        .expect("Failed to start actor");

    actor.stop(None);
    handle.await.unwrap();
}

async fn spawn_destroy_task() {
    let handle = tokio::spawn(async {
        black_box(());
    });

    handle.await.unwrap();
}

fn spawn_destroy_thread() {

    let handle = thread::spawn(|| {
        black_box(());
    });

    handle.join().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawning");

    group.bench_function("Spawn & Destroy Thread", |b| {
        b.iter(|| black_box(spawn_destroy_thread()));
    });

    group.bench_function("Spawn & Destroy Task", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap(); // Not timed
        b.to_async(&rt).iter(|| black_box(spawn_destroy_task()));
    });

    group.bench_function("Spawn & Destroy Actor", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap(); // Not timed
        b.to_async(&rt).iter(|| black_box(spawn_destroy_actor()));
    });

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
