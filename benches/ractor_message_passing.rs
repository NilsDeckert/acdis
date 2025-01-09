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


async fn spawn_actor() {
    let (_actor1, handle) =
        ractor::Actor::spawn(None, BenchActor, ()).await
            .expect("Failed to start actor");

    drop(handle);
}
async fn call_actor(actor: ActorRef<BenchMessage>) {

    call!(actor, BenchMessage::Foo).expect("RPC failed");
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("Spawn actor", |b| {
        b.to_async(&rt).iter(|| black_box(spawn_actor()));
    });
    
    let mut actors: Vec<ActorRef<BenchMessage>> = Vec::new();
    
    rt.block_on(async {
        let (actor, _handle) = Actor::spawn(None, BenchActor, ()).await.unwrap();
        actors.push(actor);
    });

    c.bench_function("Call actor", |b| {
        b.iter(|| black_box(call_actor(actors.first().unwrap().clone())));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
