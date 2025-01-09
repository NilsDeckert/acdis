use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};

use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;

fn send_messages_through_channels(tx: Sender<String>, rx: Receiver<String>, num: usize) {
    for _ in 0..num {
        tx.send(String::from("Ping")).unwrap();
    }

    for _ in 0..num {
        rx.recv().unwrap();
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Send messages through channel", |b| {
        b.iter(|| {
            let (tx, rx) = mpsc::channel();
            black_box(send_messages_through_channels(tx, rx, 10_000));
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
