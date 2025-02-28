use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::thread::sleep;

#[inline]
fn function_to_benchmark(input: u8) -> u8 {
    let mut a = 0;
    for _ in 0..10 {
        a += input
    }
    a
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("benchmark without sleep", |b| {
        b.iter(|| black_box(function_to_benchmark(1)))
    });

    // Calls inside bench_function() but outside iter()
    //  do not count towards the benchmark time
    c.bench_function("benchmark with sleep", |b| {
        sleep(core::time::Duration::from_millis(500));
        b.iter(|| black_box(function_to_benchmark(1)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
