use criterion::{criterion_group, criterion_main};
fn bench_correctness(c: &mut criterion::Criterion) {
    c.benchmark_group("correctness");
}
criterion_group!(benches, bench_correctness);
criterion_main!(benches);
