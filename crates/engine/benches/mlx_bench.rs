//! Benchmark: CPU backend vs MLX backend on large numeric operations.
//!
//! Run with:
//!   cargo bench -p blazer-engine --features mlx --bench mlx_bench
//!
//! On non-macOS or without the `mlx` feature, only the CPU benchmarks run.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use blazer_engine::compute::backend::{BackendPreference, CpuBackend, ComputeBackend};
use blazer_engine::series::Series;

#[cfg(all(target_os = "macos", feature = "mlx"))]
use blazer_engine::compute::backend::MlxBackend;

// ------------------------------------------------------------------ //
// Helpers                                                              //
// ------------------------------------------------------------------ //

fn make_f64_series(name: &str, n: usize) -> Series {
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 0.001).collect();
    Series::from_f64(name, values)
}

fn make_i64_series(name: &str, n: usize) -> Series {
    let values: Vec<i64> = (0..n as i64).collect();
    Series::from_i64(name, values)
}

// ------------------------------------------------------------------ //
// Arithmetic benchmarks                                                //
// ------------------------------------------------------------------ //

fn bench_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("add");

    for &n in &[100_000usize, 1_000_000, 5_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        let left = make_f64_series("a", n);
        let right = make_f64_series("b", n);

        group.bench_with_input(BenchmarkId::new("cpu", n), &n, |b, _| {
            let backend = CpuBackend;
            b.iter(|| black_box(backend.add(&left, &right).unwrap()));
        });

        #[cfg(all(target_os = "macos", feature = "mlx"))]
        group.bench_with_input(BenchmarkId::new("mlx", n), &n, |b, _| {
            let backend = MlxBackend;
            b.iter(|| black_box(backend.add(&left, &right).unwrap()));
        });
    }

    group.finish();
}

// ------------------------------------------------------------------ //
// Aggregation benchmarks                                               //
// ------------------------------------------------------------------ //

fn bench_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum");

    for &n in &[100_000usize, 1_000_000, 10_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        let series = make_f64_series("x", n);

        group.bench_with_input(BenchmarkId::new("cpu", n), &n, |b, _| {
            let backend = CpuBackend;
            b.iter(|| black_box(backend.sum(&series).unwrap()));
        });

        #[cfg(all(target_os = "macos", feature = "mlx"))]
        group.bench_with_input(BenchmarkId::new("mlx", n), &n, |b, _| {
            let backend = MlxBackend;
            b.iter(|| black_box(backend.sum(&series).unwrap()));
        });
    }

    group.finish();
}

fn bench_mean(c: &mut Criterion) {
    let mut group = c.benchmark_group("mean");

    for &n in &[100_000usize, 1_000_000, 10_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        let series = make_f64_series("x", n);

        group.bench_with_input(BenchmarkId::new("cpu", n), &n, |b, _| {
            let backend = CpuBackend;
            b.iter(|| black_box(backend.mean(&series).unwrap()));
        });

        #[cfg(all(target_os = "macos", feature = "mlx"))]
        group.bench_with_input(BenchmarkId::new("mlx", n), &n, |b, _| {
            let backend = MlxBackend;
            b.iter(|| black_box(backend.mean(&series).unwrap()));
        });
    }

    group.finish();
}

// ------------------------------------------------------------------ //
// Sort benchmarks                                                      //
// ------------------------------------------------------------------ //

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");

    for &n in &[100_000usize, 1_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        let series = make_f64_series("x", n);

        group.bench_with_input(BenchmarkId::new("cpu_asc", n), &n, |b, _| {
            let backend = CpuBackend;
            b.iter(|| black_box(backend.sort(&series, false).unwrap()));
        });

        group.bench_with_input(BenchmarkId::new("cpu_desc", n), &n, |b, _| {
            let backend = CpuBackend;
            b.iter(|| black_box(backend.sort(&series, true).unwrap()));
        });

        #[cfg(all(target_os = "macos", feature = "mlx"))]
        group.bench_with_input(BenchmarkId::new("mlx_asc", n), &n, |b, _| {
            let backend = MlxBackend;
            b.iter(|| black_box(backend.sort(&series, false).unwrap()));
        });

        #[cfg(all(target_os = "macos", feature = "mlx"))]
        group.bench_with_input(BenchmarkId::new("mlx_desc", n), &n, |b, _| {
            let backend = MlxBackend;
            b.iter(|| black_box(backend.sort(&series, true).unwrap()));
        });
    }

    group.finish();
}

// ------------------------------------------------------------------ //
// Comparison benchmarks                                                //
// ------------------------------------------------------------------ //

fn bench_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("gt");

    for &n in &[100_000usize, 1_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        let left = make_f64_series("a", n);
        let right = make_f64_series("b", n);

        group.bench_with_input(BenchmarkId::new("cpu", n), &n, |b, _| {
            let backend = CpuBackend;
            b.iter(|| black_box(backend.gt_series(&left, &right).unwrap()));
        });

        #[cfg(all(target_os = "macos", feature = "mlx"))]
        group.bench_with_input(BenchmarkId::new("mlx", n), &n, |b, _| {
            let backend = MlxBackend;
            b.iter(|| black_box(backend.gt_series(&left, &right).unwrap()));
        });
    }

    group.finish();
}

// ------------------------------------------------------------------ //
// Multiply (FLOPS-intensive)                                           //
// ------------------------------------------------------------------ //

fn bench_mul(c: &mut Criterion) {
    let mut group = c.benchmark_group("mul");

    for &n in &[1_000_000usize, 10_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        let left = make_f64_series("a", n);
        let right = make_f64_series("b", n);

        group.bench_with_input(BenchmarkId::new("cpu", n), &n, |b, _| {
            let backend = CpuBackend;
            b.iter(|| black_box(backend.mul(&left, &right).unwrap()));
        });

        #[cfg(all(target_os = "macos", feature = "mlx"))]
        group.bench_with_input(BenchmarkId::new("mlx", n), &n, |b, _| {
            let backend = MlxBackend;
            b.iter(|| black_box(backend.mul(&left, &right).unwrap()));
        });
    }

    group.finish();
}

// ------------------------------------------------------------------ //
// Criterion registration                                               //
// ------------------------------------------------------------------ //

criterion_group!(benches, bench_add, bench_sum, bench_mean, bench_sort, bench_compare, bench_mul);
criterion_main!(benches);
