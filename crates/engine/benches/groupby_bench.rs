use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use blazer_engine::prelude::*;
use blazer_engine::compute::backend::{init_backend, BackendPreference};

fn make_df(n: usize) -> DataFrame {
    let depts = ["Eng", "PM", "Design", "Marketing", "Sales"];
    DataFrame::new(vec![
        Series::from_i64("id", (0..n as i64).collect()),
        Series::from_f64("salary", (0..n).map(|i| 50000.0 + (i % 100000) as f64).collect()),
        Series::from_str("dept", (0..n).map(|i| depts[i % depts.len()]).collect()),
        Series::from_i64("age", (0..n).map(|i| 22 + (i % 43) as i64).collect()),
    ]).unwrap()
}

fn bench_groupby(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_agg");

    for n in [10_000, 100_000] {
        let df = make_df(n);

        init_backend(BackendPreference::Cpu);
        group.bench_with_input(BenchmarkId::new("cpu", n), &df, |b, df| {
            b.iter(|| {
                black_box(df.clone().lazy()
                    .group_by(vec![col("dept")])
                    .agg(vec![
                        col("salary").sum().alias("total"),
                        col("salary").mean().alias("avg"),
                        col("age").max().alias("max_age"),
                    ])
                    .collect().unwrap())
            })
        });
    }
    group.finish();
}

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");
    for n in [100_000] {
        let df = make_df(n);
        init_backend(BackendPreference::Cpu);
        group.bench_with_input(BenchmarkId::new("cpu", n), &df, |b, df| {
            b.iter(|| {
                black_box(df.clone().lazy()
                    .filter(col("salary").gt(lit(100_000.0f64)))
                    .collect().unwrap())
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_groupby, bench_filter);
criterion_main!(benches);
