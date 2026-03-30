use blazer_engine::prelude::*;
use blazer_engine::dataset::Dataset;
use blazer_engine::io::ParquetWriter;

/// Helper: write N rows of test data to a Parquet file.
fn write_test_parquet(path: &str, n_rows: usize) {
    let depts = ["Eng", "PM", "Design"];
    let ids: Vec<i64> = (0..n_rows as i64).collect();
    let salaries: Vec<f64> = (0..n_rows).map(|i| 50000.0 + (i % 100000) as f64).collect();
    let dept_vals: Vec<&str> = (0..n_rows).map(|i| depts[i % 3]).collect();

    let df = DataFrame::new(vec![
        Series::from_i64("id", ids),
        Series::from_f64("salary", salaries),
        Series::from_str("dept", dept_vals),
    ])
    .unwrap();

    ParquetWriter::from_path(path).finish(&df).unwrap();
}

#[test]
fn test_sink_parquet_streaming() {
    let src = "/tmp/blazer_test_src.parquet";
    let dst = "/tmp/blazer_test_dst.parquet";
    write_test_parquet(src, 100_000);

    let rows = LazyFrame::scan_parquet(src)
        .filter(col("salary").gt(lit(60000.0f64)))
        .select(vec![col("id"), col("dept"), col("salary")])
        .sink_parquet(dst)
        .unwrap();

    assert!(rows > 0 && rows < 100_000, "rows = {}", rows);

    // Read back and verify
    let result = LazyFrame::scan_parquet(dst).collect().unwrap();
    assert_eq!(result.height(), rows);
    assert_eq!(result.width(), 3);
}

#[test]
fn test_streaming_group_by() {
    let src = "/tmp/blazer_test_agg.parquet";
    write_test_parquet(src, 500_000);

    let result = LazyFrame::scan_parquet(src)
        .group_by(vec![col("dept")])
        .agg(vec![
            col("salary").sum().alias("total"),
            col("salary").mean().alias("avg"),
            col("id").count().alias("cnt"),
        ])
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 3); // 3 unique depts
}

#[test]
fn test_dataset_partition_pruning() {
    use std::fs;

    let base = "/tmp/blazer_part";
    let _ = fs::remove_dir_all(base);
    fs::create_dir_all(format!("{base}/year=2023")).ok();
    fs::create_dir_all(format!("{base}/year=2024")).ok();
    write_test_parquet(&format!("{base}/year=2023/part-0.parquet"), 1_000);
    write_test_parquet(&format!("{base}/year=2024/part-0.parquet"), 2_000);

    let ds = Dataset::scan_parquet(base).unwrap();

    // Partition filter: only year=2024
    let matching = ds
        .matching_files(&[col("year").eq(lit(2024i64))])
        .unwrap();
    assert_eq!(matching.len(), 1);
    assert!(matching[0].to_str().unwrap().contains("year=2024"));
}

#[test]
fn test_spill_manager() {
    use blazer_engine::io::spill::SpillManager;

    let df = DataFrame::new(vec![
        Series::from_i64("x", vec![1, 2, 3, 4, 5]),
        Series::from_f64("y", vec![1.1, 2.2, 3.3, 4.4, 5.5]),
    ])
    .unwrap();

    let mut spill = SpillManager::new(1024 * 1024, None).unwrap(); // 1MB budget
    let path = spill.spill(&df).unwrap();
    let restored = spill.read(&path).unwrap();

    assert_eq!(restored.height(), 5);
    assert_eq!(restored.width(), 2);
}

#[test]
fn test_parallel_scan() {
    use blazer_engine::compute::parallel_scan::ParallelScanner;

    let files: Vec<_> = (0..4)
        .map(|i| {
            let p = format!("/tmp/blazer_scan_{i}.parquet");
            write_test_parquet(&p, 10_000);
            std::path::PathBuf::from(p)
        })
        .collect();

    let result = ParallelScanner::new(files)
        .with_projection(vec!["id".into(), "salary".into()])
        .with_predicate(col("salary").gt(lit(55000.0f64)))
        .scan_and_collect()
        .unwrap();

    assert!(result.height() > 0, "height = {}", result.height());
    assert_eq!(result.width(), 2);
}

#[test]
fn test_limit_stream() {
    let src = "/tmp/blazer_limit.parquet";
    write_test_parquet(src, 50_000);

    let result = LazyFrame::scan_parquet(src)
        .limit(1000)
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 1000);
}

#[test]
fn test_large_sort_with_spill() {
    let src = "/tmp/blazer_sort.parquet";
    write_test_parquet(src, 200_000);

    let result = LazyFrame::scan_parquet(src)
        .with_streaming_budget(1024 * 1024) // 1MB — forces spill
        .sort("salary", SortOptions::descending())
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 200_000);

    // Verify sort order
    let salaries = result.column("salary").unwrap().as_f64().unwrap();
    assert!(
        salaries.value(0) >= salaries.value(1),
        "first {} >= second {}",
        salaries.value(0),
        salaries.value(1)
    );
    assert!(
        salaries.value(1) >= salaries.value(2),
        "second {} >= third {}",
        salaries.value(1),
        salaries.value(2)
    );
}
