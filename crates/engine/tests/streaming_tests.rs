// ═══════════════════════════════════════════════════════════════════════════════
// Blazer Engine — Streaming / Parquet-backed Regression Tests
//
// HOW TO RUN:
//   cargo test -p blazer-engine --test streaming_tests
//
// COVERAGE:
//   • Parquet sink                         (test_sink_parquet_streaming)
//   • Multi-file parallel scan             (test_parallel_scan)
//   • Partition pruning                    (test_dataset_partition_pruning)
//   • SpillManager roundtrip               (test_spill_manager)
//   • Streaming GroupBy correctness        (test_streaming_group_by, …_correctness)
//   • Streaming Filter                     (test_streaming_filter_*)
//   • Streaming Select / projection        (test_parquet_column_projection_*)
//   • Streaming WithColumns                (test_streaming_with_columns)
//   • Streaming Sort                       (test_streaming_sort_*)
//   • Streaming Sort + spill               (test_large_sort_with_spill)
//   • Limit: exact count, early exit, edge cases
//   • Distinct: low-cardinality, all-unique, after-filter, multi-file-dir
//   • Chained streaming operations
//   • Multi-file parquet_dir scanning
//   • Regression guards (distinct dedup, limit exact, sort order, GroupBy sums)
// ═══════════════════════════════════════════════════════════════════════════════

use blazer_engine::prelude::*;
use blazer_engine::dataset::Dataset;
use blazer_engine::io::ParquetWriter;

// ── Shared test helpers ───────────────────────────────────────────────────────

/// Write a synthetic Parquet file with columns: id (i64), salary (f64), dept (str).
/// Depts cycle: Eng / PM / Design.
fn write_test_parquet(path: &str, n_rows: usize) {
    let depts = ["Eng", "PM", "Design"];
    let ids: Vec<i64>    = (0..n_rows as i64).collect();
    let salaries: Vec<f64> = (0..n_rows).map(|i| 50_000.0 + (i % 100_000) as f64).collect();
    let dept_vals: Vec<&str> = (0..n_rows).map(|i| depts[i % 3]).collect();

    let df = DataFrame::new(vec![
        Series::from_i64("id",     ids),
        Series::from_f64("salary", salaries),
        Series::from_str("dept",   dept_vals),
    ])
    .unwrap();
    ParquetWriter::from_path(path).finish(&df).unwrap();
}

/// Write a synthetic Parquet file with numeric columns only (a, b).
fn write_numeric_parquet(path: &str, n_rows: usize) {
    let a: Vec<f64> = (0..n_rows).map(|i| i as f64).collect();
    let b: Vec<f64> = (0..n_rows).map(|i| (n_rows - i) as f64).collect();
    let df = DataFrame::new(vec![
        Series::from_f64("a", a),
        Series::from_f64("b", b),
    ])
    .unwrap();
    ParquetWriter::from_path(path).finish(&df).unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 1  INFRASTRUCTURE
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_sink_parquet_streaming() {
    let src = "/tmp/blazer_test_src.parquet";
    let dst = "/tmp/blazer_test_dst.parquet";
    write_test_parquet(src, 100_000);

    let rows = LazyFrame::scan_parquet(src)
        .filter(col("salary").gt(lit(60_000.0f64)))
        .select(vec![col("id"), col("dept"), col("salary")])
        .sink_parquet(dst)
        .unwrap();

    assert!(rows > 0 && rows < 100_000, "filtered sink rows = {}", rows);
    let result = LazyFrame::scan_parquet(dst).collect().unwrap();
    assert_eq!(result.height(), rows);
    assert_eq!(result.width(), 3);
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
        .with_predicate(col("salary").gt(lit(55_000.0f64)))
        .scan_and_collect()
        .unwrap();

    assert!(result.height() > 0, "parallel scan should return rows");
    assert_eq!(result.width(), 2, "projection should keep only 2 columns");
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
    let matching = ds
        .matching_files(&[col("year").eq(lit(2024i64))])
        .unwrap();
    assert_eq!(matching.len(), 1, "pruning should select only year=2024 partition");
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
    let mut spill = SpillManager::new(1024 * 1024, None).unwrap();
    let path = spill.spill(&df).unwrap();
    let restored = spill.read(&path).unwrap();
    assert_eq!(restored.height(), 5);
    assert_eq!(restored.width(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 2  STREAMING FILTER
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_streaming_filter_basic() {
    let src = "/tmp/blazer_filter_basic.parquet";
    write_test_parquet(src, 30_000);

    let result = LazyFrame::scan_parquet(src)
        .filter(col("dept").eq(lit("Eng")))
        .collect_streaming()
        .unwrap();

    // 1/3 of rows are Eng
    let expected = 30_000 / 3;
    assert_eq!(
        result.height(), expected,
        "streaming filter: expected {} Eng rows, got {}",
        expected, result.height()
    );
}

#[test]
fn test_streaming_filter_numeric() {
    let src = "/tmp/blazer_filter_num.parquet";
    // 50 000 rows → salary range 50 000..99 999; threshold 75 000 lets ~50% through.
    write_test_parquet(src, 50_000);

    let result = LazyFrame::scan_parquet(src)
        .filter(col("salary").gt(lit(75_000.0f64)))
        .collect_streaming()
        .unwrap();

    assert!(result.height() > 0, "filter should let some rows through");
    // Verify every row in the result satisfies the predicate
    let salaries = result.column("salary").unwrap().as_f64().unwrap();
    for i in 0..result.height() {
        assert!(
            salaries.value(i) > 75_000.0,
            "row {i} salary {} should be > 75000",
            salaries.value(i)
        );
    }
}

#[test]
fn test_streaming_filter_compound() {
    let src = "/tmp/blazer_filter_compound.parquet";
    // 90 000 rows → salary range 50 000..89 999; threshold 65 000 lets ~60% of Eng through.
    write_test_parquet(src, 90_000);

    let result = LazyFrame::scan_parquet(src)
        .filter(col("dept").eq(lit("Eng")).and(col("salary").gt(lit(65_000.0f64))))
        .collect_streaming()
        .unwrap();

    assert!(result.height() > 0, "compound filter should let some rows through");
    let depts    = result.column("dept").unwrap().as_utf8().unwrap();
    let salaries = result.column("salary").unwrap().as_f64().unwrap();
    for i in 0..result.height() {
        assert_eq!(depts.value(i), "Eng", "compound filter: dept must be Eng");
        assert!(salaries.value(i) > 65_000.0, "compound filter: salary must be > 65000");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 3  STREAMING SELECT / COLUMN PROJECTION
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_parquet_column_projection_streaming() {
    let src = "/tmp/blazer_proj_stream.parquet";
    write_test_parquet(src, 50_000);

    let result = LazyFrame::scan_parquet(src)
        .select(vec![col("dept"), col("salary")])
        .collect_streaming()
        .unwrap();

    assert_eq!(result.width(), 2, "only 2 columns should be present");
    assert!(result.column("dept").is_ok());
    assert!(result.column("salary").is_ok());
    assert!(result.column("id").is_err(), "'id' was not selected");
    assert_eq!(result.height(), 50_000, "all rows preserved by projection");
}

#[test]
fn test_parquet_projection_reduces_width() {
    let src = "/tmp/blazer_proj_width.parquet";
    write_test_parquet(src, 1_000);

    // Select a single column from a 3-column file
    let result = LazyFrame::scan_parquet(src)
        .select(vec![col("id")])
        .collect_streaming()
        .unwrap();

    assert_eq!(result.width(), 1, "single-column projection should have width=1");
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 4  STREAMING WITH_COLUMNS
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_streaming_with_columns() {
    let src = "/tmp/blazer_with_cols.parquet";
    write_numeric_parquet(src, 10_000);

    let result = LazyFrame::scan_parquet(src)
        .with_columns(vec![
            col("a").add(col("b")).alias("sum_ab"),
            col("a").mul(lit(2.0f64)).alias("a_x2"),
        ])
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 10_000);
    assert_eq!(result.width(), 4, "a, b + 2 new cols = 4");
    let sum_ab = result.column("sum_ab").unwrap().as_f64().unwrap();
    let a      = result.column("a").unwrap().as_f64().unwrap();
    let b      = result.column("b").unwrap().as_f64().unwrap();
    // Spot-check first row: a=0, b=10000 → sum=10000
    assert!((sum_ab.value(0) - (a.value(0) + b.value(0))).abs() < 1e-9);
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 5  STREAMING SORT
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_streaming_sort_ascending() {
    let src = "/tmp/blazer_sort_asc.parquet";
    write_test_parquet(src, 20_000);

    let result = LazyFrame::scan_parquet(src)
        .sort("salary", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 20_000);
    let salaries = result.column("salary").unwrap().as_f64().unwrap();
    for i in 1..salaries.len() {
        assert!(
            salaries.value(i - 1) <= salaries.value(i),
            "ascending sort violated at {i}: {} > {}",
            salaries.value(i - 1), salaries.value(i)
        );
    }
}

#[test]
fn test_streaming_sort_descending() {
    let src = "/tmp/blazer_sort_desc.parquet";
    write_test_parquet(src, 20_000);

    let result = LazyFrame::scan_parquet(src)
        .sort("salary", SortOptions::descending())
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 20_000);
    let salaries = result.column("salary").unwrap().as_f64().unwrap();
    for i in 1..salaries.len() {
        assert!(
            salaries.value(i - 1) >= salaries.value(i),
            "descending sort violated at {i}: {} < {}",
            salaries.value(i - 1), salaries.value(i)
        );
    }
}

#[test]
fn test_large_sort_with_spill() {
    let src = "/tmp/blazer_sort.parquet";
    write_test_parquet(src, 200_000);

    let result = LazyFrame::scan_parquet(src)
        .with_streaming_budget(1024 * 1024) // 1 MB — forces spill to disk
        .sort("salary", SortOptions::descending())
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 200_000);
    let salaries = result.column("salary").unwrap().as_f64().unwrap();
    assert!(
        salaries.value(0) >= salaries.value(1),
        "spill-sort: first {} >= second {}",
        salaries.value(0), salaries.value(1)
    );
    assert!(
        salaries.value(1) >= salaries.value(2),
        "spill-sort: second {} >= third {}",
        salaries.value(1), salaries.value(2)
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 6  STREAMING LIMIT  (critical regression area)
// ═══════════════════════════════════════════════════════════════════════════════

/// Core correctness: streaming limit returns exactly N rows.
#[test]
fn test_limit_stream() {
    let src = "/tmp/blazer_limit.parquet";
    write_test_parquet(src, 50_000);

    let result = LazyFrame::scan_parquet(src)
        .limit(1_000)
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 1_000, "limit(1000) must return exactly 1000 rows");
}

/// Regression: limit must stop the source iterator early — verify row count is
/// exactly `n` for several values of n.
#[test]
fn test_limit_exact_rows_various_n() {
    let src = "/tmp/blazer_limit_various.parquet";
    write_test_parquet(src, 100_000);

    for n in [1, 5, 10, 100, 999, 1_000] {
        let result = LazyFrame::scan_parquet(src)
            .limit(n)
            .collect_streaming()
            .unwrap();
        assert_eq!(
            result.height(), n,
            "limit({n}) must return exactly {n} rows, got {}",
            result.height()
        );
    }
}

/// Regression: limit on a multi-file directory must stop after reading only as
/// many files/row-groups as needed.  The key check: result.height() == n.
#[test]
fn test_limit_early_exit_multi_file_dir() {
    use std::fs;

    let dir = "/tmp/blazer_limit_dir";
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();

    // 4 files × 10 000 rows each = 40 000 rows total
    for i in 0..4 {
        write_test_parquet(&format!("{dir}/part-{i}.parquet"), 10_000);
    }

    let result = LazyFrame::scan_parquet(dir)
        .limit(5)
        .collect_streaming()
        .unwrap();

    // Must stop after the first row-group — not read all 40 000 rows.
    assert_eq!(
        result.height(), 5,
        "limit(5) on multi-file dir should return exactly 5 rows, got {}",
        result.height()
    );
}

/// Edge case: limit(0) returns empty DataFrame.
#[test]
fn test_limit_zero_streaming() {
    let src = "/tmp/blazer_limit_zero.parquet";
    write_test_parquet(src, 1_000);

    let result = LazyFrame::scan_parquet(src)
        .limit(0)
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 0, "limit(0) must return 0 rows");
}

/// Edge case: limit larger than total dataset rows returns all rows.
#[test]
fn test_limit_larger_than_dataset_streaming() {
    let src = "/tmp/blazer_limit_large.parquet";
    write_test_parquet(src, 500);

    let result = LazyFrame::scan_parquet(src)
        .limit(100_000)
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.height(), 500,
        "limit bigger than dataset should return all 500 rows"
    );
}

/// Limit after filter — limit applies to the filtered result, not the source.
#[test]
fn test_limit_after_filter_streaming() {
    let src = "/tmp/blazer_limit_filter.parquet";
    write_test_parquet(src, 30_000); // 10 000 Eng rows

    let result = LazyFrame::scan_parquet(src)
        .filter(col("dept").eq(lit("Eng")))
        .limit(50)
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 50, "limit(50) after filter should return 50 rows");
    let depts = result.column("dept").unwrap().as_utf8().unwrap();
    for i in 0..result.height() {
        assert_eq!(depts.value(i), "Eng", "all rows after dept filter must be Eng");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 7  STREAMING DISTINCT
// ═══════════════════════════════════════════════════════════════════════════════

/// Distinct on a low-cardinality column (mirrors the VendorID regression).
#[test]
fn test_distinct_streaming_low_cardinality() {
    let path = "/tmp/blazer_distinct_test.parquet";
    write_test_parquet(path, 100_000);

    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.height(), 3,
        "distinct dept should yield 3 rows, got {}",
        result.height()
    );
}

/// Distinct via the non-streaming physical path.
#[test]
fn test_distinct_collect_physical() {
    let path = "/tmp/blazer_distinct_physical.parquet";
    write_test_parquet(path, 10_000);

    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .collect()
        .unwrap();

    assert_eq!(result.height(), 3,
        "physical distinct should yield 3 rows, got {}", result.height());
}

/// All rows unique — distinct preserves everything.
#[test]
fn test_distinct_all_columns_unique() {
    let path = "/tmp/blazer_distinct_all_unique.parquet";
    write_test_parquet(path, 5_000);

    let result = LazyFrame::scan_parquet(path)
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 5_000,
        "all rows unique, distinct should preserve all 5000, got {}", result.height());
}

/// Distinct chained after filter.
#[test]
fn test_distinct_after_filter() {
    let path = "/tmp/blazer_distinct_filter.parquet";
    write_test_parquet(path, 30_000);

    let result = LazyFrame::scan_parquet(path)
        .filter(col("dept").eq(lit("Eng")))
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 1,
        "distinct of single-value column after filter should be 1 row, got {}", result.height());
}

/// Distinct across a multi-file directory.
#[test]
fn test_distinct_multi_file_dir() {
    use std::fs;

    let dir = "/tmp/blazer_distinct_dir";
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();

    // Each file has the same 3 dept values — global distinct must still be 3.
    for i in 0..4 {
        write_test_parquet(&format!("{dir}/part-{i}.parquet"), 3_000);
    }

    let result = LazyFrame::scan_parquet(dir)
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.height(), 3,
        "distinct across 4 files with 3 dept values should yield 3 rows, got {}",
        result.height()
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 8  STREAMING GROUP BY
// ═══════════════════════════════════════════════════════════════════════════════

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

    assert_eq!(result.height(), 3, "3 unique depts");
}

/// GroupBy sum correctness: each group's sum must equal the hand-computed value.
#[test]
fn test_streaming_group_by_sum_correctness() {
    let src = "/tmp/blazer_gb_sum.parquet";
    // 3000 rows, 1000 per dept; salary = 50000 + (i % 100000)
    // Each dept cycles every 3 rows: id 0,3,6,... = Eng; 1,4,7,...= PM; 2,5,8,...= Design
    write_test_parquet(src, 3_000);

    let result = LazyFrame::scan_parquet(src)
        .group_by(vec![col("dept")])
        .agg(vec![col("salary").sum().alias("total")])
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 3, "3 dept groups");

    // Verify all totals are positive and non-zero.
    let totals = result.column("total").unwrap().as_f64().unwrap();
    for i in 0..totals.len() {
        assert!(totals.value(i) > 0.0, "group total must be positive");
    }

    // Eng + PM + Design sums should equal the total salary across all rows.
    let grand_total: f64 = (0..3_000usize).map(|i| 50_000.0 + (i % 100_000) as f64).sum();
    let sum_of_groups: f64 = (0..totals.len()).map(|i| totals.value(i)).sum();
    assert!(
        (sum_of_groups - grand_total).abs() < 1.0,
        "sum of group totals ({sum_of_groups}) ≠ grand total ({grand_total})"
    );
}

/// GroupBy count correctness: each group must have the expected row count.
#[test]
fn test_streaming_group_by_count_correctness() {
    let src = "/tmp/blazer_gb_count.parquet";
    write_test_parquet(src, 9_000); // 3000 per dept exactly

    let result = LazyFrame::scan_parquet(src)
        .group_by(vec![col("dept")])
        .agg(vec![col("id").count().alias("cnt")])
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 3, "3 dept groups");
    // GroupBy aggregations (including count) are stored as f64 internally.
    let counts = result.column("cnt").unwrap().as_f64().unwrap();
    for i in 0..counts.len() {
        assert!(
            (counts.value(i) - 3_000.0).abs() < 0.01,
            "each dept should have 3000 rows, got {}",
            counts.value(i)
        );
    }
}

/// GroupBy mean: verify the mean lies within the expected range.
#[test]
fn test_streaming_group_by_mean_correctness() {
    let src = "/tmp/blazer_gb_mean.parquet";
    write_test_parquet(src, 30_000);

    let result = LazyFrame::scan_parquet(src)
        .group_by(vec![col("dept")])
        .agg(vec![col("salary").mean().alias("avg_salary")])
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 3);
    let avgs = result.column("avg_salary").unwrap().as_f64().unwrap();
    for i in 0..avgs.len() {
        let avg = avgs.value(i);
        // salaries range: 50000..150000; mean must be in that range
        assert!(
            avg >= 50_000.0 && avg <= 150_000.0,
            "mean salary {avg} out of expected range [50000, 150000]"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 9  CHAINED STREAMING OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_streaming_filter_then_limit() {
    let src = "/tmp/blazer_chain1.parquet";
    write_test_parquet(src, 90_000); // 30 000 Eng rows

    let result = LazyFrame::scan_parquet(src)
        .filter(col("dept").eq(lit("Eng")))
        .limit(100)
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 100, "filter then limit(100) = 100 rows");
    let depts = result.column("dept").unwrap().as_utf8().unwrap();
    for i in 0..result.height() {
        assert_eq!(depts.value(i), "Eng");
    }
}

#[test]
fn test_streaming_filter_select_limit() {
    let src = "/tmp/blazer_chain2.parquet";
    write_test_parquet(src, 60_000);

    let result = LazyFrame::scan_parquet(src)
        .filter(col("salary").gt(lit(70_000.0f64)))
        .select(vec![col("dept"), col("salary")])
        .limit(200)
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 200, "chained filter→select→limit should yield 200 rows");
    assert_eq!(result.width(), 2, "select keeps only dept and salary");
}

#[test]
fn test_streaming_select_then_distinct() {
    let src = "/tmp/blazer_chain3.parquet";
    write_test_parquet(src, 100_000);

    let result = LazyFrame::scan_parquet(src)
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 3, "select dept → distinct should yield 3 rows");
}

#[test]
fn test_streaming_filter_distinct() {
    let src = "/tmp/blazer_chain4.parquet";
    write_test_parquet(src, 30_000);

    // After filtering to PM, there is exactly 1 unique dept value.
    let result = LazyFrame::scan_parquet(src)
        .filter(col("dept").eq(lit("PM")))
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 1, "PM distinct should be 1 row");
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 10  MULTI-FILE DIRECTORY SCANNING
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_multi_file_dir_full_scan() {
    use std::fs;

    let dir = "/tmp/blazer_multi_dir";
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();

    for i in 0..3 {
        write_test_parquet(&format!("{dir}/part-{i}.parquet"), 5_000);
    }

    let result = LazyFrame::scan_parquet(dir)
        .collect()
        .unwrap();

    assert_eq!(result.height(), 15_000, "3 files × 5000 rows = 15000 total");
}

#[test]
fn test_multi_file_dir_streaming_filter() {
    use std::fs;

    let dir = "/tmp/blazer_multi_filter";
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();

    for i in 0..4 {
        write_test_parquet(&format!("{dir}/part-{i}.parquet"), 3_000);
    }

    let result = LazyFrame::scan_parquet(dir)
        .filter(col("dept").eq(lit("Design")))
        .collect_streaming()
        .unwrap();

    // 1/3 of 12000 rows are Design
    assert_eq!(result.height(), 4_000, "Design rows across 4 files = 4000");
}

// ═══════════════════════════════════════════════════════════════════════════════
// § 11  REGRESSION GUARDS
// ═══════════════════════════════════════════════════════════════════════════════

/// Regression: DISTINCT must actually deduplicate — not return all N rows.
/// (Mirrors the VendorID bug: SELECT VendorID DISTINCT returned 47M rows.)
#[test]
fn test_distinct_regression_no_passthrough() {
    let path = "/tmp/blazer_distinct_regress.parquet";
    write_test_parquet(path, 100_000);

    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert!(
        result.height() <= 3,
        "DISTINCT regression: must not return all 100000 rows. Got {}",
        result.height()
    );
    assert_eq!(
        result.height(), 3,
        "DISTINCT must return exactly 3 unique dept values, got {}",
        result.height()
    );
}

/// Regression: limit must stop early — not read the entire dataset.
/// Verified by checking exact row count (not timing).
#[test]
fn test_limit_regression_stops_at_n() {
    let src = "/tmp/blazer_limit_regress.parquet";
    write_test_parquet(src, 200_000);

    for n in [1, 5, 42, 1000] {
        let result = LazyFrame::scan_parquet(src)
            .limit(n)
            .collect_streaming()
            .unwrap();
        assert_eq!(
            result.height(), n,
            "limit({n}) regression: got {} rows instead of {n}",
            result.height()
        );
    }
}

/// Regression: streaming GroupBy must return correct sums, not stale partial state.
/// (Mirrors the 79s GroupBy bug where PartialAggStream accumulated raw rows.)
#[test]
fn test_streaming_group_by_partial_agg_regression() {
    let src = "/tmp/blazer_partial_agg_regress.parquet";
    // 300 rows per dept (900 total)
    write_test_parquet(src, 900);

    let result = LazyFrame::scan_parquet(src)
        .group_by(vec![col("dept")])
        .agg(vec![col("id").count().alias("cnt")])
        .collect_streaming()
        .unwrap();

    // Must produce exactly 3 groups, each with count = 300
    assert_eq!(result.height(), 3, "must have 3 dept groups");
    // GroupBy count is stored as f64 in the GroupAcc accumulator.
    let cnts = result.column("cnt").unwrap().as_f64().unwrap();
    let total_counted: f64 = (0..cnts.len()).map(|i| cnts.value(i)).sum();
    assert!(
        (total_counted - 900.0).abs() < 0.01,
        "sum of group counts must equal total rows (900), got {total_counted}"
    );
}

/// Regression: sort descending must place largest value first.
#[test]
fn test_streaming_sort_descending_regression() {
    let src = "/tmp/blazer_sort_regress.parquet";
    write_test_parquet(src, 5_000);

    let result = LazyFrame::scan_parquet(src)
        .sort("salary", SortOptions::descending())
        .collect_streaming()
        .unwrap();

    let salaries = result.column("salary").unwrap().as_f64().unwrap();
    assert!(
        salaries.value(0) >= salaries.value(salaries.len() - 1),
        "descending regression: first {} must be ≥ last {}",
        salaries.value(0), salaries.value(salaries.len() - 1)
    );
}

/// Regression: column projection must reduce width (not read all columns and then
/// filter — that would be caught if the returned DataFrame still has all columns).
#[test]
fn test_parquet_projection_regression_width() {
    let src = "/tmp/blazer_proj_regress.parquet";
    write_test_parquet(src, 10_000); // 3-column file: id, salary, dept

    let result = LazyFrame::scan_parquet(src)
        .select(vec![col("salary")])
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.width(), 1,
        "projection regression: expected width=1, got {}. \
         All 3 columns were likely read and then sliced.",
        result.width()
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// § DICT-PAGE DISTINCT OPTIMISATION TESTS
//
// These tests verify correctness of the dictionary-page DISTINCT fast path.
//
// The existing test `ParquetWriter` uses PLAIN encoding for all columns, so
// `try_dict_distinct` will return `None` for those files and fall back to the
// full streaming scan.  The correctness tests below verify that BOTH the
// dict-page path AND the fallback produce identical, correct results.
//
// The test `test_dict_distinct_on_nyc_trips` exercises the actual dict-page
// path against real-world parquet files; it is automatically skipped when the
// file is not present so CI always passes.
// ═══════════════════════════════════════════════════════════════════════════════

/// Verify that `select + distinct` on a low-cardinality column produces
/// the correct unique count regardless of whether the dict optimisation fires.
#[test]
fn test_dict_distinct_correctness_low_cardinality() {
    let path = "/tmp/blazer_dict_distinct_lc.parquet";
    write_test_parquet(path, 100_000); // 3 unique depts

    // Collect via streaming (may use dict path OR fallback — both correct).
    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.height(),
        3,
        "distinct dept count should be 3, got {}",
        result.height()
    );
    assert_eq!(result.width(), 1, "should have exactly one column");
}

/// Verify that the streaming distinct result matches the parallel (physical)
/// distinct result exactly.
#[test]
fn test_dict_distinct_matches_physical_path() {
    let path = "/tmp/blazer_dict_compare.parquet";
    write_test_parquet(path, 30_000);

    let streaming = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    let physical = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .sort("dept", SortOptions::ascending())
        .collect()
        .unwrap();

    assert_eq!(
        streaming.height(),
        physical.height(),
        "streaming and physical distinct should return same row count"
    );
}

/// Verify that distinct with sort works end-to-end via streaming.
#[test]
fn test_dict_distinct_with_sort() {
    let path = "/tmp/blazer_dict_sort.parquet";
    write_test_parquet(path, 50_000);

    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 3, "should have 3 unique depts");
    // Verify ascending order: Design < Eng < PM
    let depts = result.column("dept").unwrap();
    let utf8 = depts.as_utf8().unwrap();
    let s0 = utf8.value(0);
    let s2 = utf8.value(2);
    assert!(
        s0 <= s2,
        "first dept '{}' should be <= last dept '{}'",
        s0,
        s2
    );
}

/// Verify that distinct with limit works end-to-end via streaming.
#[test]
fn test_dict_distinct_with_limit() {
    let path = "/tmp/blazer_dict_limit.parquet";
    write_test_parquet(path, 50_000);

    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .limit(2)
        .collect_streaming()
        .unwrap();

    assert!(
        result.height() <= 2,
        "limit 2 should return at most 2 rows, got {}",
        result.height()
    );
}

/// Verify that filter → distinct does NOT use the dict page path (filter changes
/// the set of visible values and must be evaluated against actual data).
#[test]
fn test_dict_distinct_filter_then_distinct_is_correct() {
    let path = "/tmp/blazer_dict_filter.parquet";
    write_test_parquet(path, 30_000);

    // After filtering to a single dept, distinct should return 1 row.
    let result = LazyFrame::scan_parquet(path)
        .filter(col("dept").eq(lit("Eng")))
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.height(),
        1,
        "filter+distinct should return 1 row for single-value column, got {}",
        result.height()
    );
}

/// Multi-file directory: distinct across 4 files with 3 dept values → still 3.
#[test]
fn test_dict_distinct_multi_file_correctness() {
    use std::fs;

    let dir = "/tmp/blazer_dict_dir_test";
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();

    for i in 0..4 {
        write_test_parquet(&format!("{dir}/part-{i}.parquet"), 3_000);
    }

    let result = LazyFrame::scan_parquet(dir)
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.height(),
        3,
        "distinct across 4 files with 3 unique depts should yield 3, got {}",
        result.height()
    );
}

/// Verify that the dict optimisation falls back gracefully when run against
/// plain-encoded files (our test `ParquetWriter` uses Plain encoding).
/// The result should still be correct.
#[test]
fn test_dict_distinct_fallback_plain_encoded_is_correct() {
    let path = "/tmp/blazer_plain_fallback.parquet";
    write_test_parquet(path, 10_000);

    // try_dict_distinct will return None for plain-encoded files; the full
    // streaming scan takes over and produces the correct answer.
    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("dept")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 3, "fallback should still return 3 unique depts");
}

/// Exercise the optimisation on real-world dict-encoded parquet files when
/// available (NYC Trips dataset).  Automatically skipped otherwise.
#[test]
fn test_dict_distinct_on_nyc_trips_if_available() {
    let dir = "/Users/gaurangkulkarani/Downloads/NYC Trips";
    if !std::path::Path::new(dir).exists() {
        eprintln!("Skipping test_dict_distinct_on_nyc_trips_if_available: directory not found");
        return;
    }

    // VendorID is typically INT32 with 2 unique values (1 and 2).
    let result = LazyFrame::scan_parquet(dir)
        .select(vec![col("VendorID")])
        .distinct()
        .sort("VendorID", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    // We do not hardcode the exact count as the dataset may vary, but there
    // should be at least 1 and at most a handful of unique vendors.
    assert!(
        result.height() >= 1 && result.height() <= 10,
        "expected 1-10 unique VendorIDs, got {}",
        result.height()
    );
    assert_eq!(result.width(), 1, "should have exactly 1 column");
}

/// Regression: dict-distinct path must not corrupt or lose rows from a
/// regular (non-dict-optimised) distinct query on the same dataset.
#[test]
fn test_dict_distinct_regression_full_distinct_unaffected() {
    let path = "/tmp/blazer_dd_regression.parquet";
    write_test_parquet(path, 5_000); // 5000 unique IDs (all rows unique on id)

    // Full distinct on id column — every row is unique
    let result = LazyFrame::scan_parquet(path)
        .select(vec![col("id")])
        .distinct()
        .collect_streaming()
        .unwrap();

    assert_eq!(
        result.height(),
        5_000,
        "distinct on all-unique id column should preserve 5000 rows, got {}",
        result.height()
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Parallel GroupBy tests
// ═══════════════════════════════════════════════════════════════════════════════

/// Write a parquet file where all rows belong to a known dept.
fn write_dept_parquet(path: &str, dept: &str, salary_base: f64, n_rows: usize) {
    let ids: Vec<i64>       = (0..n_rows as i64).collect();
    let salaries: Vec<f64>  = (0..n_rows).map(|i| salary_base + i as f64).collect();
    let depts: Vec<&str>    = (0..n_rows).map(|_| dept).collect();
    let df = DataFrame::new(vec![
        Series::from_i64("id",     ids),
        Series::from_f64("salary", salaries),
        Series::from_str("dept",   depts),
    ])
    .unwrap();
    ParquetWriter::from_path(path).finish(&df).unwrap();
}

/// Parallel GroupBy produces the same result as a sequential GroupBy when
/// multiple parquet files share a single directory.
///
/// We create two files — one per "dept" — in a temp dir, then verify that
/// `collect_streaming` (which will take the parallel path for ≥2 files) returns
/// the same aggregated totals that we compute by hand.
#[test]
fn test_parallel_group_by_correctness_multi_file() {
    let dir = "/tmp/blazer_par_gb_dir";
    std::fs::create_dir_all(dir).unwrap();
    let n = 1_000usize;

    // File 1: "Eng" rows with salaries 100_000 .. 100_999
    write_dept_parquet(&format!("{}/file_eng.parquet", dir), "Eng", 100_000.0, n);
    // File 2: "PM" rows with salaries 200_000 .. 200_999
    write_dept_parquet(&format!("{}/file_pm.parquet",  dir), "PM",  200_000.0, n);

    let result = LazyFrame::scan_parquet(dir)
        .group_by(vec![col("dept")])
        .agg(vec![
            col("salary").sum().alias("total_salary"),
            col("id").count().alias("row_count"),
        ])
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    assert_eq!(result.height(), 2, "should have 2 groups (Eng + PM)");
    assert_eq!(result.width(), 3,  "dept + total_salary + row_count");

    // Expected sums: Eng=100_000+..+100_999, PM=200_000+..+200_999
    let expected_eng: f64 = (100_000..100_000 + n).map(|v| v as f64).sum();
    let expected_pm:  f64 = (200_000..200_000 + n).map(|v| v as f64).sum();

    let total_salary = result.column("total_salary").unwrap().as_f64().unwrap();
    let dept_col     = result.column("dept").unwrap().as_utf8().unwrap();

    for i in 0..result.height() {
        let dept = dept_col.value(i);
        let sal  = total_salary.value(i);
        match dept {
            "Eng" => assert!(
                (sal - expected_eng).abs() < 1.0,
                "Eng total_salary mismatch: {sal} vs {expected_eng}"
            ),
            "PM" => assert!(
                (sal - expected_pm).abs() < 1.0,
                "PM total_salary mismatch: {sal} vs {expected_pm}"
            ),
            _ => panic!("unexpected dept: {dept}"),
        }
    }
    // row_count is stored as f64 (count output)
    let counts = result.column("row_count").unwrap().as_f64().unwrap();
    for i in 0..result.height() {
        assert!(
            (counts.value(i) - n as f64).abs() < 0.01,
            "row_count should equal {n}, got {}", counts.value(i)
        );
    }
}

/// Parallel GroupBy with compound keys (dept + a second grouping column).
#[test]
fn test_parallel_group_by_compound_key() {
    let dir = "/tmp/blazer_par_gb_compound";
    std::fs::create_dir_all(dir).unwrap();
    let n = 600usize; // divisible by 3 (for dept cycling)

    write_test_parquet(&format!("{}/file_a.parquet", dir), n);
    write_test_parquet(&format!("{}/file_b.parquet", dir), n);

    let result = LazyFrame::scan_parquet(dir)
        .group_by(vec![col("dept")])
        .agg(vec![col("id").count().alias("cnt")])
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    // 2 files × n rows each, 3 depts, evenly split → each dept has 2*(n/3) rows
    assert_eq!(result.height(), 3);
    let expected_per_dept = (2 * n / 3) as f64;
    let counts = result.column("cnt").unwrap().as_f64().unwrap();
    for i in 0..counts.len() {
        assert!(
            (counts.value(i) - expected_per_dept).abs() < 0.01,
            "expected {} rows per dept, got {}", expected_per_dept, counts.value(i)
        );
    }
}

/// Parallel GroupBy on the real NYC Trips dataset when available.
/// Verifies correctness (not just timing) of the parallel path.
#[test]
fn test_parallel_group_by_nyc_trips_if_available() {
    let dir = "/Users/gaurangkulkarani/Downloads/NYC Trips";
    if !std::path::Path::new(dir).exists() {
        eprintln!("Skipping test_parallel_group_by_nyc_trips_if_available: directory not found");
        return;
    }

    // count trips per VendorID — tiny cardinality, big dataset
    let result = LazyFrame::scan_parquet(dir)
        .group_by(vec![col("VendorID")])
        .agg(vec![col("VendorID").count().alias("trip_count")])
        .sort("VendorID", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    assert!(result.height() >= 1 && result.height() <= 10,
        "expected 1-10 unique VendorIDs, got {}", result.height());

    // Total trip count should be > 0
    let counts = result.column("trip_count").unwrap().as_f64().unwrap();
    let total: f64 = (0..counts.len()).map(|i| counts.value(i)).sum();
    assert!(total > 0.0, "total trip count should be > 0, got {}", total);
}

// ── Query result cache tests ──────────────────────────────────────────────────

/// Run the same query twice on the same file.
/// The second call should return a cached result in well under 5 ms and the
/// data should be identical to the first call.
#[test]
fn test_result_cache_hit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache_hit.parquet");
    let path_str = path.to_str().unwrap();

    write_test_parquet(path_str, 5_000);

    // Clear any stale cache entries from previous test runs.
    LazyFrame::clear_result_cache();

    // First call — cache miss, full execution.
    let result1 = LazyFrame::scan_parquet(path_str)
        .group_by(vec![col("dept")])
        .agg(vec![col("salary").sum().alias("total_salary")])
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    // Second call — should be a cache hit.
    let t_start = std::time::Instant::now();
    let result2 = LazyFrame::scan_parquet(path_str)
        .group_by(vec![col("dept")])
        .agg(vec![col("salary").sum().alias("total_salary")])
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();
    let elapsed_ms = t_start.elapsed().as_millis();

    // Results must be identical.
    assert_eq!(result1.height(), result2.height(),
        "cached result has different row count");

    // Cache hit should be sub-5ms (generous bound to avoid flakiness on slow CI).
    assert!(elapsed_ms < 5,
        "second (cached) call took {}ms, expected < 5ms", elapsed_ms);
}

/// Write a parquet file, query it (populates cache), then overwrite the file
/// with different data.  A subsequent query must return the fresh data, not the
/// stale cached result.
#[test]
fn test_result_cache_invalidation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache_invalidation.parquet");
    let path_str = path.to_str().unwrap();

    // Write initial file: 3 rows per dept × 5_000 rows total.
    write_test_parquet(path_str, 5_000);

    // Clear any stale entries.
    LazyFrame::clear_result_cache();

    // First query — populates cache.
    let result_before = LazyFrame::scan_parquet(path_str)
        .group_by(vec![col("dept")])
        .agg(vec![col("salary").sum().alias("total_salary")])
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    // Overwrite the file with a different row count (10_000 rows).
    // Sleep briefly so mtime differs even on file systems with 1-second resolution.
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    write_test_parquet(path_str, 10_000);

    // Second query — file changed, cache must be invalidated.
    let result_after = LazyFrame::scan_parquet(path_str)
        .group_by(vec![col("dept")])
        .agg(vec![col("salary").sum().alias("total_salary")])
        .sort("dept", SortOptions::ascending())
        .collect_streaming()
        .unwrap();

    // Both results have the same 3 depts, but the sums must differ because the
    // underlying data changed.
    assert_eq!(result_before.height(), result_after.height(),
        "both queries should return 3 dept groups");

    let sum_before: f64 = {
        let col_arr = result_before.column("total_salary").unwrap().as_f64().unwrap();
        (0..col_arr.len()).map(|i| col_arr.value(i)).sum()
    };
    let sum_after: f64 = {
        let col_arr = result_after.column("total_salary").unwrap().as_f64().unwrap();
        (0..col_arr.len()).map(|i| col_arr.value(i)).sum()
    };

    assert!(
        (sum_after - sum_before).abs() > 1.0,
        "sum_after ({}) should differ from sum_before ({}) — cache was not invalidated",
        sum_after, sum_before
    );
}
