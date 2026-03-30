use blazer_engine::prelude::*;

#[test]
fn test_basic_filter() {
    let df = DataFrame::new(vec![
        Series::from_i64("age", vec![25, 30, 35, 28, 42]),
        Series::from_f64("salary", vec![70000.0, 95000.0, 120000.0, 80000.0, 150000.0]),
        Series::from_str("dept", vec!["Eng", "PM", "Eng", "Design", "Eng"]),
    ])
    .unwrap();

    let result = df
        .lazy()
        .filter(col("age").gt(lit(28i64)))
        .collect()
        .unwrap();

    assert_eq!(result.height(), 3);
}

#[test]
fn test_group_by_sum() {
    let df = DataFrame::new(vec![
        Series::from_str("dept", vec!["Eng", "PM", "Eng", "PM"]),
        Series::from_f64("salary", vec![100000.0, 90000.0, 110000.0, 95000.0]),
    ])
    .unwrap();

    let result = df
        .lazy()
        .group_by(vec![col("dept")])
        .agg(vec![col("salary").sum().alias("total")])
        .sort("total", SortOptions::descending())
        .collect()
        .unwrap();

    assert_eq!(result.height(), 2);
    let totals = result.column("total").unwrap();
    let first = totals.as_f64().unwrap().value(0);
    // Eng: 210000, PM: 185000 — sorted descending, first should be 210000
    assert!((first - 210000.0).abs() < 1.0);
}

#[test]
fn test_inner_join() {
    let left = DataFrame::new(vec![
        Series::from_i64("id", vec![1, 2, 3, 4]),
        Series::from_str("val", vec!["a", "b", "c", "d"]),
    ])
    .unwrap();

    let right = DataFrame::new(vec![
        Series::from_i64("id", vec![2, 3, 5]),
        Series::from_str("label", vec!["x", "y", "z"]),
    ])
    .unwrap();

    let result = left
        .lazy()
        .join(
            right.lazy(),
            vec![col("id")],
            vec![col("id")],
            JoinType::Inner,
        )
        .collect()
        .unwrap();

    assert_eq!(result.height(), 2);
}

#[test]
fn test_rolling_mean() {
    let df = DataFrame::new(vec![Series::from_f64(
        "price",
        vec![1.0, 2.0, 3.0, 4.0, 5.0],
    )])
    .unwrap();

    let result = df
        .lazy()
        .with_columns(vec![col("price").rolling_mean(3).alias("ma3")])
        .collect()
        .unwrap();

    let ma3 = result.column("ma3").unwrap().as_f64().unwrap();
    // First two values should be NaN (< window)
    assert!(ma3.value(0).is_nan());
    assert!(ma3.value(1).is_nan());
    assert!(ma3.value(2).is_finite());
    // mean(3,4,5) = 4.0
    assert!((ma3.value(4) - 4.0).abs() < 0.01);
}

#[test]
fn test_string_ops() {
    let df = DataFrame::new(vec![Series::from_str(
        "name",
        vec!["Alice Smith", "bob jones", "Carol White"],
    )])
    .unwrap();

    let result = df
        .lazy()
        .with_columns(vec![
            col("name").str().to_uppercase().alias("upper"),
            col("name").str().contains("bob").alias("is_bob"),
        ])
        .collect()
        .unwrap();

    let upper = result.column("upper").unwrap().as_utf8().unwrap();
    assert_eq!(upper.value(0), "ALICE SMITH");

    let is_bob = result.column("is_bob").unwrap().as_bool().unwrap();
    assert!(!is_bob.value(0));
    assert!(is_bob.value(1));
    assert!(!is_bob.value(2));
}

#[test]
fn test_lazy_csv_roundtrip() {
    use blazer_engine::io::{CsvReader, CsvWriter};

    let path = "/tmp/blaze_test.csv";
    let df = DataFrame::new(vec![
        Series::from_i64("x", vec![1, 2, 3]),
        Series::from_f64("y", vec![1.1, 2.2, 3.3]),
    ])
    .unwrap();
    CsvWriter::from_path(path).unwrap().finish(&df).unwrap();

    let df2 = CsvReader::from_path(path).unwrap().finish().unwrap();
    assert_eq!(df2.height(), 3);
    assert_eq!(df2.width(), 2);
}

#[test]
fn test_window_functions() {
    let df = DataFrame::new(vec![
        Series::from_str("dept", vec!["Eng", "Eng", "PM", "PM", "Eng"]),
        Series::from_f64("salary", vec![100.0, 200.0, 150.0, 250.0, 300.0]),
    ])
    .unwrap();

    let result = df
        .lazy()
        .with_columns(vec![col("salary")
            .sum()
            .over(vec![col("dept")])
            .alias("dept_total")])
        .collect()
        .unwrap();

    assert_eq!(result.height(), 5);

    // Verify window sums
    let dept_total = result.column("dept_total").unwrap().as_f64().unwrap();
    // Eng rows should have 600.0 (100+200+300), PM rows should have 400.0 (150+250)
    assert!((dept_total.value(0) - 600.0).abs() < 0.01); // Eng
    assert!((dept_total.value(1) - 600.0).abs() < 0.01); // Eng
    assert!((dept_total.value(2) - 400.0).abs() < 0.01); // PM
    assert!((dept_total.value(3) - 400.0).abs() < 0.01); // PM
    assert!((dept_total.value(4) - 600.0).abs() < 0.01); // Eng
}

#[test]
fn test_query_plan_explain() {
    let df = DataFrame::new(vec![Series::from_i64("x", vec![1, 2, 3])]).unwrap();

    let plan = df
        .lazy()
        .filter(col("x").gt(lit(1i64)))
        .select(vec![col("x")])
        .explain(true);

    assert!(plan.contains("DataFrameScan") || plan.contains("Filter"));
    println!("{}", plan);
}
