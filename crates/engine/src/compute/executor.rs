use std::sync::Arc;

use ahash::AHashMap;
use arrow2::array::{BooleanArray, PrimitiveArray, Utf8Array};
use rayon::prelude::*;

use crate::compute::backend::global_backend;
use crate::dataframe::DataFrame;
use crate::dtype::DataType;
use crate::error::{BlazeError, Result};
use crate::expr::{AggFunc, BinaryOp, DatePart, Expr, LitValue, StringOp};
use crate::lazy::LogicalPlan;
use crate::series::Series;

// ── Typed group-by key ────────────────────────────────────────────────────────
//
// Using a typed enum avoids the O(n·k) string allocation of the old approach
// (where every numeric key was formatted to String). Integer and float keys now
// live entirely on the stack and hash with a single word-size comparison.

/// A typed group-by key value for a single column.
/// All variants are `Hash + Eq` so `Vec<GroupKey>` can be used as a HashMap key.
#[derive(Hash, PartialEq, Eq, Clone)]
enum GroupKey {
    Int64(i64),
    Int32(i32),
    Bool(bool),
    /// f64/f32 stored as bit pattern — NaN ≠ NaN semantics are acceptable for
    /// group keys (two NaN values are treated as the same group).
    F64Bits(u64),
    Utf8(String),
    Null,
}

// ── Per-group running accumulator ─────────────────────────────────────────────
//
// Maintained in a single forward pass over the data, replacing the old approach
// of collecting row indices per group and then `take()`-ing them for each agg.

/// Running accumulator for one aggregation expression within one group.
struct GroupAcc {
    func: AggFunc,
    sum: f64,
    count: usize,
    min: f64,
    max: f64,
    first: Option<f64>,
    last: f64,
    /// For `NUnique`: bit-packed hash of every distinct value seen.
    n_bits: ahash::AHashSet<u64>,
}

impl GroupAcc {
    fn new(func: AggFunc) -> Self {
        GroupAcc {
            func,
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: f64::NAN,
            n_bits: ahash::AHashSet::default(),
        }
    }

    #[inline]
    fn update(&mut self, val: f64) {
        self.count += 1;
        match self.func {
            AggFunc::Sum | AggFunc::Mean => self.sum += val,
            AggFunc::Min => {
                if val < self.min {
                    self.min = val;
                }
            }
            AggFunc::Max => {
                if val > self.max {
                    self.max = val;
                }
            }
            AggFunc::Count => {} // count is incremented above
            AggFunc::First => {
                if self.first.is_none() {
                    self.first = Some(val);
                }
            }
            AggFunc::Last => {
                self.last = val;
            }
            AggFunc::NUnique => {
                self.n_bits.insert(val.to_bits());
            }
        }
    }

    fn result(&self) -> f64 {
        match self.func {
            AggFunc::Sum => self.sum,
            AggFunc::Mean => {
                if self.count == 0 {
                    f64::NAN
                } else {
                    self.sum / self.count as f64
                }
            }
            AggFunc::Min => {
                if self.min == f64::INFINITY {
                    f64::NAN
                } else {
                    self.min
                }
            }
            AggFunc::Max => {
                if self.max == f64::NEG_INFINITY {
                    f64::NAN
                } else {
                    self.max
                }
            }
            AggFunc::Count => self.count as f64,
            AggFunc::First => self.first.unwrap_or(f64::NAN),
            AggFunc::Last => self.last,
            AggFunc::NUnique => self.n_bits.len() as f64,
        }
    }

    /// Merge a partial accumulator `other` into `self`.
    ///
    /// Used by the parallel group-by to combine per-thread partial results
    /// into the final global accumulators.  Correctness notes:
    ///
    /// - **Mean**: both `sum` and `count` are accumulated independently so
    ///   `result()` divides the *global* sum by the *global* count → correct.
    /// - **First/Last**: the thread that owns the earliest/latest rows wins.
    ///   Since threads are assigned contiguous row ranges in original order,
    ///   the first non-None `first` and the last non-zero-count `last` are kept.
    /// - **NUnique**: union of the two distinct-value bit sets (hash collision
    ///   risk is negligible in practice).
    #[inline]
    fn merge_owned(&mut self, other: GroupAcc) {
        self.count += other.count;
        match self.func {
            AggFunc::Sum | AggFunc::Mean => {
                self.sum += other.sum;
            }
            AggFunc::Min => {
                if other.min < self.min {
                    self.min = other.min;
                }
            }
            AggFunc::Max => {
                if other.max > self.max {
                    self.max = other.max;
                }
            }
            AggFunc::Count => {} // count already merged above
            AggFunc::First => {
                // Keep the earliest-seen value (self came from a lower row range)
                if self.first.is_none() {
                    self.first = other.first;
                }
            }
            AggFunc::Last => {
                // `other` covers higher row indices → its last value wins
                if other.count > 0 {
                    self.last = other.last;
                }
            }
            AggFunc::NUnique => {
                self.n_bits.extend(other.n_bits);
            }
        }
    }
}

/// Extract the calendar year from a microseconds-since-Unix-epoch value using
/// Howard Hinnant's branchless civil-calendar algorithm.
///
/// No branches in the hot path → the compiler can auto-vectorise this with SIMD.
/// Handles negative timestamps (pre-1970 dates) correctly via Euclidean division.
#[inline(always)]
fn micros_to_year_fast(micros: i64) -> i32 {
    // microseconds → days since epoch (Euclidean div handles negatives correctly)
    let days = micros.div_euclid(86_400_000_000_i64) as i32;
    // Shift to 0000-03-01 epoch (Hinnant: simplifies leap-year arithmetic)
    let z = days + 719_468;
    // 400-year era
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;                         // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i32 + era * 400;
    // Adjust: Hinnant's epoch starts March 1, so Jan/Feb belong to the previous year
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);           // day of year [0, 365]
    let mp = (5 * doy + 2) / 153;                                 // month of year [0, 11]
    if mp < 10 { y } else { y + 1 }
}

/// Physical plan executor: evaluates a LogicalPlan into a DataFrame.
pub struct PhysicalExecutor;

impl PhysicalExecutor {
    pub fn execute(plan: LogicalPlan) -> Result<DataFrame> {
        match plan {
            LogicalPlan::DataFrameScan { df, .. } => Ok(df),

            LogicalPlan::Filter { input, predicate } => {
                let df = Self::execute(*input)?;
                let mask_series = Self::eval_expr(&predicate, &df)?;
                let mask = mask_series.as_bool()?;
                df.filter(&mask.0)
            }

            LogicalPlan::Select { input, exprs } => {
                let df = Self::execute(*input)?;
                let mut columns = Vec::with_capacity(exprs.len());
                for expr in &exprs {
                    let series = Self::eval_expr(expr, &df)?;
                    columns.push(series);
                }
                DataFrame::new(columns)
            }

            LogicalPlan::WithColumns { input, exprs } => {
                let mut df = Self::execute(*input)?;
                for expr in &exprs {
                    let series = Self::eval_expr(expr, &df)?;
                    df = df.with_column(series)?;
                }
                Ok(df)
            }

            LogicalPlan::Sort {
                input,
                by_column,
                options,
            } => {
                let df = Self::execute(*input)?;
                // Use global backend argsort (MLX GPU radix sort when active),
                // then gather every column by the resulting index array.
                let key_col = df.column(&by_column)?;
                let indices = global_backend().argsort(key_col, options.descending)?;
                let sorted_columns: Result<Vec<Series>> =
                    df.columns().iter().map(|c| c.take(&indices)).collect();
                DataFrame::new(sorted_columns?)
            }

            LogicalPlan::GroupBy {
                input,
                keys,
                aggs,
            } => {
                let df = Self::execute(*input)?;
                Self::execute_group_by(&df, &keys, &aggs)
            }

            LogicalPlan::Join {
                left,
                right,
                left_on,
                right_on,
                join_type,
            } => {
                let left_df = Self::execute(*left)?;
                let right_df = Self::execute(*right)?;
                Self::execute_join(&left_df, &right_df, &left_on, &right_on, join_type)
            }

            LogicalPlan::Limit { input, n } => {
                let df = Self::execute(*input)?;
                Ok(df.head(n))
            }

            LogicalPlan::Distinct { input } => {
                let df = Self::execute(*input)?;
                Self::execute_distinct(&df)
            }

            LogicalPlan::DatasetScan {
                root, format, projection, n_rows, row_filters, ..
            } => {
                let ext = match format {
                    crate::dataset::FileFormat::Parquet => "parquet",
                    crate::dataset::FileFormat::Csv => "csv",
                    crate::dataset::FileFormat::Json => "json",
                };

                let root_path = std::path::Path::new(&root);
                let files = if root_path.is_file() {
                    vec![root_path.to_path_buf()]
                } else {
                    crate::dataset::collect_files(root_path, ext)
                };

                if files.is_empty() {
                    return Ok(DataFrame::empty());
                }

                // Extract zone-map predicates from the pushed-down row_filters expression.
                // These are used to skip whole parquet row groups via min/max statistics.
                let rg_preds: Vec<crate::io::RgPredicate> = row_filters
                    .as_deref()
                    .map(|e| Self::extract_rg_predicates(e))
                    .unwrap_or_default();

                // ── Strategy ─────────────────────────────────────────────────
                // Parallel: multiple files AND (no row limit OR large limit).
                //   → all files read simultaneously on the Rayon thread pool.
                //   → order is preserved (rayon par_iter + collect keeps index order).
                //
                // Sequential: single file OR small limit (limit pushdown already
                //   makes small limits fast; parallel overhead isn't worth it).
                // ─────────────────────────────────────────────────────────────
                let use_parallel = files.len() > 1
                    && n_rows.map_or(true, |n| n > 50_000);

                let result = if use_parallel {
                    // Read every file on a separate Rayon worker thread
                    let per_file_dfs: Vec<Result<DataFrame>> = files
                        .par_iter()
                        .map(|file| {
                            let mut reader = crate::io::ParquetReader::from_path(file)?;
                            if let Some(ref proj) = projection {
                                reader = reader.with_projection(proj.clone());
                            }
                            // Give each file a budget of n_rows so we don't
                            // over-read; after vstacking we truncate precisely.
                            if let Some(n) = n_rows {
                                reader = reader.with_n_rows(n);
                            }
                            if !rg_preds.is_empty() {
                                reader = reader.with_row_group_predicates(rg_preds.clone());
                            }
                            reader.finish()
                        })
                        .collect();

                    // Vstack in original file order (deterministic output)
                    let mut acc: Option<DataFrame> = None;
                    for df_result in per_file_dfs {
                        let df = df_result?;
                        if df.height() == 0 {
                            continue;
                        }
                        acc = Some(match acc {
                            None => df,
                            Some(prev) => prev.vstack(&df)?,
                        });
                    }
                    acc.unwrap_or_else(DataFrame::empty)
                } else {
                    // Sequential scan — stops reading as soon as n_rows is met
                    let mut acc: Option<DataFrame> = None;
                    for file in &files {
                        if let Some(n) = n_rows {
                            if acc.as_ref().map_or(0, |df| df.height()) >= n {
                                break;
                            }
                        }
                        let remaining = n_rows.map(|n| {
                            n.saturating_sub(acc.as_ref().map_or(0, |df| df.height()))
                        });
                        let mut reader = crate::io::ParquetReader::from_path(file)?;
                        if let Some(ref proj) = projection {
                            reader = reader.with_projection(proj.clone());
                        }
                        if let Some(r) = remaining {
                            reader = reader.with_n_rows(r);
                        }
                        if !rg_preds.is_empty() {
                            reader = reader.with_row_group_predicates(rg_preds.clone());
                        }
                        let df = reader.finish()?;
                        acc = Some(match acc {
                            None => df,
                            Some(prev) => prev.vstack(&df)?,
                        });
                    }
                    acc.unwrap_or_else(DataFrame::empty)
                };

                // Final precise truncation (row groups are coarser than n_rows)
                if let Some(n) = n_rows {
                    Ok(result.head(n))
                } else {
                    Ok(result)
                }
            }
        }
    }

    /// Evaluate an expression against a DataFrame, producing a Series.
    pub fn eval_expr(expr: &Expr, df: &DataFrame) -> Result<Series> {
        match expr {
            Expr::Column(name) => {
                let col = df.column(name)?;
                Ok(col.clone())
            }

            Expr::Literal(lit) => {
                let len = df.height().max(1);
                match lit {
                    LitValue::Int64(v) => Ok(Series::new_scalar_i64("literal", *v, len)),
                    LitValue::Float64(v) => Ok(Series::new_scalar_f64("literal", *v, len)),
                    LitValue::Utf8(v) => Ok(Series::new_scalar_str("literal", v, len)),
                    LitValue::Boolean(v) => Ok(Series::new_scalar_bool("literal", *v, len)),
                    LitValue::Null => {
                        Ok(Series::from_opt_i64("literal", vec![None; len]))
                    }
                }
            }

            Expr::BinaryExpr { left, op, right } => {
                let left_series = Self::eval_expr(left, df)?;
                let right_series = Self::eval_expr(right, df)?;
                Self::eval_binary(&left_series, *op, &right_series)
            }

            Expr::Agg { input, func } => {
                let series = Self::eval_expr(input, df)?;
                Self::eval_agg(&series, *func)
            }

            Expr::Alias { expr, name } => {
                let mut series = Self::eval_expr(expr, df)?;
                series.rename(name);
                Ok(series)
            }

            Expr::Sort { expr, options } => {
                let series = Self::eval_expr(expr, df)?;
                // Route through the global backend so MLX/GPU sort is used when active.
                global_backend().sort(&series, options.descending)
            }

            Expr::StringExpr { input, op } => {
                let series = Self::eval_expr(input, df)?;
                match op {
                    StringOp::ToUppercase  => series.str_to_uppercase(),
                    StringOp::ToLowercase  => series.str_to_lowercase(),
                    StringOp::Contains(p)  => series.str_contains(p),
                    StringOp::StartsWith(p) => series.str_starts_with(p),
                    StringOp::EndsWith(p)   => series.str_ends_with(p),
                    StringOp::Len          => Err(BlazeError::InvalidOperation(
                        "str.len() not yet implemented".into(),
                    )),
                    StringOp::Replace(_, _) => Err(BlazeError::InvalidOperation(
                        "str.replace() not yet implemented".into(),
                    )),
                }
            }

            Expr::Rolling {
                input,
                func,
                window_size,
            } => {
                let series = Self::eval_expr(input, df)?;
                match func {
                    AggFunc::Mean => series.rolling_mean(*window_size),
                    AggFunc::Sum  => series.rolling_sum(*window_size),
                    _ => Err(BlazeError::InvalidOperation(
                        "Unsupported rolling function".into(),
                    )),
                }
            }

            Expr::Window {
                input,
                partition_by,
            } => {
                Self::eval_window(input, partition_by, df)
            }

            Expr::Not(e) => {
                let series = Self::eval_expr(e, df)?;
                let mask = series.as_bool()?;
                let result = arrow2::compute::boolean::not(&mask.0);
                Series::from_arrow(series.name(), Arc::new(result))
            }

            Expr::IsNull(e) => {
                let series = Self::eval_expr(e, df)?;
                let arr = series.to_array();
                let validity = arr.validity();
                let result = match validity {
                    Some(bitmap) => {
                        let values: Vec<bool> = (0..arr.len())
                            .map(|i| !bitmap.get_bit(i))
                            .collect();
                        BooleanArray::from_slice(values)
                    }
                    None => BooleanArray::from_slice(vec![false; arr.len()]),
                };
                Ok(Series::from_arrow(series.name(), Arc::new(result))?)
            }

            Expr::IsNotNull(e) => {
                let series = Self::eval_expr(e, df)?;
                let arr = series.to_array();
                let validity = arr.validity();
                let result = match validity {
                    Some(bitmap) => {
                        let values: Vec<bool> = (0..arr.len())
                            .map(|i| bitmap.get_bit(i))
                            .collect();
                        BooleanArray::from_slice(values)
                    }
                    None => BooleanArray::from_slice(vec![true; arr.len()]),
                };
                Ok(Series::from_arrow(series.name(), Arc::new(result))?)
            }

            Expr::Wildcard => {
                Err(BlazeError::InvalidOperation(
                    "Cannot evaluate wildcard expression directly".into(),
                ))
            }

            Expr::Cast { expr, dtype } => {
                let series = Self::eval_expr(expr, df)?;
                let arr = series.to_array();
                let target = dtype.to_arrow();
                let casted = arrow2::compute::cast::cast(arr.as_ref(), &target, Default::default())?;
                Series::from_arrow(series.name(), casted.into())
            }

            Expr::DateExpr { input, part } => {
                let series = Self::eval_expr(input, df)?;

                // ── Utf8 fast-path ──────────────────────────────────────────
                // Parquet files often store datetime columns as plain strings
                // (e.g. "2019-01-01 00:26:02") instead of typed Timestamps.
                // arrow2 0.18 doesn't implement Utf8→Timestamp cast, so we
                // parse date parts positionally from "YYYY-MM-DD HH:MM:SS".
                if *series.dtype() == DataType::Utf8 {
                    let arr = series.to_array();
                    let utf8 = arr
                        .as_any()
                        .downcast_ref::<arrow2::array::Utf8Array<i32>>()
                        .ok_or_else(|| BlazeError::ComputeError(
                            format!("Expected Utf8 array for '{}'", series.name())
                        ))?;

                    // Check nulls via the validity bitmap (avoids needing Array trait in scope)
                    let validity = utf8.validity();
                    let values: Vec<Option<i32>> = (0..utf8.len()).map(|i| {
                        if validity.map_or(false, |v| !v.get_bit(i)) { return None; }
                        let s = utf8.value(i);
                        match part {
                            // Positional slices into "YYYY-MM-DD HH:MM:SS"
                            DatePart::Year    => s.get(0..4) .and_then(|v| v.parse().ok()),
                            DatePart::Month   => s.get(5..7) .and_then(|v| v.parse().ok()),
                            DatePart::Day     => s.get(8..10).and_then(|v| v.parse().ok()),
                            DatePart::Hour    => s.get(11..13).and_then(|v| v.parse().ok()),
                            DatePart::Minute  => s.get(14..16).and_then(|v| v.parse().ok()),
                            DatePart::Second  => s.get(17..19).and_then(|v| v.parse().ok()),
                            DatePart::Weekday => {
                                // Sakamoto's algorithm → 0 = Sunday … 6 = Saturday
                                let y: i32 = s.get(0..4) .and_then(|v| v.parse().ok())?;
                                let m: i32 = s.get(5..7) .and_then(|v| v.parse().ok())?;
                                let d: i32 = s.get(8..10).and_then(|v| v.parse().ok())?;
                                const T: [i32; 12] = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
                                let y = if m < 3 { y - 1 } else { y };
                                Some((y + y/4 - y/100 + y/400 + T[(m-1) as usize] + d).rem_euclid(7))
                            }
                        }
                    }).collect();

                    let result = Arc::new(arrow2::array::Int32Array::from(values))
                        as Arc<dyn arrow2::array::Array>;
                    return Series::from_arrow(series.name(), result.into());
                }

                let arr = series.to_array();

                // ── Parallel branchless year extraction ────────────────────
                // For large Timestamp / Int64 columns, extract year in parallel
                // using Howard Hinnant's branchless civil-calendar algorithm.
                // This avoids the serial arrow2::temporal::year() call and gives
                // full CPU utilisation for multi-million-row GroupBy key columns.
                let already_in_rayon = rayon::current_thread_index().is_some();
                if !already_in_rayon && *part == DatePart::Year && series.len() > 50_000 {
                    let arrow_dt = arr.data_type();
                    match arrow_dt {
                        arrow2::datatypes::DataType::Timestamp(
                            arrow2::datatypes::TimeUnit::Microsecond, _
                        ) | arrow2::datatypes::DataType::Int64 => {
                            // Downcast to i64 primitive array.
                            if let Some(prim) = arr
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                            {
                                let values = prim.values().as_slice();
                                let years: Vec<i32> = values
                                    .par_chunks(8192)
                                    .flat_map(|chunk| {
                                        chunk.iter().map(|&v| micros_to_year_fast(v)).collect::<Vec<_>>()
                                    })
                                    .collect();
                                let result_arr: Arc<dyn arrow2::array::Array> =
                                    Arc::new(arrow2::array::Int32Array::from_vec(years));
                                return Series::from_arrow(series.name(), result_arr);
                            }
                        }
                        arrow2::datatypes::DataType::Timestamp(
                            arrow2::datatypes::TimeUnit::Nanosecond, _
                        ) => {
                            if let Some(prim) = arr
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                            {
                                let values = prim.values().as_slice();
                                let years: Vec<i32> = values
                                    .par_chunks(8192)
                                    .flat_map(|chunk| {
                                        chunk.iter().map(|&v| micros_to_year_fast(v / 1_000)).collect::<Vec<_>>()
                                    })
                                    .collect();
                                let result_arr: Arc<dyn arrow2::array::Array> =
                                    Arc::new(arrow2::array::Int32Array::from_vec(years));
                                return Series::from_arrow(series.name(), result_arr);
                            }
                        }
                        _ => {}
                    }
                } else if *part == DatePart::Year && series.len() > 50_000 {
                    // Sequential fallback when already inside a Rayon worker thread.
                    let arrow_dt = arr.data_type();
                    match arrow_dt {
                        arrow2::datatypes::DataType::Timestamp(
                            arrow2::datatypes::TimeUnit::Microsecond, _
                        ) | arrow2::datatypes::DataType::Int64 => {
                            if let Some(prim) = arr
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                            {
                                let values = prim.values().as_slice();
                                let years: Vec<i32> = values.iter().map(|&v| micros_to_year_fast(v)).collect();
                                let result_arr: Arc<dyn arrow2::array::Array> =
                                    Arc::new(arrow2::array::Int32Array::from_vec(years));
                                return Series::from_arrow(series.name(), result_arr);
                            }
                        }
                        arrow2::datatypes::DataType::Timestamp(
                            arrow2::datatypes::TimeUnit::Nanosecond, _
                        ) => {
                            if let Some(prim) = arr
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                            {
                                let values = prim.values().as_slice();
                                let years: Vec<i32> = values.iter().map(|&v| micros_to_year_fast(v / 1_000)).collect();
                                let result_arr: Arc<dyn arrow2::array::Array> =
                                    Arc::new(arrow2::array::Int32Array::from_vec(years));
                                return Series::from_arrow(series.name(), result_arr);
                            }
                        }
                        _ => {}
                    }
                }

                use arrow2::compute::temporal as t;
                // All temporal functions return PrimitiveArray<i32> or u32 wrapped
                // as a Box<dyn Array>.  We normalise everything to Int32 Series.
                let result: Arc<dyn arrow2::array::Array> = match part {
                    DatePart::Year    => Arc::new(t::year(arr.as_ref())?),
                    DatePart::Month   => Arc::new(t::month(arr.as_ref())?),
                    DatePart::Day     => Arc::new(t::day(arr.as_ref())?),
                    DatePart::Hour    => Arc::new(t::hour(arr.as_ref())?),
                    DatePart::Minute  => Arc::new(t::minute(arr.as_ref())?),
                    DatePart::Second  => Arc::new(t::second(arr.as_ref())?),
                    DatePart::Weekday => Arc::new(t::weekday(arr.as_ref())?),
                };
                Series::from_arrow(series.name(), result)
            }
        }
    }

    /// Evaluate binary operations using the global backend.
    fn eval_binary(left: &Series, op: BinaryOp, right: &Series) -> Result<Series> {
        let backend = global_backend();
        match op {
            BinaryOp::Add => backend.add(left, right),
            BinaryOp::Sub => backend.sub(left, right),
            BinaryOp::Mul => backend.mul(left, right),
            BinaryOp::Div => backend.div(left, right),
            BinaryOp::Mod => backend.modulo(left, right),
            BinaryOp::Eq => backend.eq_series(left, right),
            BinaryOp::NotEq => backend.neq_series(left, right),
            BinaryOp::Lt => backend.lt_series(left, right),
            BinaryOp::LtEq => backend.lte_series(left, right),
            BinaryOp::Gt => backend.gt_series(left, right),
            BinaryOp::GtEq => backend.gte_series(left, right),
            BinaryOp::And => backend.and_series(left, right),
            BinaryOp::Or => backend.or_series(left, right),
        }
    }

    /// Evaluate an aggregation function, returning a single-element Series.
    ///
    /// Sum / Mean / Min / Max are routed through the global compute backend so
    /// that MLX (or any future GPU backend) handles the reduction when active.
    /// Count / First / Last / NUnique remain CPU-side because they have no
    /// counterpart in the `ComputeBackend` trait (they're cheap or non-numeric).
    fn eval_agg(series: &Series, func: AggFunc) -> Result<Series> {
        let name = series.name().to_string();
        let backend = global_backend();
        match func {
            AggFunc::Sum => {
                let v = backend.sum(series)?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Mean => {
                let v = backend.mean(series)?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Min => {
                let v = backend.min(series)?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Max => {
                let v = backend.max(series)?;
                Ok(Series::from_f64(&name, vec![v]))
            }
            AggFunc::Count => {
                Ok(Series::from_i64(&name, vec![series.count() as i64]))
            }
            AggFunc::NUnique => {
                let mut seen = std::collections::HashSet::new();
                let arr = series.to_array();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        seen.insert(Self::series_value_to_string(series, i));
                    }
                }
                Ok(Series::from_i64(&name, vec![seen.len() as i64]))
            }
            AggFunc::First => {
                if series.is_empty() {
                    return Err(BlazeError::InvalidOperation("Empty series".into()));
                }
                Ok(series.slice(0, 1))
            }
            AggFunc::Last => {
                if series.is_empty() {
                    return Err(BlazeError::InvalidOperation("Empty series".into()));
                }
                Ok(series.slice(series.len() - 1, 1))
            }
        }
    }

    /// Dispatcher: choose the best group-by strategy based on backend and data size.
    ///
    /// Priority:
    ///   1. MLX backend + ≥ 50 000 rows → `execute_group_by_mlx` (GPU radix sort
    ///      + contiguous CPU reduction).
    ///   2. ≥ 50 000 rows + ≥ 2 Rayon threads → `execute_group_by_parallel`
    ///      (two-phase CPU hash-map).
    ///   3. Otherwise → `execute_group_by_single` (single-pass CPU hash-map).
    fn execute_group_by(
        df: &DataFrame,
        keys: &[Expr],
        aggs: &[Expr],
    ) -> Result<DataFrame> {
        #[cfg(all(target_os = "macos", feature = "mlx"))]
        if global_backend().name() == "mlx" && df.height() >= 50_000 {
            return Self::execute_group_by_mlx(df, keys, aggs);
        }

        let n_threads = rayon::current_num_threads();
        let already_in_rayon = rayon::current_thread_index().is_some();
        if !already_in_rayon && df.height() >= 50_000 && n_threads >= 2 {
            Self::execute_group_by_parallel(df, keys, aggs)
        } else {
            Self::execute_group_by_single(df, keys, aggs)
        }
    }

    /// MLX-accelerated Sort-Reduce group-by.
    ///
    /// # Strategy
    ///
    /// Traditional hash group-by allocates one `HashMap` entry per unique group
    /// and touches each row in an unpredictable order — hard to vectorise.
    ///
    /// Sort-Reduce replaces hashing with **GPU radix sort** (MLX sort on Apple
    /// Silicon runs on the ANE / GPU and is 3–8× faster than CPU for large
    /// arrays):
    ///
    /// 1. **MLX argsort** the first key column → a row-index permutation that
    ///    places all rows of the same group contiguously.
    /// 2. **CPU gather** all key and agg-source columns using those indices
    ///    (Arrow2 `take`, ~O(n) memcpy equivalent).
    /// 3. **CPU boundary scan** (one linear pass) to identify group start/end.
    /// 4. **CPU slice-reduce** per group using `GroupAcc` — now every group's
    ///    rows are contiguous so cache locality is perfect.
    ///
    /// For compound keys (multiple group-by columns) only the first key drives
    /// the GPU sort; ties within the first key are broken by the subsequent
    /// CPU boundary scan (which checks all key columns for equality).
    ///
    /// This is only compiled/used on macOS with the `mlx` feature.
    #[cfg(all(target_os = "macos", feature = "mlx"))]
    fn execute_group_by_mlx(
        df: &DataFrame,
        keys: &[Expr],
        aggs: &[Expr],
    ) -> Result<DataFrame> {
        // ── 1. Evaluate expressions ──────────────────────────────────────────
        let key_series: Vec<Series> = keys
            .iter()
            .map(|k| Self::eval_expr(k, df))
            .collect::<Result<Vec<_>>>()?;

        let agg_info: Vec<(Expr, AggFunc, Option<String>)> = aggs
            .iter()
            .map(|e| Self::extract_agg(e))
            .collect::<Result<Vec<_>>>()?;

        let agg_sources: Vec<Series> = agg_info
            .iter()
            .map(|(input_expr, _, _)| Self::eval_expr(input_expr, df))
            .collect::<Result<Vec<_>>>()?;

        let agg_funcs: Vec<AggFunc> = agg_info.iter().map(|(_, f, _)| *f).collect();

        // ── 2. GPU radix sort: argsort on first key column via MLX ───────────
        // For compound keys we sort on the first key; the boundary scan below
        // uses all key columns for equality, so correctness is preserved.
        let sort_indices = global_backend().argsort(&key_series[0], false)?;

        // ── 3. CPU gather: reorder key and agg-source columns ───────────────
        let sorted_keys: Vec<Series> = key_series
            .iter()
            .map(|s| s.take(&sort_indices))
            .collect::<Result<Vec<_>>>()?;

        let sorted_srcs: Vec<Series> = agg_sources
            .iter()
            .map(|s| s.take(&sort_indices))
            .collect::<Result<Vec<_>>>()?;

        // Pre-extract arrays for O(1) per-row access.
        let key_arrays: Vec<Arc<dyn arrow2::array::Array>> =
            sorted_keys.iter().map(|s| s.to_array()).collect();
        let src_arrays: Vec<Arc<dyn arrow2::array::Array>> =
            sorted_srcs.iter().map(|s| s.to_array()).collect();
        let key_dtypes: Vec<DataType> = sorted_keys.iter().map(|s| s.dtype().clone()).collect();

        // ── 4. CPU boundary scan + slice-reduce ──────────────────────────────
        let n = df.height();
        let mut group_keys: Vec<Vec<GroupKey>> = Vec::new();
        let mut group_accs: Vec<Vec<GroupAcc>> = Vec::new();

        // Emit a new group whenever the key tuple changes.
        let mut group_start = 0usize;
        for row in 0..=n {
            // Detect group boundary: either end-of-data or key change.
            let boundary = row == n || (row > 0 && {
                let cur: Vec<GroupKey> = key_arrays
                    .iter()
                    .zip(key_dtypes.iter())
                    .map(|(arr, dt)| Self::group_key_at(arr.as_ref(), dt, row))
                    .collect();
                let prev: Vec<GroupKey> = key_arrays
                    .iter()
                    .zip(key_dtypes.iter())
                    .map(|(arr, dt)| Self::group_key_at(arr.as_ref(), dt, row - 1))
                    .collect();
                cur != prev
            });

            if boundary && row > 0 {
                // Flush group [group_start, row).
                let key: Vec<GroupKey> = key_arrays
                    .iter()
                    .zip(key_dtypes.iter())
                    .map(|(arr, dt)| Self::group_key_at(arr.as_ref(), dt, group_start))
                    .collect();

                let mut accs: Vec<GroupAcc> =
                    agg_funcs.iter().map(|&f| GroupAcc::new(f)).collect();

                for row_idx in group_start..row {
                    for (acc, arr) in accs.iter_mut().zip(src_arrays.iter()) {
                        if arr.is_null(row_idx) {
                            continue;
                        }
                        if let Some(v) = Self::arr_val_f64(arr.as_ref(), row_idx) {
                            acc.update(v);
                        } else if let Some(utf8) =
                            arr.as_any().downcast_ref::<Utf8Array<i32>>()
                        {
                            match acc.func {
                                AggFunc::Count => acc.count += 1,
                                AggFunc::NUnique => {
                                    use std::hash::{Hash, Hasher};
                                    let mut h = ahash::AHasher::default();
                                    utf8.value(row_idx).hash(&mut h);
                                    acc.n_bits.insert(h.finish());
                                    acc.count += 1;
                                }
                                _ => acc.count += 1,
                            }
                        }
                    }
                }

                group_keys.push(key);
                group_accs.push(accs);
                group_start = row;
            }
        }

        // ── 5. Build result DataFrame ─────────────────────────────────────────
        let g_count = group_keys.len();
        let mut result_columns: Vec<Series> =
            Vec::with_capacity(key_series.len() + agg_sources.len());

        for (ki, ks) in key_series.iter().enumerate() {
            let col = Self::build_key_column(ks.name(), ks.dtype(), &group_keys, ki)?;
            result_columns.push(col);
        }

        for (ai, (_, _, alias)) in agg_info.iter().enumerate() {
            let col_name = alias
                .clone()
                .unwrap_or_else(|| agg_sources[ai].name().to_string());
            let vals: Vec<f64> = (0..g_count).map(|g| group_accs[g][ai].result()).collect();
            result_columns.push(Series::from_f64(&col_name, vals));
        }

        DataFrame::new(result_columns)
    }

    /// Two-phase parallel group-by.
    ///
    /// **Phase 1 (parallel)**: split rows into `n_threads` contiguous chunks;
    /// each Rayon worker builds an independent `(group_keys, group_accs)` map
    /// for its slice — zero inter-thread communication.
    ///
    /// **Phase 2 (sequential)**: merge all partial maps into a single final
    /// map using `GroupAcc::merge_owned`.  Sequential merge is fast because
    /// it only iterates over *unique groups*, not rows.
    fn execute_group_by_parallel(
        df: &DataFrame,
        keys: &[Expr],
        aggs: &[Expr],
    ) -> Result<DataFrame> {
        // ── 1. Evaluate all expressions ──────────────────────────────────────
        let key_series: Vec<Series> = keys
            .iter()
            .map(|k| Self::eval_expr(k, df))
            .collect::<Result<Vec<_>>>()?;

        let agg_info: Vec<(Expr, AggFunc, Option<String>)> = aggs
            .iter()
            .map(|e| Self::extract_agg(e))
            .collect::<Result<Vec<_>>>()?;

        let agg_sources: Vec<Series> = agg_info
            .iter()
            .map(|(input_expr, _, _)| Self::eval_expr(input_expr, df))
            .collect::<Result<Vec<_>>>()?;

        let agg_funcs: Vec<AggFunc> = agg_info.iter().map(|(_, f, _)| *f).collect();

        // ── 2. Pre-extract arrays (Arc-shared across threads, zero-copy) ──────
        let key_arrays: Vec<Arc<dyn arrow2::array::Array>> =
            key_series.iter().map(|s| s.to_array()).collect();
        let src_arrays: Vec<Arc<dyn arrow2::array::Array>> =
            agg_sources.iter().map(|s| s.to_array()).collect();

        // Cheap clone of enum-based DataType so threads don't borrow key_series
        let key_dtypes: Vec<DataType> = key_series.iter().map(|s| s.dtype().clone()).collect();

        // ── 3. Partition row index space ──────────────────────────────────────
        let n = df.height();
        let n_threads = rayon::current_num_threads().max(1);
        let chunk_size = (n + n_threads - 1) / n_threads;

        // ── 4. Parallel partial aggregation ──────────────────────────────────
        let partial_results: Vec<Result<(Vec<Vec<GroupKey>>, Vec<Vec<GroupAcc>>)>> =
            (0..n_threads)
                .into_par_iter()
                .map(|t| {
                    let start = t * chunk_size;
                    if start >= n {
                        return Ok((Vec::new(), Vec::new()));
                    }
                    let end = n.min(start + chunk_size);

                    let mut local_keys: Vec<Vec<GroupKey>> = Vec::new();
                    let mut local_index: AHashMap<Vec<GroupKey>, usize> = AHashMap::new();
                    let mut local_accs: Vec<Vec<GroupAcc>> = Vec::new();

                    for row_idx in start..end {
                        // Typed key — no string allocation for numeric types
                        let key: Vec<GroupKey> = key_arrays
                            .iter()
                            .zip(key_dtypes.iter())
                            .map(|(arr, dt)| Self::group_key_at(arr.as_ref(), dt, row_idx))
                            .collect();

                        let grp = if let Some(&idx) = local_index.get(&key) {
                            idx
                        } else {
                            let idx = local_keys.len();
                            local_keys.push(key.clone());
                            local_index.insert(key, idx);
                            local_accs
                                .push(agg_funcs.iter().map(|&f| GroupAcc::new(f)).collect());
                            idx
                        };

                        let accs = &mut local_accs[grp];
                        for (acc, arr) in accs.iter_mut().zip(src_arrays.iter()) {
                            if arr.is_null(row_idx) {
                                continue;
                            }
                            if let Some(v) = Self::arr_val_f64(arr.as_ref(), row_idx) {
                                acc.update(v);
                            } else if let Some(utf8) =
                                arr.as_any().downcast_ref::<Utf8Array<i32>>()
                            {
                                match acc.func {
                                    AggFunc::Count => acc.count += 1,
                                    AggFunc::NUnique => {
                                        use std::hash::{Hash, Hasher};
                                        let mut h = ahash::AHasher::default();
                                        utf8.value(row_idx).hash(&mut h);
                                        acc.n_bits.insert(h.finish());
                                        acc.count += 1;
                                    }
                                    _ => {
                                        acc.count += 1;
                                    }
                                }
                            }
                        }
                    }

                    Ok((local_keys, local_accs))
                })
                .collect();

        // ── 5. Sequential merge ───────────────────────────────────────────────
        let mut final_keys: Vec<Vec<GroupKey>> = Vec::new();
        let mut final_index: AHashMap<Vec<GroupKey>, usize> = AHashMap::new();
        let mut final_accs: Vec<Vec<GroupAcc>> = Vec::new();

        for partial in partial_results {
            let (g_keys, g_accs) = partial?;
            for (key, accs) in g_keys.into_iter().zip(g_accs.into_iter()) {
                if let Some(&idx) = final_index.get(&key) {
                    for (fa, pa) in final_accs[idx].iter_mut().zip(accs.into_iter()) {
                        fa.merge_owned(pa);
                    }
                } else {
                    let idx = final_keys.len();
                    final_keys.push(key.clone());
                    final_index.insert(key, idx);
                    final_accs.push(accs);
                }
            }
        }

        // ── 6. Build result DataFrame ─────────────────────────────────────────
        let g_count = final_keys.len();
        let mut result_columns: Vec<Series> =
            Vec::with_capacity(key_series.len() + agg_sources.len());

        for (ki, ks) in key_series.iter().enumerate() {
            let col = Self::build_key_column(ks.name(), ks.dtype(), &final_keys, ki)?;
            result_columns.push(col);
        }

        for (ai, (_, _, alias)) in agg_info.iter().enumerate() {
            let col_name = alias
                .clone()
                .unwrap_or_else(|| agg_sources[ai].name().to_string());
            let vals: Vec<f64> = (0..g_count).map(|g| final_accs[g][ai].result()).collect();
            result_columns.push(Series::from_f64(&col_name, vals));
        }

        DataFrame::new(result_columns)
    }

    /// Single-threaded group-by using a single-pass accumulator strategy.
    ///
    /// ### What changed from the old implementation
    ///
    /// **Old**: per-row string conversion → `HashMap<Vec<String>, Vec<usize>>`
    ///          → per-group `take()` + re-aggregate.
    ///
    /// **New**: typed `GroupKey` enum (no string alloc for Int/Float/Bool) →
    ///          `AHashMap<Vec<GroupKey>, usize>` index into insertion-ordered
    ///          `group_accs` → running accumulator updated every row → result
    ///          built from accumulators at the end.
    ///
    /// Avoids:
    /// - O(n × k) string allocations for numeric key columns
    /// - O(g) `take()` calls (one copy per group per agg column)
    /// - std HashMap (replaced by AHashMap for ~20 % faster hashing)
    fn execute_group_by_single(
        df: &DataFrame,
        keys: &[Expr],
        aggs: &[Expr],
    ) -> Result<DataFrame> {
        // ── 1. Evaluate all expressions ──────────────────────────────────────
        let key_series: Vec<Series> = keys
            .iter()
            .map(|k| Self::eval_expr(k, df))
            .collect::<Result<Vec<_>>>()?;

        let agg_info: Vec<(Expr, AggFunc, Option<String>)> = aggs
            .iter()
            .map(|e| Self::extract_agg(e))
            .collect::<Result<Vec<_>>>()?;

        let agg_sources: Vec<Series> = agg_info
            .iter()
            .map(|(input_expr, _, _)| Self::eval_expr(input_expr, df))
            .collect::<Result<Vec<_>>>()?;

        let agg_funcs: Vec<AggFunc> = agg_info.iter().map(|(_, f, _)| *f).collect();

        // ── 2. Pre-extract arrays (avoid per-row to_array() overhead) ─────────
        let key_arrays: Vec<Arc<dyn arrow2::array::Array>> =
            key_series.iter().map(|s| s.to_array()).collect();
        let src_arrays: Vec<Arc<dyn arrow2::array::Array>> =
            agg_sources.iter().map(|s| s.to_array()).collect();

        // ── 3. Single forward pass ────────────────────────────────────────────
        let n = df.height();
        // group_keys[i] = the typed key tuple for group i (insertion order)
        let mut group_keys: Vec<Vec<GroupKey>> = Vec::new();
        // group_index maps key → index into group_keys / group_accs
        let mut group_index: AHashMap<Vec<GroupKey>, usize> =
            AHashMap::with_capacity(n / 4 + 1);
        // group_accs[i][j] = accumulator for group i, agg expression j
        let mut group_accs: Vec<Vec<GroupAcc>> = Vec::new();

        for row_idx in 0..n {
            // Build the typed key for this row (no string allocations for numeric types)
            let key: Vec<GroupKey> = key_arrays
                .iter()
                .zip(key_series.iter())
                .map(|(arr, ks)| Self::group_key_at(arr.as_ref(), ks.dtype(), row_idx))
                .collect();

            // Look up or create the group
            let grp = if let Some(&idx) = group_index.get(&key) {
                idx
            } else {
                let idx = group_keys.len();
                group_keys.push(key.clone());
                group_index.insert(key, idx);
                group_accs.push(agg_funcs.iter().map(|&f| GroupAcc::new(f)).collect());
                idx
            };

            // Update running accumulators for this row
            let accs = &mut group_accs[grp];
            for (acc, arr) in accs.iter_mut().zip(src_arrays.iter()) {
                if arr.is_null(row_idx) {
                    continue;
                }
                if let Some(v) = Self::arr_val_f64(arr.as_ref(), row_idx) {
                    acc.update(v);
                } else if let Some(utf8) =
                    arr.as_any().downcast_ref::<Utf8Array<i32>>()
                {
                    // String column: handle the ops that make sense on strings
                    match acc.func {
                        AggFunc::Count => acc.count += 1,
                        AggFunc::NUnique => {
                            use std::hash::{Hash, Hasher};
                            let mut h = ahash::AHasher::default();
                            utf8.value(row_idx).hash(&mut h);
                            acc.n_bits.insert(h.finish());
                            acc.count += 1;
                        }
                        _ => {
                            acc.count += 1;
                        }
                    }
                }
            }
        }

        // ── 4. Build result DataFrame ─────────────────────────────────────────
        let g_count = group_keys.len();
        let mut result_columns: Vec<Series> =
            Vec::with_capacity(key_series.len() + agg_sources.len());

        // Key columns (typed, null-safe)
        for (ki, ks) in key_series.iter().enumerate() {
            let col = Self::build_key_column(ks.name(), ks.dtype(), &group_keys, ki)?;
            result_columns.push(col);
        }

        // Agg columns (all returned as f64)
        for (ai, (_, _, alias)) in agg_info.iter().enumerate() {
            let col_name = alias
                .clone()
                .unwrap_or_else(|| agg_sources[ai].name().to_string());
            let vals: Vec<f64> = (0..g_count).map(|g| group_accs[g][ai].result()).collect();
            result_columns.push(Series::from_f64(&col_name, vals));
        }

        DataFrame::new(result_columns)
    }

    /// Extract a typed `GroupKey` from a pre-loaded array at `idx`.
    /// No heap allocation for Int32, Int64, Bool, Float key columns.
    fn group_key_at(arr: &dyn arrow2::array::Array, dtype: &DataType, idx: usize) -> GroupKey {
        if arr.is_null(idx) {
            return GroupKey::Null;
        }
        match dtype {
            DataType::Int64 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                GroupKey::Int64(p.value(idx))
            }
            DataType::Int32 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                GroupKey::Int32(p.value(idx))
            }
            DataType::Boolean => {
                let p = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                GroupKey::Bool(p.value(idx))
            }
            DataType::Float64 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                GroupKey::F64Bits(p.value(idx).to_bits())
            }
            DataType::Float32 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
                GroupKey::F64Bits((p.value(idx) as f64).to_bits())
            }
            DataType::Utf8 => {
                let p = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                GroupKey::Utf8(p.value(idx).to_string())
            }
            _ => {
                // Fallback: index-based key (preserves grouping but loses type)
                GroupKey::Utf8(format!("#{}", idx))
            }
        }
    }

    /// Read the value at `idx` from any primitive array as f64 without allocating.
    /// Returns `None` for non-numeric types (e.g. Utf8, Boolean).
    fn arr_val_f64(arr: &dyn arrow2::array::Array, idx: usize) -> Option<f64> {
        use arrow2::datatypes::DataType as AD;
        match arr.data_type() {
            AD::Float64 => arr
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .map(|a| a.value(idx)),
            AD::Float32 => arr
                .as_any()
                .downcast_ref::<PrimitiveArray<f32>>()
                .map(|a| a.value(idx) as f64),
            AD::Int64 | AD::Timestamp(_, _) | AD::Date64 => arr
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .map(|a| a.value(idx) as f64),
            AD::Int32 | AD::Date32 => arr
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .map(|a| a.value(idx) as f64),
            AD::UInt64 => arr
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .map(|a| a.value(idx) as f64),
            AD::UInt32 => arr
                .as_any()
                .downcast_ref::<PrimitiveArray<u32>>()
                .map(|a| a.value(idx) as f64),
            _ => None,
        }
    }

    /// Reconstruct a typed key column from insertion-ordered `group_keys`.
    fn build_key_column(
        name: &str,
        dtype: &DataType,
        group_keys: &[Vec<GroupKey>],
        ki: usize,
    ) -> Result<Series> {
        match dtype {
            DataType::Int64 | DataType::Int32 => {
                let vals: Vec<Option<i64>> = group_keys
                    .iter()
                    .map(|k| match &k[ki] {
                        GroupKey::Int64(v) => Some(*v),
                        GroupKey::Int32(v) => Some(*v as i64),
                        _ => None,
                    })
                    .collect();
                Ok(Series::from_opt_i64(name, vals))
            }
            DataType::Float64 | DataType::Float32 => {
                let vals: Vec<Option<f64>> = group_keys
                    .iter()
                    .map(|k| match &k[ki] {
                        GroupKey::F64Bits(b) => Some(f64::from_bits(*b)),
                        _ => None,
                    })
                    .collect();
                Ok(Series::from_opt_f64(name, vals))
            }
            DataType::Boolean => {
                // Boolean doesn't have nulls in this path
                let vals: Vec<bool> = group_keys
                    .iter()
                    .map(|k| match &k[ki] {
                        GroupKey::Bool(v) => *v,
                        _ => false,
                    })
                    .collect();
                Ok(Series::from_bool(name, vals))
            }
            DataType::Utf8 => {
                let owned: Vec<Option<String>> = group_keys
                    .iter()
                    .map(|k| match &k[ki] {
                        GroupKey::Utf8(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let str_refs: Vec<Option<&str>> =
                    owned.iter().map(|v| v.as_deref()).collect();
                Ok(Series::from_opt_str(name, str_refs))
            }
            _ => {
                // Generic fallback: emit nulls
                Ok(Series::from_opt_str(
                    name,
                    vec![None; group_keys.len()],
                ))
            }
        }
    }

    /// Extract the input expression, aggregation function, and optional alias from an agg expression.
    fn extract_agg(expr: &Expr) -> Result<(Expr, AggFunc, Option<String>)> {
        match expr {
            Expr::Alias { expr, name } => {
                let (input, func, _) = Self::extract_agg(expr)?;
                Ok((input, func, Some(name.clone())))
            }
            Expr::Agg { input, func } => Ok((*input.clone(), *func, None)),
            _ => Err(BlazeError::InvalidOperation(format!(
                "Expected aggregation expression, got: {:?}",
                expr
            ))),
        }
    }

    /// Execute a join operation.
    fn execute_join(
        left: &DataFrame,
        right: &DataFrame,
        left_on: &[Expr],
        right_on: &[Expr],
        join_type: JoinType,
    ) -> Result<DataFrame> {
        // Only support single-column joins for now, and inner join
        if left_on.len() != 1 || right_on.len() != 1 {
            return Err(BlazeError::InvalidOperation(
                "Only single-column joins supported currently".into(),
            ));
        }

        let left_key_name = match &left_on[0] {
            Expr::Column(name) => name.clone(),
            _ => return Err(BlazeError::InvalidOperation("Join key must be a column".into())),
        };
        let right_key_name = match &right_on[0] {
            Expr::Column(name) => name.clone(),
            _ => return Err(BlazeError::InvalidOperation("Join key must be a column".into())),
        };

        let left_key = left.column(&left_key_name)?;
        let right_key = right.column(&right_key_name)?;

        // Build hash index on right
        let mut right_index: AHashMap<String, Vec<usize>> = AHashMap::new();
        for i in 0..right_key.len() {
            let key = Self::series_value_to_string(right_key, i);
            right_index.entry(key).or_default().push(i);
        }

        let mut left_indices: Vec<u32> = Vec::new();
        let mut right_indices: Vec<u32> = Vec::new();

        match join_type {
            JoinType::Inner => {
                for i in 0..left_key.len() {
                    let key = Self::series_value_to_string(left_key, i);
                    if let Some(matches) = right_index.get(&key) {
                        for &j in matches {
                            left_indices.push(i as u32);
                            right_indices.push(j as u32);
                        }
                    }
                }
            }
            JoinType::Left => {
                for i in 0..left_key.len() {
                    let key = Self::series_value_to_string(left_key, i);
                    if let Some(matches) = right_index.get(&key) {
                        for &j in matches {
                            left_indices.push(i as u32);
                            right_indices.push(j as u32);
                        }
                    }
                    // For left join, if no match, we'd need null handling.
                    // Simplified: inner-like for now.
                }
            }
            _ => {
                return Err(BlazeError::InvalidOperation(format!(
                    "Join type {:?} not yet implemented",
                    join_type
                )));
            }
        }

        let left_idx = PrimitiveArray::<u32>::from_vec(left_indices);
        let right_idx = PrimitiveArray::<u32>::from_vec(right_indices);

        // Build result columns
        let mut columns = Vec::new();

        // All left columns
        for col in left.columns() {
            columns.push(col.take(&left_idx)?);
        }

        // Right columns except the join key
        for col in right.columns() {
            if col.name() != right_key_name {
                let mut taken = col.take(&right_idx)?;
                // If name conflicts with left, suffix with _right
                if left.schema().index_of(col.name()).is_some() {
                    taken.rename(&format!("{}_right", col.name()));
                }
                columns.push(taken);
            }
        }

        DataFrame::new(columns)
    }

    /// Deduplicate every row in a materialised DataFrame.
    ///
    /// Uses the same per-column u64 fingerprint strategy as `DistinctStream` —
    /// one word per column, null-safe, no string allocations for numeric types.
    fn execute_distinct(df: &DataFrame) -> Result<DataFrame> {
        use ahash::AHashSet;
        use arrow2::array::{BooleanArray, PrimitiveArray, Utf8Array};

        let n = df.height();
        if n == 0 {
            return Ok(df.clone());
        }

        // Pre-extract arrays and dtypes once.
        let cols: Vec<(Arc<dyn arrow2::array::Array>, DataType)> = df
            .columns()
            .iter()
            .map(|s| (s.to_array(), s.dtype().clone()))
            .collect();

        // fingerprint_val mirrors DistinctStream::fingerprint_col.
        let fingerprint_val = |arr: &dyn arrow2::array::Array, dtype: &DataType, row: usize| -> u64 {
            if arr.is_null(row) {
                return u64::MAX;
            }
            match dtype {
                DataType::Int64 | DataType::Timestamp | DataType::Date64 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap().value(row) as u64
                }
                DataType::Int32 | DataType::Date32 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap().value(row) as u64
                }
                DataType::Int16 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<i16>>().unwrap().value(row) as u64
                }
                DataType::Int8 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap().value(row) as u64
                }
                DataType::UInt64 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap().value(row)
                }
                DataType::UInt32 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap().value(row) as u64
                }
                DataType::UInt16 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap().value(row) as u64
                }
                DataType::UInt8 => {
                    arr.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap().value(row) as u64
                }
                DataType::Float64 => {
                    let v = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap().value(row);
                    if v == 0.0 { 0 } else { v.to_bits() }
                }
                DataType::Float32 => {
                    let v = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap().value(row);
                    if v == 0.0f32 { 0 } else { (v as f64).to_bits() }
                }
                DataType::Boolean => {
                    arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(row) as u64
                }
                DataType::Utf8 => {
                    use std::hash::{Hash, Hasher};
                    let p = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                    let mut h = ahash::AHasher::default();
                    p.value(row).hash(&mut h);
                    h.finish()
                }
                DataType::LargeUtf8 => {
                    use std::hash::{Hash, Hasher};
                    let p = arr.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
                    let mut h = ahash::AHasher::default();
                    p.value(row).hash(&mut h);
                    h.finish()
                }
                _ => row as u64,
            }
        };

        let mut seen: AHashSet<Vec<u64>> = AHashSet::with_capacity(n.min(4096));
        let mut keep: Vec<u32> = Vec::new();

        for row in 0..n {
            let fp: Vec<u64> = cols
                .iter()
                .map(|(arr, dt)| fingerprint_val(arr.as_ref(), dt, row))
                .collect();
            if seen.insert(fp) {
                keep.push(row as u32);
            }
        }

        // Fast path: nothing was removed.
        if keep.len() == n {
            return Ok(df.clone());
        }

        let indices = Arc::new(PrimitiveArray::<u32>::from_vec(keep));
        let deduped_cols: Result<Vec<Series>> = df.columns().iter().map(|s| s.take(&indices)).collect();
        DataFrame::new(deduped_cols?)
    }

    fn series_value_to_string(series: &Series, idx: usize) -> String {
        let arr = series.to_array();
        if arr.is_null(idx) {
            return "<<NULL>>".to_string();
        }
        match series.dtype() {
            DataType::Int64 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            DataType::Float64 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f64>>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            DataType::Utf8 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap();
                p.value(idx).to_string()
            }
            DataType::Boolean => {
                let p = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            DataType::Int32 => {
                let p = arr
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();
                format!("{}", p.value(idx))
            }
            _ => format!("row_{}", idx),
        }
    }

    /// Evaluate a window function.
    fn eval_window(
        input: &Expr,
        partition_by: &[Expr],
        df: &DataFrame,
    ) -> Result<Series> {
        // Evaluate partition keys
        let partition_series: Vec<Series> = partition_by
            .iter()
            .map(|e| Self::eval_expr(e, df))
            .collect::<Result<Vec<_>>>()?;

        // Build partition groups
        let n = df.height();
        let mut groups: AHashMap<Vec<String>, Vec<usize>> = AHashMap::new();
        for i in 0..n {
            let key: Vec<String> = partition_series
                .iter()
                .map(|s| Self::series_value_to_string(s, i))
                .collect();
            groups.entry(key).or_default().push(i);
        }

        // Extract the aggregation from input
        let (inner_expr, func) = match input {
            Expr::Agg { input, func } => (input.as_ref(), *func),
            _ => {
                return Err(BlazeError::InvalidOperation(
                    "Window function requires an aggregation expression".into(),
                ));
            }
        };

        let source = Self::eval_expr(inner_expr, df)?;

        // Compute per-partition aggregate and broadcast back
        let mut result_values: Vec<f64> = vec![0.0; n];
        for (_key, indices) in &groups {
            let idx_arr = PrimitiveArray::<u32>::from_vec(
                indices.iter().map(|&i| i as u32).collect(),
            );
            let group_series = source.take(&idx_arr)?;
            let agg_val = match func {
                AggFunc::Sum => group_series.sum_as_f64()?,
                AggFunc::Mean => group_series.mean_as_f64()?,
                AggFunc::Min => group_series.min_as_f64()?,
                AggFunc::Max => group_series.max_as_f64()?,
                AggFunc::Count => group_series.count() as f64,
                AggFunc::NUnique => {
                    let mut seen = std::collections::HashSet::new();
                    for i in 0..group_series.len() {
                        let arr = group_series.to_array();
                        if !arr.is_null(i) {
                            seen.insert(Self::series_value_to_string(&group_series, i));
                        }
                    }
                    seen.len() as f64
                }
                _ => {
                    return Err(BlazeError::InvalidOperation(
                        "Unsupported window function".into(),
                    ));
                }
            };
            for &idx in indices {
                result_values[idx] = agg_val;
            }
        }

        Ok(Series::from_f64(source.name(), result_values))
    }

    /// Extract a flat list of `RgPredicate`s from a filter expression.
    ///
    /// Only `col OP lit` leaves and AND-trees of them are handled.  Anything
    /// more complex (OR, NOT, expressions involving multiple columns, etc.) is
    /// safely ignored — the zone-map filter is always a conservative
    /// over-approximation; the Filter node above the DatasetScan handles
    /// correctness.
    fn extract_rg_predicates(expr: &Expr) -> Vec<crate::io::RgPredicate> {
        use crate::io::{RgOp, RgPredicate, RgValue};

        match expr {
            // AND tree — recurse into both branches
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut preds = Self::extract_rg_predicates(left);
                preds.extend(Self::extract_rg_predicates(right));
                preds
            }

            // col OP lit
            Expr::BinaryExpr { left, op, right } => {
                let mk_val = |lit: &LitValue| -> Option<RgValue> {
                    match lit {
                        LitValue::Int64(v) => Some(RgValue::Int64(*v)),
                        LitValue::Float64(v) => Some(RgValue::Float64(*v)),
                        LitValue::Utf8(v) => Some(RgValue::Utf8(v.clone())),
                        _ => None,
                    }
                };

                match (left.as_ref(), right.as_ref()) {
                    // col OP lit
                    (Expr::Column(name), Expr::Literal(lit)) => {
                        let rg_op = match op {
                            BinaryOp::Gt   => RgOp::Gt,
                            BinaryOp::GtEq => RgOp::GtEq,
                            BinaryOp::Lt   => RgOp::Lt,
                            BinaryOp::LtEq => RgOp::LtEq,
                            BinaryOp::Eq   => RgOp::Eq,
                            _ => return vec![],
                        };
                        if let Some(value) = mk_val(lit) {
                            vec![RgPredicate { column: name.clone(), op: rg_op, value }]
                        } else {
                            vec![]
                        }
                    }
                    // lit OP col — flip the operator
                    (Expr::Literal(lit), Expr::Column(name)) => {
                        let rg_op = match op {
                            BinaryOp::Gt   => RgOp::Lt,   // lit > col ≡ col < lit
                            BinaryOp::GtEq => RgOp::LtEq,
                            BinaryOp::Lt   => RgOp::Gt,
                            BinaryOp::LtEq => RgOp::GtEq,
                            BinaryOp::Eq   => RgOp::Eq,
                            _ => return vec![],
                        };
                        if let Some(value) = mk_val(lit) {
                            vec![RgPredicate { column: name.clone(), op: rg_op, value }]
                        } else {
                            vec![]
                        }
                    }
                    _ => vec![],
                }
            }

            _ => vec![],
        }
    }
}

/// Join type enum (re-exported from lazy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
    Cross,
}
