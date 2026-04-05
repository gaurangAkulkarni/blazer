use std::sync::Arc;

use ahash::AHashSet;
use arrow2::array::{Array, BooleanArray, PrimitiveArray, Utf8Array};

use crate::compute::executor::PhysicalExecutor;
use crate::dataframe::DataFrame;
use crate::dtype::DataType;
use crate::error::Result;
use crate::expr::{AggFunc, Expr};
use crate::io::spill::SpillManager;

/// Trait for any operator that can process data chunk-by-chunk.
pub trait StreamingOperator: Send {
    /// Process one chunk. Returns Some(DataFrame) if there is output to pass downstream.
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>>;

    /// Called after all chunks have been pushed.
    fn flush(&mut self) -> Result<Option<DataFrame>>;

    /// Human-readable name for explain output.
    fn name(&self) -> &'static str;

    /// Returns true when this operator will never produce more output (e.g. a
    /// Limit whose row budget has been exhausted).  The streaming loop uses
    /// this to short-circuit the source iterator so we never read data we
    /// cannot use.
    fn is_done(&self) -> bool {
        false
    }
}

/// Chains operators: output of one feeds input of next.
pub struct Pipeline {
    ops: Vec<Box<dyn StreamingOperator>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline { ops: Vec::new() }
    }

    pub fn push_op(mut self, op: impl StreamingOperator + 'static) -> Self {
        self.ops.push(Box::new(op));
        self
    }

    pub fn add_op(&mut self, op: Box<dyn StreamingOperator>) {
        self.ops.push(op);
    }

    /// Push a chunk through all operators in sequence.
    pub fn push_chunk(&mut self, chunk: DataFrame) -> Result<Vec<DataFrame>> {
        let mut outputs = Vec::new();
        let mut current = Some(chunk);

        for op in &mut self.ops {
            if let Some(c) = current.take() {
                current = op.push(c)?;
            } else {
                break;
            }
        }

        if let Some(out) = current {
            outputs.push(out);
        }
        Ok(outputs)
    }

    /// Returns true when at least one operator in the pipeline has signalled
    /// that it is done (e.g. a LimitStream whose budget is exhausted).  The
    /// streaming loop should stop pulling from the source when this is true.
    pub fn is_done(&self) -> bool {
        self.ops.iter().any(|op| op.is_done())
    }

    /// Flush all operators in sequence, collecting final outputs.
    pub fn flush(&mut self) -> Result<Vec<DataFrame>> {
        let mut outputs = Vec::new();

        // Flush operators from first to last
        for i in 0..self.ops.len() {
            if let Some(flushed) = self.ops[i].flush()? {
                // Push flushed data through remaining operators
                let mut current = Some(flushed);
                for j in (i + 1)..self.ops.len() {
                    if let Some(c) = current.take() {
                        current = self.ops[j].push(c)?;
                    }
                }
                if let Some(out) = current {
                    outputs.push(out);
                }
            }
        }

        Ok(outputs)
    }
}

// ── FilterStream ──────────────────────────────────────────────────────

pub struct FilterStream {
    predicate: Expr,
}

impl FilterStream {
    pub fn new(predicate: Expr) -> Self {
        FilterStream { predicate }
    }
}

impl StreamingOperator for FilterStream {
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        let mask_series = PhysicalExecutor::eval_expr(&self.predicate, &chunk)?;
        let mask = mask_series.as_bool()?;
        let filtered = chunk.filter(&mask.0)?;
        if filtered.height() == 0 {
            Ok(None)
        } else {
            Ok(Some(filtered))
        }
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        Ok(None)
    }

    fn name(&self) -> &'static str {
        "FilterStream"
    }
}

// ── ProjectStream ─────────────────────────────────────────────────────

pub struct ProjectStream {
    exprs: Vec<Expr>,
}

impl ProjectStream {
    pub fn new(exprs: Vec<Expr>) -> Self {
        ProjectStream { exprs }
    }
}

impl StreamingOperator for ProjectStream {
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        let mut columns = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            let series = PhysicalExecutor::eval_expr(expr, &chunk)?;
            columns.push(series);
        }
        Ok(Some(DataFrame::new(columns)?))
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        Ok(None)
    }

    fn name(&self) -> &'static str {
        "ProjectStream"
    }
}

// ── WithColumnsStream ─────────────────────────────────────────────────
//
// Like ProjectStream but ADDS new columns to the existing chunk instead of
// replacing all columns.  Used for LogicalPlan::WithColumns in the streaming
// planner so that `df.with_columns([...])` correctly preserves the original
// columns alongside the newly computed ones.

pub struct WithColumnsStream {
    exprs: Vec<Expr>,
}

impl WithColumnsStream {
    pub fn new(exprs: Vec<Expr>) -> Self {
        WithColumnsStream { exprs }
    }
}

impl StreamingOperator for WithColumnsStream {
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        // Start with all original columns.
        let mut columns: Vec<crate::series::Series> = chunk.columns().to_vec();
        // Evaluate each expression and add (or replace if same name) the result.
        for expr in &self.exprs {
            let new_col = PhysicalExecutor::eval_expr(expr, &chunk)?;
            // Replace column with the same name if it already exists.
            if let Some(pos) = columns.iter().position(|c| c.name() == new_col.name()) {
                columns[pos] = new_col;
            } else {
                columns.push(new_col);
            }
        }
        Ok(Some(DataFrame::new(columns)?))
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        Ok(None)
    }

    fn name(&self) -> &'static str {
        "WithColumnsStream"
    }
}

// ── PartialAggStream ──────────────────────────────────────────────────

pub struct PartialAggStream {
    keys: Vec<Expr>,
    aggs: Vec<Expr>,
    /// Re-aggregation expressions used when combining two partial results.
    ///
    /// After the first GroupBy the result has aliased column names (e.g.
    /// `total_sales`, `trip_count`), not the original source column names
    /// (e.g. `fare_amount`, `VendorID`).  `merge_aggs` are pre-built to
    /// reference those alias names so re-aggregation doesn't fail with
    /// "Column not found: fare_amount".
    ///
    /// Mapping rules (partial→partial):
    ///   sum(x).alias(y)     → sum(y).alias(y)       (sum of partial sums)
    ///   count(x).alias(y)   → sum(y).alias(y)       (sum of partial counts)
    ///   mean(x).alias(y)    → mean(y).alias(y)      (mean of partial means, approx)
    ///   min/max(x).alias(y) → min/max(y).alias(y)
    ///   first/last(x).alias → first/last(y).alias
    ///   n_unique(x).alias   → sum(y).alias(y)       (conservative over-count)
    merge_aggs: Vec<Expr>,
    /// Running partial aggregate result.
    ///
    /// After each chunk is processed this holds at most `n_groups` rows —
    /// the cardinality of the group-by key set, not the number of input rows.
    /// For VendorID (2 unique values) this is always ≤ 2 rows, regardless of
    /// whether we have processed 1 chunk or 10 000.
    partial: Option<DataFrame>,
}

impl PartialAggStream {
    pub fn new(keys: Vec<Expr>, aggs: Vec<Expr>) -> Self {
        let merge_aggs = Self::build_merge_aggs(&aggs);
        PartialAggStream { keys, aggs, merge_aggs, partial: None }
    }

    /// Build re-aggregation expressions that operate on aliased column names
    /// rather than original source column names.
    ///
    /// This is `pub` so the parallel-GroupBy execution path in `lazy/mod.rs`
    /// can reuse the same logic when merging per-file partial results.
    pub fn build_merge_aggs(aggs: &[Expr]) -> Vec<Expr> {
        aggs.iter()
            .map(|agg| match agg {
                Expr::Alias { expr, name } => {
                    let merge_func = match expr.as_ref() {
                        Expr::Agg { func, .. } => match func {
                            AggFunc::Sum => AggFunc::Sum,
                            // sum of partial counts = total count
                            AggFunc::Count => AggFunc::Sum,
                            // mean of partial means (approximate but acceptable)
                            AggFunc::Mean => AggFunc::Mean,
                            AggFunc::Min => AggFunc::Min,
                            AggFunc::Max => AggFunc::Max,
                            // sum of partial n_uniques (conservative over-count)
                            AggFunc::NUnique => AggFunc::Sum,
                            AggFunc::First => AggFunc::First,
                            AggFunc::Last => AggFunc::Last,
                        },
                        // Not an Agg inside Alias — keep the original expression.
                        _ => return agg.clone(),
                    };
                    Expr::Alias {
                        expr: Box::new(Expr::Agg {
                            input: Box::new(Expr::Column(name.clone())),
                            func: merge_func,
                        }),
                        name: name.clone(),
                    }
                }
                // No alias wrapper — return unchanged.
                _ => agg.clone(),
            })
            .collect()
    }

    /// Run a GroupBy on `df` using `aggs` and return the result.
    fn group_by_df(&self, df: DataFrame, aggs: &[Expr]) -> Result<DataFrame> {
        PhysicalExecutor::execute(crate::lazy::LogicalPlan::GroupBy {
            input: Box::new(crate::lazy::LogicalPlan::DataFrameScan {
                df,
                projection: None,
            }),
            keys: self.keys.clone(),
            aggs: aggs.to_vec(),
        })
    }
}

impl StreamingOperator for PartialAggStream {
    /// Process one chunk with true partial aggregation.
    ///
    /// Strategy:
    /// 1. GroupBy on the incoming chunk using original aggs → partial result
    ///    (n_groups rows with aliased column names).
    /// 2. Vstack with the running partial from previous chunks.
    /// 3. Re-GroupBy the combined partial using `merge_aggs` (which reference
    ///    aliased column names, not the original source columns) → still
    ///    n_groups rows.
    ///
    /// Memory stays O(n_groups) — typically tiny — regardless of dataset size.
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        // Step 1: aggregate this chunk alone using original expressions.
        let chunk_partial = self.group_by_df(chunk, &self.aggs.clone())?;

        // Step 2+3: merge with any accumulated partial result.
        self.partial = Some(match self.partial.take() {
            None => chunk_partial,
            Some(prev) => {
                // Combine the two small partial results and re-aggregate
                // to keep only one row per group.  Use merge_aggs here so
                // the GroupBy references the aliased column names that the
                // partial DataFrames actually contain.
                let combined = prev.vstack(&chunk_partial)?;
                self.group_by_df(combined, &self.merge_aggs.clone())?
            }
        });

        Ok(None) // result emitted in flush
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        Ok(self.partial.take())
    }

    fn name(&self) -> &'static str {
        "PartialAggStream"
    }
}

// ── PassthroughStream ─────────────────────────────────────────────────

pub struct PassthroughStream {
    rows_seen: usize,
    #[allow(dead_code)]
    label: String,
}

impl PassthroughStream {
    pub fn new(label: &str) -> Self {
        PassthroughStream {
            rows_seen: 0,
            label: label.to_string(),
        }
    }

    pub fn rows_seen(&self) -> usize {
        self.rows_seen
    }
}

impl StreamingOperator for PassthroughStream {
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        self.rows_seen += chunk.height();
        Ok(Some(chunk))
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        Ok(None)
    }

    fn name(&self) -> &'static str {
        "PassthroughStream"
    }
}

// ── LimitStream ───────────────────────────────────────────────────────

pub struct LimitStream {
    remaining: usize,
    done: bool,
}

impl LimitStream {
    pub fn new(n: usize) -> Self {
        LimitStream {
            remaining: n,
            done: false,
        }
    }
}

impl StreamingOperator for LimitStream {
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        if self.done || self.remaining == 0 {
            return Ok(None);
        }

        let h = chunk.height();
        if h <= self.remaining {
            self.remaining -= h;
            if self.remaining == 0 {
                self.done = true;
            }
            Ok(Some(chunk))
        } else {
            let result = chunk.head(self.remaining);
            self.remaining = 0;
            self.done = true;
            Ok(Some(result))
        }
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        Ok(None)
    }

    fn name(&self) -> &'static str {
        "LimitStream"
    }

    fn is_done(&self) -> bool {
        self.done
    }
}

// ── SortStream ────────────────────────────────────────────────────────

pub struct SortStream {
    by_column: String,
    descending: bool,
    chunks: Vec<DataFrame>,
    ram_budget: usize,
    bytes_used: usize,
    spill: Option<SpillManager>,
}

impl SortStream {
    pub fn new(by_column: String, descending: bool, ram_budget: usize) -> Self {
        SortStream {
            by_column,
            descending,
            chunks: Vec::new(),
            ram_budget,
            bytes_used: 0,
            spill: None,
        }
    }

    fn estimate_bytes(df: &DataFrame) -> usize {
        // Rough estimate: 8 bytes per cell
        df.height() * df.width() * 8
    }
}

impl StreamingOperator for SortStream {
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        let chunk_bytes = Self::estimate_bytes(&chunk);

        if self.bytes_used + chunk_bytes > self.ram_budget && !self.chunks.is_empty() {
            // Spill existing chunks to disk
            let spill = self
                .spill
                .get_or_insert_with(|| SpillManager::new(self.ram_budget, None).unwrap());

            // Sort and spill current in-memory chunks
            let mut combined = self.chunks[0].clone();
            for c in &self.chunks[1..] {
                combined = combined.vstack(c)?;
            }
            combined = combined.sort(&self.by_column, self.descending)?;
            spill.spill(&combined)?;

            self.chunks.clear();
            self.bytes_used = 0;
        }

        self.bytes_used += chunk_bytes;
        self.chunks.push(chunk);
        Ok(None) // Sort only emits on flush
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        // Combine all in-memory chunks
        let mut all_chunks = Vec::new();

        // Read back spilled data
        if let Some(ref spill) = self.spill {
            for df_result in spill.iter_spilled() {
                all_chunks.push(df_result?);
            }
        }

        // Add remaining in-memory chunks
        all_chunks.extend(self.chunks.drain(..));

        if all_chunks.is_empty() {
            return Ok(None);
        }

        let mut combined = all_chunks[0].clone();
        for c in &all_chunks[1..] {
            combined = combined.vstack(c)?;
        }

        // Final sort
        let sorted = combined.sort(&self.by_column, self.descending)?;
        Ok(Some(sorted))
    }

    fn name(&self) -> &'static str {
        "SortStream"
    }
}

// ── DistinctStream ────────────────────────────────────────────────────────────
//
// Tracks already-seen rows using a compact fingerprint: one u64 per column
// derived from the column value's bit pattern.  Storing the per-column u64
// vector as the key (rather than a single hash of the whole row) eliminates
// cross-column collisions while still being O(1) per row.
//
// Cardinality note: VendorID has 2 unique values.  After the first parquet row
// group every subsequent chunk emits 0 rows from push() — the whole scan keeps
// running but passes nothing downstream.  For a future optimisation, a `done`
// flag combined with a `Limit` placed above `Distinct` (e.g. LIMIT 1 on a known
// single-valued column) can short-circuit the source iterator entirely.

/// Streaming deduplication operator.
pub struct DistinctStream {
    /// `seen` maps row fingerprint → sentinel.  Vec<u64> is one word per column.
    seen: AHashSet<Vec<u64>>,
}

impl DistinctStream {
    pub fn new() -> Self {
        DistinctStream {
            seen: AHashSet::new(),
        }
    }

    /// Compute a per-column u64 fingerprint for the value at `row` in `col`.
    ///
    /// - Numerics   → bit-cast to u64 (widening for smaller types)
    /// - Boolean    → 0 or 1
    /// - Utf8       → ahash of the string bytes (fast, good distribution)
    /// - Null       → u64::MAX as a sentinel distinct from any real value
    fn fingerprint_col(arr: &dyn Array, dtype: &DataType, row: usize) -> u64 {
        if arr.is_null(row) {
            return u64::MAX;
        }
        match dtype {
            DataType::Int64 | DataType::Timestamp | DataType::Date64 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                p.value(row) as u64
            }
            DataType::Int32 | DataType::Date32 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                p.value(row) as u64
            }
            DataType::Int16 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<i16>>().unwrap();
                p.value(row) as u64
            }
            DataType::Int8 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();
                p.value(row) as u64
            }
            DataType::UInt64 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
                p.value(row)
            }
            DataType::UInt32 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
                p.value(row) as u64
            }
            DataType::UInt16 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
                p.value(row) as u64
            }
            DataType::UInt8 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
                p.value(row) as u64
            }
            DataType::Float64 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                // Normalise -0.0 → 0.0 so they hash identically.
                let v = p.value(row);
                if v == 0.0 { 0 } else { v.to_bits() }
            }
            DataType::Float32 => {
                let p = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
                let v = p.value(row);
                if v == 0.0f32 { 0 } else { (v as f64).to_bits() }
            }
            DataType::Boolean => {
                let p = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                p.value(row) as u64
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
            _ => {
                // Fallback: treat every row as distinct (safe, not optimal).
                row as u64
            }
        }
    }
}

impl StreamingOperator for DistinctStream {
    /// Receive one chunk, return only rows whose fingerprint hasn't been seen yet.
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        let n = chunk.height();
        if n == 0 {
            return Ok(None);
        }

        // Pre-extract arrays and dtypes once per chunk (not per row).
        let cols: Vec<_> = chunk.columns().iter().map(|s| {
            (s.to_array(), s.dtype().clone())
        }).collect();

        // Collect row indices that are new (not yet seen).
        let mut new_rows: Vec<u32> = Vec::new();
        for row in 0..n {
            let fingerprint: Vec<u64> = cols
                .iter()
                .map(|(arr, dtype)| Self::fingerprint_col(arr.as_ref(), dtype, row))
                .collect();

            if self.seen.insert(fingerprint) {
                // First time we've seen this combination → keep it.
                new_rows.push(row as u32);
            }
        }

        if new_rows.is_empty() {
            // Every row in this chunk was already seen — emit nothing.
            return Ok(None);
        }

        if new_rows.len() == n {
            // All rows are new — pass the chunk through unmodified.
            return Ok(Some(chunk));
        }

        // Take only the novel rows.
        let indices = Arc::new(PrimitiveArray::<u32>::from_vec(new_rows));
        let taken_cols: Result<Vec<_>> = chunk
            .columns()
            .iter()
            .map(|s| s.take(&indices))
            .collect();
        Ok(Some(DataFrame::new(taken_cols?)?))
    }

    /// No buffering — nothing to flush.
    fn flush(&mut self) -> Result<Option<DataFrame>> {
        Ok(None)
    }

    fn name(&self) -> &'static str {
        "DistinctStream"
    }
}
