use crate::compute::executor::PhysicalExecutor;
use crate::dataframe::DataFrame;
use crate::error::Result;
use crate::expr::Expr;
use crate::io::spill::SpillManager;

/// Trait for any operator that can process data chunk-by-chunk.
pub trait StreamingOperator: Send {
    /// Process one chunk. Returns Some(DataFrame) if there is output to pass downstream.
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>>;

    /// Called after all chunks have been pushed.
    fn flush(&mut self) -> Result<Option<DataFrame>>;

    /// Human-readable name for explain output.
    fn name(&self) -> &'static str;
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

// ── PartialAggStream ──────────────────────────────────────────────────

pub struct PartialAggStream {
    keys: Vec<Expr>,
    aggs: Vec<Expr>,
    chunks: Vec<DataFrame>,
}

impl PartialAggStream {
    pub fn new(keys: Vec<Expr>, aggs: Vec<Expr>) -> Self {
        PartialAggStream {
            keys,
            aggs,
            chunks: Vec::new(),
        }
    }
}

impl StreamingOperator for PartialAggStream {
    fn push(&mut self, chunk: DataFrame) -> Result<Option<DataFrame>> {
        // Accumulate raw chunks — aggregate on flush
        self.chunks.push(chunk);
        Ok(None)
    }

    fn flush(&mut self) -> Result<Option<DataFrame>> {
        if self.chunks.is_empty() {
            return Ok(None);
        }

        // Vstack all raw chunks
        let mut combined = self.chunks[0].clone();
        for c in &self.chunks[1..] {
            combined = combined.vstack(c)?;
        }
        self.chunks.clear();

        // Run a single group_by over all data
        let result = PhysicalExecutor::execute(
            crate::lazy::LogicalPlan::GroupBy {
                input: Box::new(crate::lazy::LogicalPlan::DataFrameScan {
                    df: combined,
                    projection: None,
                }),
                keys: self.keys.clone(),
                aggs: self.aggs.clone(),
            },
        )?;

        Ok(Some(result))
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
