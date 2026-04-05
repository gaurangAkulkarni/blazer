use crate::compute::streaming::{
    DistinctStream, FilterStream, LimitStream, PartialAggStream, Pipeline,
    ProjectStream, SortStream, WithColumnsStream,
};
use crate::dataframe::DataFrame;
use crate::dataset::collect_files;
use crate::error::{BlazeError, Result};
use crate::io::parquet_stream::ParquetStream;
use crate::lazy::LogicalPlan;

pub struct StreamingPlanner;

impl StreamingPlanner {
    /// Convert an optimized logical plan into a streaming pipeline.
    pub fn build_pipeline(plan: &LogicalPlan, ram_budget: usize) -> Result<Pipeline> {
        let mut pipeline = Pipeline::new();
        Self::collect_operators(plan, &mut pipeline, ram_budget)?;
        Ok(pipeline)
    }

    /// Recursively collect operators from the plan tree (post-order: source first).
    fn collect_operators(
        plan: &LogicalPlan,
        pipeline: &mut Pipeline,
        ram_budget: usize,
    ) -> Result<()> {
        match plan {
            LogicalPlan::DataFrameScan { .. } => {
                // Source — handled separately in build_source
                Ok(())
            }
            LogicalPlan::DatasetScan { .. } => {
                // Source — handled separately in build_source
                Ok(())
            }
            LogicalPlan::Filter { input, predicate } => {
                Self::collect_operators(input, pipeline, ram_budget)?;
                pipeline.add_op(Box::new(FilterStream::new(predicate.clone())));
                Ok(())
            }
            LogicalPlan::Select { input, exprs } => {
                Self::collect_operators(input, pipeline, ram_budget)?;
                pipeline.add_op(Box::new(ProjectStream::new(exprs.clone())));
                Ok(())
            }
            LogicalPlan::WithColumns { input, exprs } => {
                Self::collect_operators(input, pipeline, ram_budget)?;
                // WithColumnsStream adds new columns while preserving originals,
                // unlike ProjectStream which replaces all columns.
                pipeline.add_op(Box::new(WithColumnsStream::new(exprs.clone())));
                Ok(())
            }
            LogicalPlan::GroupBy {
                input, keys, aggs, ..
            } => {
                Self::collect_operators(input, pipeline, ram_budget)?;
                pipeline.add_op(Box::new(PartialAggStream::new(
                    keys.clone(),
                    aggs.clone(),
                )));
                Ok(())
            }
            LogicalPlan::Sort {
                input,
                by_column,
                options,
            } => {
                Self::collect_operators(input, pipeline, ram_budget)?;
                pipeline.add_op(Box::new(SortStream::new(
                    by_column.clone(),
                    options.descending,
                    ram_budget,
                )));
                Ok(())
            }
            LogicalPlan::Limit { input, n } => {
                Self::collect_operators(input, pipeline, ram_budget)?;
                pipeline.add_op(Box::new(LimitStream::new(*n)));
                Ok(())
            }
            LogicalPlan::Distinct { input } => {
                Self::collect_operators(input, pipeline, ram_budget)?;
                pipeline.add_op(Box::new(DistinctStream::new()));
                Ok(())
            }
            _ => Err(BlazeError::InvalidOperation(format!(
                "Plan node not supported in streaming mode: {:?}",
                std::mem::discriminant(plan)
            ))),
        }
    }

    /// Extract the source iterator from the plan root.
    pub fn build_source(
        plan: &LogicalPlan,
    ) -> Result<Box<dyn Iterator<Item = Result<DataFrame>>>> {
        match Self::find_source(plan) {
            Some(LogicalPlan::DataFrameScan { df, .. }) => {
                // Chunk the in-memory DataFrame into pieces
                let chunk_size = 500_000;
                let height = df.height();
                let mut chunks = Vec::new();
                let mut offset = 0;
                while offset < height {
                    let len = (height - offset).min(chunk_size);
                    let sliced_cols: Vec<crate::series::Series> = df
                        .columns()
                        .iter()
                        .map(|s| s.slice(offset, len))
                        .collect();
                    if let Ok(chunk_df) = DataFrame::new(sliced_cols) {
                        chunks.push(Ok(chunk_df));
                    }
                    offset += len;
                }
                if chunks.is_empty() {
                    chunks.push(Ok(df.clone()));
                }
                Ok(Box::new(chunks.into_iter()))
            }
            Some(LogicalPlan::DatasetScan {
                root,
                format,
                projection,
                partition_filters: _,
                n_rows: _,
                ..
            }) => {
                let root_path = std::path::Path::new(root);
                let ext = match format {
                    crate::dataset::FileFormat::Parquet => "parquet",
                    crate::dataset::FileFormat::Csv => "csv",
                    _ => "parquet",
                };
                // collect_files only walks directories — handle single-file paths
                // explicitly so that `scan_parquet("/some/file.parquet")` works.
                let files = if root_path.is_file() {
                    vec![root_path.to_path_buf()]
                } else {
                    collect_files(root_path, ext)
                };

                let mut stream = ParquetStream::new(files);
                if let Some(proj) = projection {
                    stream = stream.with_projection(proj.clone());
                }
                // Pushdown: if the plan contains a Limit node, tell the
                // parquet reader to stop decompressing pages after that many
                // rows.  This mirrors what DuckDB does and shrinks
                // `SELECT … LIMIT 10` from seconds to milliseconds by never
                // reading the bulk of the row group.
                if let Some(n) = Self::find_limit(plan) {
                    stream = stream.with_row_limit(n);
                }
                Ok(Box::new(stream))
            }
            _ => Err(BlazeError::InvalidOperation(
                "Could not find a source node in the plan".into(),
            )),
        }
    }

    /// Return the Limit n to push to the parquet page reader, if and only if
    /// the path from the Limit node to the DatasetScan is **clean** — i.e.
    /// contains only column-projection operators (Select, WithColumns) with no
    /// row-altering barriers in between.
    ///
    /// # Why barriers matter
    ///
    /// Pushing `n_rows = LIMIT` to the parquet reader is an optimisation that
    /// tells the page decoder to stop decompressing once N rows have been
    /// produced.  It is only safe when every row the parquet reader emits is
    /// guaranteed to reach the output unchanged (or with only column pruning).
    ///
    /// Barriers that invalidate the pushdown:
    ///
    ///   Filter    — may drop rows, so reading N source rows yields < N output
    ///               rows.  The limit must NOT be pushed down; all matching rows
    ///               must be found first.
    ///   Sort      — requires seeing every row to determine the correct top-N.
    ///   GroupBy   — M input rows → G groups; limit on output ≠ limit on input.
    ///   Distinct  — deduplicates; unique count ≠ source row count.
    ///
    /// The optimizer can hoist a Limit inside Select/WithColumns wrappers, so
    /// we look through those to find an inner Limit node.
    fn find_limit(plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::Limit { n, input } => {
                // Only push when there is a clean column-projection-only path
                // from this Limit to the underlying DatasetScan.
                if Self::path_to_scan_is_clean(input) {
                    Some(*n)
                } else {
                    None
                }
            }
            // The optimizer can push Limit inside Select/WithColumns; look
            // through them to find an inner Limit.
            LogicalPlan::Select { input, .. }
            | LogicalPlan::WithColumns { input, .. } => Self::find_limit(input),
            _ => None,
        }
    }

    /// Returns `true` when `plan` is a DatasetScan (or DataFrameScan) reached
    /// through zero or more column-projection nodes (Select, WithColumns).
    /// Any other node — Filter, Sort, GroupBy, Distinct, … — returns `false`.
    fn path_to_scan_is_clean(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::DatasetScan { .. } | LogicalPlan::DataFrameScan { .. } => true,
            LogicalPlan::Select { input, .. } | LogicalPlan::WithColumns { input, .. } => {
                Self::path_to_scan_is_clean(input)
            }
            _ => false,
        }
    }

    /// Walk the plan tree to find the leaf source node.
    fn find_source(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::DataFrameScan { .. } | LogicalPlan::DatasetScan { .. } => Some(plan),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Select { input, .. }
            | LogicalPlan::WithColumns { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::GroupBy { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input, .. } => Self::find_source(input),
            _ => None,
        }
    }

    /// Determine if a plan can run in streaming mode.
    pub fn is_streamable(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::DataFrameScan { .. }
            | LogicalPlan::DatasetScan { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Select { .. }
            | LogicalPlan::WithColumns { .. }
            | LogicalPlan::GroupBy { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Distinct { .. } => {
                // Recurse into child for all single-input nodes.
                match plan {
                    LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Select { input, .. }
                    | LogicalPlan::WithColumns { input, .. }
                    | LogicalPlan::Sort { input, .. }
                    | LogicalPlan::GroupBy { input, .. }
                    | LogicalPlan::Limit { input, .. }
                    | LogicalPlan::Distinct { input } => Self::is_streamable(input),
                    _ => true,
                }
            }
            // Joins cannot stream (require full materialisation of both sides).
            _ => false,
        }
    }
}
