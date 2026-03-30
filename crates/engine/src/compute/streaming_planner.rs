use crate::compute::streaming::*;
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
                pipeline.add_op(Box::new(ProjectStream::new(exprs.clone())));
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
                let files = match format {
                    crate::dataset::FileFormat::Parquet => {
                        collect_files(std::path::Path::new(root), "parquet")
                    }
                    crate::dataset::FileFormat::Csv => {
                        collect_files(std::path::Path::new(root), "csv")
                    }
                    _ => Vec::new(),
                };

                let mut stream = ParquetStream::new(files);
                if let Some(proj) = projection {
                    stream = stream.with_projection(proj.clone());
                }
                Ok(Box::new(stream))
            }
            _ => Err(BlazeError::InvalidOperation(
                "Could not find a source node in the plan".into(),
            )),
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
            | LogicalPlan::Limit { .. } => {
                // Check child too
                match plan {
                    LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Select { input, .. }
                    | LogicalPlan::WithColumns { input, .. }
                    | LogicalPlan::Sort { input, .. }
                    | LogicalPlan::GroupBy { input, .. }
                    | LogicalPlan::Limit { input, .. } => Self::is_streamable(input),
                    _ => true,
                }
            }
            // Joins and Distinct cannot stream
            _ => false,
        }
    }
}
