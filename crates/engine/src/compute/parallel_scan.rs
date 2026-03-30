use std::path::PathBuf;

use rayon::prelude::*;

use crate::compute::executor::PhysicalExecutor;
use crate::dataframe::DataFrame;
use crate::error::Result;
use crate::expr::Expr;
use crate::io::ParquetReader;

/// Reads multiple Parquet files in parallel using Rayon, applies per-file
/// filter + projection, and returns partial DataFrames.
pub struct ParallelScanner {
    files: Vec<PathBuf>,
    projection: Option<Vec<String>>,
    predicate: Option<Expr>,
    n_threads: usize,
}

impl ParallelScanner {
    pub fn new(files: Vec<PathBuf>) -> Self {
        ParallelScanner {
            files,
            projection: None,
            predicate: None,
            n_threads: rayon::current_num_threads(),
        }
    }

    pub fn with_projection(mut self, cols: Vec<String>) -> Self {
        self.projection = Some(cols);
        self
    }

    pub fn with_predicate(mut self, pred: Expr) -> Self {
        self.predicate = Some(pred);
        self
    }

    pub fn with_threads(mut self, n: usize) -> Self {
        self.n_threads = n;
        self
    }

    /// Read all files in parallel, returning one DataFrame per file.
    pub fn scan_partial(&self) -> Result<Vec<DataFrame>> {
        let results: Vec<Result<DataFrame>> = self
            .files
            .par_iter()
            .map(|file| {
                let mut reader = ParquetReader::from_path(file)?;
                if let Some(ref proj) = self.projection {
                    reader = reader.with_projection(proj.clone());
                }
                let df = reader.finish()?;

                // Apply predicate filter if specified
                if let Some(ref pred) = self.predicate {
                    let mask_series = PhysicalExecutor::eval_expr(pred, &df)?;
                    let mask = mask_series.as_bool()?;
                    let filtered = df.filter(&mask.0)?;
                    Ok(filtered)
                } else {
                    Ok(df)
                }
            })
            .collect();

        // Collect results, propagating errors
        let mut dfs = Vec::with_capacity(results.len());
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(df) => dfs.push(df),
                Err(e) => errors.push(e),
            }
        }

        if !errors.is_empty() {
            return Err(errors.remove(0));
        }

        Ok(dfs)
    }

    /// Read all files, filter+project each, then vstack into one DataFrame.
    pub fn scan_and_collect(&self) -> Result<DataFrame> {
        let partials = self.scan_partial()?;
        if partials.is_empty() {
            return Ok(DataFrame::empty());
        }

        let mut combined = partials[0].clone();
        for df in &partials[1..] {
            combined = combined.vstack(df)?;
        }
        Ok(combined)
    }
}
