use crate::compute::executor::PhysicalExecutor;
use crate::compute::streaming_planner::StreamingPlanner;
use crate::io::sink::{MemorySink, ParquetSink, CsvSink, Sink};
pub use crate::compute::executor::JoinType;
use crate::dataframe::DataFrame;
use crate::error::Result;
use crate::expr::{Expr, SortOptions};

/// A logical plan node representing a lazy computation.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    DataFrameScan {
        df: DataFrame,
        projection: Option<Vec<String>>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Select {
        input: Box<LogicalPlan>,
        exprs: Vec<Expr>,
    },
    WithColumns {
        input: Box<LogicalPlan>,
        exprs: Vec<Expr>,
    },
    Sort {
        input: Box<LogicalPlan>,
        by_column: String,
        options: SortOptions,
    },
    GroupBy {
        input: Box<LogicalPlan>,
        keys: Vec<Expr>,
        aggs: Vec<Expr>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
    },
    Limit {
        input: Box<LogicalPlan>,
        n: usize,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    /// Lazy scan of a partitioned dataset (not yet loaded).
    DatasetScan {
        root: String,
        format: crate::dataset::FileFormat,
        projection: Option<Vec<String>>,
        partition_filters: Vec<Expr>,
        row_filters: Option<Box<Expr>>,
        n_rows: Option<usize>,
    },
}

impl LogicalPlan {
    /// Pretty-print the plan tree.
    pub fn describe(&self, indent: usize) -> String {
        let pad = " ".repeat(indent);
        match self {
            LogicalPlan::DataFrameScan { projection, .. } => {
                format!("{}DataFrameScan [projection: {:?}]", pad, projection)
            }
            LogicalPlan::Filter { input, predicate } => {
                format!(
                    "{}Filter [{}]\n{}",
                    pad,
                    predicate,
                    input.describe(indent + 2)
                )
            }
            LogicalPlan::Select { input, exprs } => {
                let expr_strs: Vec<String> = exprs.iter().map(|e| format!("{}", e)).collect();
                format!(
                    "{}Select [{}]\n{}",
                    pad,
                    expr_strs.join(", "),
                    input.describe(indent + 2)
                )
            }
            LogicalPlan::WithColumns { input, exprs } => {
                let expr_strs: Vec<String> = exprs.iter().map(|e| format!("{}", e)).collect();
                format!(
                    "{}WithColumns [{}]\n{}",
                    pad,
                    expr_strs.join(", "),
                    input.describe(indent + 2)
                )
            }
            LogicalPlan::Sort {
                input,
                by_column,
                options,
            } => {
                format!(
                    "{}Sort [by={}, desc={}]\n{}",
                    pad,
                    by_column,
                    options.descending,
                    input.describe(indent + 2)
                )
            }
            LogicalPlan::GroupBy {
                input, keys, aggs, ..
            } => {
                format!(
                    "{}GroupBy [keys={:?}, aggs={}]\n{}",
                    pad,
                    keys,
                    aggs.len(),
                    input.describe(indent + 2)
                )
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                ..
            } => {
                format!(
                    "{}Join [{:?}]\n{}\n{}",
                    pad,
                    join_type,
                    left.describe(indent + 2),
                    right.describe(indent + 2)
                )
            }
            LogicalPlan::Limit { input, n } => {
                format!("{}Limit [{}]\n{}", pad, n, input.describe(indent + 2))
            }
            LogicalPlan::Distinct { input } => {
                format!("{}Distinct\n{}", pad, input.describe(indent + 2))
            }
            LogicalPlan::DatasetScan {
                root, format, projection, ..
            } => {
                format!(
                    "{}DatasetScan [root={}, format={:?}, projection={:?}]",
                    pad, root, format, projection
                )
            }
        }
    }
}

/// Query optimizer.
pub struct Optimizer;

impl Optimizer {
    /// Optimize a logical plan.
    pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
        let plan = Self::predicate_pushdown(plan);
        let plan = Self::projection_pushdown(plan);
        plan
    }

    /// Push filter predicates closer to the data source.
    fn predicate_pushdown(plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Filter over Select: push filter below select if possible
            LogicalPlan::Filter {
                input,
                predicate,
            } => {
                let optimized_input = Self::predicate_pushdown(*input);
                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }
            // Recursively optimize children
            LogicalPlan::Select { input, exprs } => LogicalPlan::Select {
                input: Box::new(Self::predicate_pushdown(*input)),
                exprs,
            },
            LogicalPlan::WithColumns { input, exprs } => LogicalPlan::WithColumns {
                input: Box::new(Self::predicate_pushdown(*input)),
                exprs,
            },
            LogicalPlan::Sort {
                input,
                by_column,
                options,
            } => LogicalPlan::Sort {
                input: Box::new(Self::predicate_pushdown(*input)),
                by_column,
                options,
            },
            LogicalPlan::GroupBy { input, keys, aggs } => LogicalPlan::GroupBy {
                input: Box::new(Self::predicate_pushdown(*input)),
                keys,
                aggs,
            },
            LogicalPlan::Limit { input, n } => LogicalPlan::Limit {
                input: Box::new(Self::predicate_pushdown(*input)),
                n,
            },
            LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(Self::predicate_pushdown(*input)),
            },
            LogicalPlan::DatasetScan { .. } => plan,
            other => other,
        }
    }

    /// Push projections closer to the data source.
    fn projection_pushdown(plan: LogicalPlan) -> LogicalPlan {
        // Simple pass-through for now — full projection pushdown is complex
        plan
    }
}

/// A LazyFrame is a deferred DataFrame computation.
#[derive(Clone)]
pub struct LazyFrame {
    plan: LogicalPlan,
    streaming_budget: usize,
}

const DEFAULT_STREAMING_BUDGET: usize = 2 * 1024 * 1024 * 1024; // 2GB

impl LazyFrame {
    pub fn from_dataframe(df: DataFrame) -> Self {
        LazyFrame {
            plan: LogicalPlan::DataFrameScan {
                df,
                projection: None,
            },
            streaming_budget: DEFAULT_STREAMING_BUDGET,
        }
    }

    /// Create a LazyFrame from a raw LogicalPlan.
    pub fn from_plan(plan: LogicalPlan) -> Self {
        LazyFrame {
            plan,
            streaming_budget: DEFAULT_STREAMING_BUDGET,
        }
    }

    /// Scan a Parquet file as a lazy dataset.
    pub fn scan_parquet(path: &str) -> Self {
        let path_buf = std::path::PathBuf::from(path);
        if path_buf.is_dir() {
            // Directory of parquet files
            LazyFrame {
                plan: LogicalPlan::DatasetScan {
                    root: path.to_string(),
                    format: crate::dataset::FileFormat::Parquet,
                    projection: None,
                    partition_filters: Vec::new(),
                    row_filters: None,
                    n_rows: None,
                },
                streaming_budget: DEFAULT_STREAMING_BUDGET,
            }
        } else {
            // Single file — read directly and wrap in DataFrameScan
            match crate::io::ParquetReader::from_path(path) {
                Ok(reader) => match reader.finish() {
                    Ok(df) => LazyFrame::from_dataframe(df),
                    Err(_) => LazyFrame {
                        plan: LogicalPlan::DatasetScan {
                            root: path.to_string(),
                            format: crate::dataset::FileFormat::Parquet,
                            projection: None,
                            partition_filters: Vec::new(),
                            row_filters: None,
                            n_rows: None,
                        },
                        streaming_budget: DEFAULT_STREAMING_BUDGET,
                    },
                },
                Err(_) => LazyFrame {
                    plan: LogicalPlan::DatasetScan {
                        root: path.to_string(),
                        format: crate::dataset::FileFormat::Parquet,
                        projection: None,
                        partition_filters: Vec::new(),
                        row_filters: None,
                        n_rows: None,
                    },
                    streaming_budget: DEFAULT_STREAMING_BUDGET,
                },
            }
        }
    }

    pub fn filter(self, predicate: Expr) -> Self {
        LazyFrame {
            plan: LogicalPlan::Filter {
                input: Box::new(self.plan),
                predicate,
            },
            streaming_budget: self.streaming_budget,
        }
    }

    pub fn select(self, exprs: Vec<Expr>) -> Self {
        LazyFrame {
            plan: LogicalPlan::Select {
                input: Box::new(self.plan),
                exprs,
            },
            streaming_budget: self.streaming_budget,
        }
    }

    pub fn with_columns(self, exprs: Vec<Expr>) -> Self {
        LazyFrame {
            plan: LogicalPlan::WithColumns {
                input: Box::new(self.plan),
                exprs,
            },
            streaming_budget: self.streaming_budget,
        }
    }

    pub fn sort(self, by: &str, options: SortOptions) -> Self {
        LazyFrame {
            plan: LogicalPlan::Sort {
                input: Box::new(self.plan),
                by_column: by.to_string(),
                options,
            },
            streaming_budget: self.streaming_budget,
        }
    }

    pub fn group_by(self, keys: Vec<Expr>) -> GroupByBuilder {
        GroupByBuilder {
            plan: self.plan,
            keys,
            streaming_budget: self.streaming_budget,
        }
    }

    pub fn join(
        self,
        other: LazyFrame,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
    ) -> Self {
        LazyFrame {
            plan: LogicalPlan::Join {
                left: Box::new(self.plan),
                right: Box::new(other.plan),
                left_on,
                right_on,
                join_type,
            },
            streaming_budget: self.streaming_budget,
        }
    }

    pub fn limit(self, n: usize) -> Self {
        LazyFrame {
            plan: LogicalPlan::Limit {
                input: Box::new(self.plan),
                n,
            },
            streaming_budget: self.streaming_budget,
        }
    }

    pub fn distinct(self) -> Self {
        LazyFrame {
            plan: LogicalPlan::Distinct {
                input: Box::new(self.plan),
            },
            streaming_budget: self.streaming_budget,
        }
    }

    /// Set max RAM for intermediate results before spilling.
    pub fn with_streaming_budget(mut self, bytes: usize) -> Self {
        self.streaming_budget = bytes;
        self
    }

    /// Execute the plan in streaming mode, writing output to a Parquet file.
    pub fn sink_parquet(self, path: &str) -> Result<usize> {
        let budget = self.streaming_budget;
        let plan = Optimizer::optimize(self.plan);
        let mut pipeline = StreamingPlanner::build_pipeline(&plan, budget)?;
        let mut sink = Box::new(ParquetSink::new(path)?);
        let source = StreamingPlanner::build_source(&plan)?;

        let mut rows = 0;
        for chunk in source {
            let chunk = chunk?;
            let outputs = pipeline.push_chunk(chunk)?;
            for out in outputs {
                rows += out.height();
                sink.write_chunk(&out)?;
            }
        }
        for out in pipeline.flush()? {
            rows += out.height();
            sink.write_chunk(&out)?;
        }
        sink.finish()?;
        Ok(rows)
    }

    /// Execute in streaming mode, writing to CSV.
    pub fn sink_csv(self, path: &str) -> Result<usize> {
        let budget = self.streaming_budget;
        let plan = Optimizer::optimize(self.plan);
        let mut pipeline = StreamingPlanner::build_pipeline(&plan, budget)?;
        let mut sink = Box::new(CsvSink::new(path)?);
        let source = StreamingPlanner::build_source(&plan)?;

        let mut rows = 0;
        for chunk in source {
            let chunk = chunk?;
            let outputs = pipeline.push_chunk(chunk)?;
            for out in outputs {
                rows += out.height();
                sink.write_chunk(&out)?;
            }
        }
        for out in pipeline.flush()? {
            rows += out.height();
            sink.write_chunk(&out)?;
        }
        sink.finish()?;
        Ok(rows)
    }

    /// Execute in streaming mode, collecting into a DataFrame.
    pub fn collect_streaming(self) -> Result<DataFrame> {
        let budget = self.streaming_budget;
        let plan = Optimizer::optimize(self.plan);
        let mut pipeline = StreamingPlanner::build_pipeline(&plan, budget)?;
        let mut sink = MemorySink::new();
        let source = StreamingPlanner::build_source(&plan)?;

        for chunk in source {
            let chunk = chunk?;
            let outputs = pipeline.push_chunk(chunk)?;
            for out in outputs {
                sink.write_chunk(&out)?;
            }
        }
        for out in pipeline.flush()? {
            sink.write_chunk(&out)?;
        }
        sink.into_dataframe()
    }

    /// Explain the streaming execution plan.
    pub fn explain_streaming(&self) -> String {
        format!("Streaming plan:\n{}", self.plan.describe(0))
    }

    /// Materialize the lazy computation into a DataFrame.
    /// Runs the optimizer before execution.
    pub fn collect(self) -> Result<DataFrame> {
        let optimized = Optimizer::optimize(self.plan);
        PhysicalExecutor::execute(optimized)
    }

    /// Explain the query plan.
    pub fn explain(self, optimized: bool) -> String {
        if optimized {
            let plan = Optimizer::optimize(self.plan);
            plan.describe(0)
        } else {
            self.plan.describe(0)
        }
    }
}

/// Builder for group-by operations.
pub struct GroupByBuilder {
    plan: LogicalPlan,
    keys: Vec<Expr>,
    streaming_budget: usize,
}

impl GroupByBuilder {
    pub fn agg(self, aggs: Vec<Expr>) -> LazyFrame {
        LazyFrame {
            plan: LogicalPlan::GroupBy {
                input: Box::new(self.plan),
                keys: self.keys,
                aggs,
            },
            streaming_budget: self.streaming_budget,
        }
    }
}

