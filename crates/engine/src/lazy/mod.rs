use crate::compute::executor::PhysicalExecutor;
use crate::compute::streaming::PartialAggStream;
use crate::compute::streaming_planner::StreamingPlanner;
use crate::dataset::collect_files;
use crate::io::parquet_dict::try_dict_distinct;
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
        let plan = Self::limit_pushdown(plan);
        let plan = Self::predicate_pushdown(plan);
        let plan = Self::projection_pushdown(plan);
        plan
    }

    /// Push Limit nodes down into DatasetScan so we never read more rows than needed.
    /// Rules:
    ///   Limit(n) over DatasetScan     → Limit(n)(DatasetScan(n_rows=n))
    ///     The Limit node is KEPT so the streaming planner's LimitStream fires.
    ///     n_rows is also set on DatasetScan so the physical executor stops early.
    ///   Limit(n) over Select(DatasetScan) → Select(DatasetScan(n_rows=n))
    ///   Limit(n) over WithColumns(...)    → WithColumns(Limit pushed further)
    ///   Limit(n) over Sort/GroupBy/Filter → keep Limit (row count may change)
    fn limit_pushdown(plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Limit { input, n } => {
                match *input {
                    // Direct dataset scan — set n_rows for physical-executor early exit
                    // AND keep the Limit node for the streaming LimitStream operator.
                    LogicalPlan::DatasetScan {
                        root, format, projection, partition_filters, row_filters, n_rows,
                    } => LogicalPlan::Limit {
                        input: Box::new(LogicalPlan::DatasetScan {
                            root,
                            format,
                            projection,
                            partition_filters,
                            row_filters,
                            n_rows: Some(n_rows.map_or(n, |existing| existing.min(n))),
                        }),
                        n,
                    },

                    // Select doesn't change row count — push through and drop Limit
                    LogicalPlan::Select { input: inner, exprs } => {
                        let pushed = Self::limit_pushdown(LogicalPlan::Limit {
                            input: inner,
                            n,
                        });
                        LogicalPlan::Select {
                            input: Box::new(pushed),
                            exprs,
                        }
                    }

                    // WithColumns doesn't change row count — push through and drop Limit
                    LogicalPlan::WithColumns { input: inner, exprs } => {
                        let pushed = Self::limit_pushdown(LogicalPlan::Limit {
                            input: inner,
                            n,
                        });
                        LogicalPlan::WithColumns {
                            input: Box::new(pushed),
                            exprs,
                        }
                    }

                    // Sort/GroupBy/Filter/Distinct can change row count or require all rows —
                    // keep the Limit node but still recurse into the child
                    other => LogicalPlan::Limit {
                        input: Box::new(Self::limit_pushdown(other)),
                        n,
                    },
                }
            }

            // Recurse into all other node types
            LogicalPlan::Select { input, exprs } => LogicalPlan::Select {
                input: Box::new(Self::limit_pushdown(*input)),
                exprs,
            },
            LogicalPlan::WithColumns { input, exprs } => LogicalPlan::WithColumns {
                input: Box::new(Self::limit_pushdown(*input)),
                exprs,
            },
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(Self::limit_pushdown(*input)),
                predicate,
            },
            LogicalPlan::Sort { input, by_column, options } => LogicalPlan::Sort {
                input: Box::new(Self::limit_pushdown(*input)),
                by_column,
                options,
            },
            LogicalPlan::GroupBy { input, keys, aggs } => LogicalPlan::GroupBy {
                input: Box::new(Self::limit_pushdown(*input)),
                keys,
                aggs,
            },
            LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(Self::limit_pushdown(*input)),
            },
            other => other,
        }
    }

    /// Push filter predicates closer to the data source.
    /// When a Filter sits directly over a DatasetScan we copy the predicate into
    /// `DatasetScan.row_filters` so the executor can perform zone-map (min/max)
    /// row-group skipping.  The Filter node is KEPT in the plan for correctness —
    /// zone-map skipping is approximate (whole row groups, not individual rows).
    fn predicate_pushdown(plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let optimized_input = Self::predicate_pushdown(*input);
                match optimized_input {
                    // Filter directly over DatasetScan → copy predicate for zone maps
                    LogicalPlan::DatasetScan {
                        root,
                        format,
                        projection,
                        partition_filters,
                        row_filters,
                        n_rows,
                    } => {
                        // AND the new predicate with any existing row_filters
                        let new_row_filters = match row_filters {
                            None => Box::new(predicate.clone()),
                            Some(existing) => Box::new(Expr::BinaryExpr {
                                left: existing,
                                op: crate::expr::BinaryOp::And,
                                right: Box::new(predicate.clone()),
                            }),
                        };
                        LogicalPlan::Filter {
                            input: Box::new(LogicalPlan::DatasetScan {
                                root,
                                format,
                                projection,
                                partition_filters,
                                row_filters: Some(new_row_filters),
                                n_rows,
                            }),
                            predicate,
                        }
                    }
                    // Everything else: keep the Filter over the (recursively optimised) child
                    other => LogicalPlan::Filter {
                        input: Box::new(other),
                        predicate,
                    },
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

    /// Push column projections down to the DatasetScan so parquet only reads
    /// the columns actually needed — unused columns cost zero I/O.
    ///
    /// `required = None`       → all columns needed, no reduction possible
    /// `required = Some(cols)` → only these columns are consumed downstream
    fn projection_pushdown(plan: LogicalPlan) -> LogicalPlan {
        Self::pushdown_with_required(plan, None)
    }

    fn pushdown_with_required(
        plan: LogicalPlan,
        required: Option<std::collections::HashSet<String>>,
    ) -> LogicalPlan {
        match plan {
            // ── Terminal: inject projection into the scan ─────────────────────
            LogicalPlan::DatasetScan {
                root,
                format,
                partition_filters,
                row_filters,
                n_rows,
                ..
            } => {
                let projection = required.map(|mut cols| {
                    let mut v: Vec<String> = cols.drain().collect();
                    v.sort_unstable();
                    v
                });
                LogicalPlan::DatasetScan {
                    root,
                    format,
                    projection,
                    partition_filters,
                    row_filters,
                    n_rows,
                }
            }

            // ── Select: output is exactly the listed expressions ──────────────
            // The required set from above doesn't matter — only the exprs matter.
            LogicalPlan::Select { input, exprs } => {
                let has_wildcard = exprs.iter().any(|e| matches!(e, Expr::Wildcard));
                let input_required = if has_wildcard {
                    None
                } else {
                    let mut cols = std::collections::HashSet::new();
                    for e in &exprs {
                        Self::collect_columns(e, &mut cols);
                    }
                    Some(cols)
                };
                LogicalPlan::Select {
                    input: Box::new(Self::pushdown_with_required(*input, input_required)),
                    exprs,
                }
            }

            // ── Filter: adds predicate columns to the required set ────────────
            LogicalPlan::Filter { input, predicate } => {
                let input_required = match required {
                    None => None,
                    Some(mut cols) => {
                        Self::collect_columns(&predicate, &mut cols);
                        Some(cols)
                    }
                };
                LogicalPlan::Filter {
                    input: Box::new(Self::pushdown_with_required(*input, input_required)),
                    predicate,
                }
            }

            // ── WithColumns: new columns are computed, not read from source ───
            LogicalPlan::WithColumns { input, exprs } => {
                let input_required = match required {
                    None => None,
                    Some(mut cols) => {
                        // Computed output names don't need to come from the source
                        for e in &exprs {
                            cols.remove(&e.output_name());
                        }
                        // But the expressions themselves reference source columns
                        for e in &exprs {
                            Self::collect_columns(e, &mut cols);
                        }
                        Some(cols)
                    }
                };
                LogicalPlan::WithColumns {
                    input: Box::new(Self::pushdown_with_required(*input, input_required)),
                    exprs,
                }
            }

            // ── Sort: requires the sort column in addition to parent's set ────
            LogicalPlan::Sort {
                input,
                by_column,
                options,
            } => {
                let input_required = match required {
                    None => None,
                    Some(mut cols) => {
                        cols.insert(by_column.clone());
                        Some(cols)
                    }
                };
                LogicalPlan::Sort {
                    input: Box::new(Self::pushdown_with_required(*input, input_required)),
                    by_column,
                    options,
                }
            }

            // ── GroupBy: output schema is fully reshapen — only need key + agg
            // source columns regardless of what the parent requires ────────────
            LogicalPlan::GroupBy { input, keys, aggs } => {
                let mut cols = std::collections::HashSet::new();
                for k in &keys {
                    Self::collect_columns(k, &mut cols);
                }
                for a in &aggs {
                    Self::collect_columns(a, &mut cols);
                }
                let input_required = if cols.is_empty() { None } else { Some(cols) };
                LogicalPlan::GroupBy {
                    input: Box::new(Self::pushdown_with_required(*input, input_required)),
                    keys,
                    aggs,
                }
            }

            // ── Pass-through: propagate required set unchanged ────────────────
            LogicalPlan::Limit { input, n } => LogicalPlan::Limit {
                input: Box::new(Self::pushdown_with_required(*input, required)),
                n,
            },

            LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(Self::pushdown_with_required(*input, required)),
            },

            // DataFrameScan / Join — already materialised, nothing to push into
            other => other,
        }
    }

    /// Recursively collect every Column name referenced inside an expression.
    fn collect_columns(expr: &Expr, cols: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::Column(name) => {
                cols.insert(name.clone());
            }
            Expr::BinaryExpr { left, right, .. } => {
                Self::collect_columns(left, cols);
                Self::collect_columns(right, cols);
            }
            Expr::Agg { input, .. } => Self::collect_columns(input, cols),
            Expr::Alias { expr, .. } => Self::collect_columns(expr, cols),
            Expr::Sort { expr, .. } => Self::collect_columns(expr, cols),
            Expr::StringExpr { input, .. } => Self::collect_columns(input, cols),
            Expr::Rolling { input, .. } => Self::collect_columns(input, cols),
            Expr::Window { input, partition_by } => {
                Self::collect_columns(input, cols);
                for e in partition_by {
                    Self::collect_columns(e, cols);
                }
            }
            Expr::Not(e) | Expr::IsNull(e) | Expr::IsNotNull(e) => {
                Self::collect_columns(e, cols);
            }
            Expr::Cast { expr, .. } => Self::collect_columns(expr, cols),
            Expr::DateExpr { input, .. } => Self::collect_columns(input, cols),
            // Literals and wildcards reference no columns
            Expr::Literal(_) | Expr::Wildcard => {}
        }
    }
}

// ── Dictionary-page DISTINCT detection ───────────────────────────────────────

/// Information extracted from a plan that is eligible for the dictionary-page
/// DISTINCT fast path.
struct DictDistinctInfo {
    files: Vec<std::path::PathBuf>,
    columns: Vec<String>,
    sort_by: Option<(String, bool)>,   // (column, descending)
    limit_n: Option<usize>,
}

/// Walk `plan` (already optimized) looking for the pattern:
///   `[Limit →] [Sort →] Distinct → [Select(col refs)] → DatasetScan(parquet, no row_filters)`
///
/// Returns `Some(DictDistinctInfo)` when eligible, `None` otherwise.
fn detect_dict_distinct(plan: &LogicalPlan) -> Option<DictDistinctInfo> {
    let mut current = plan;
    let mut limit_n: Option<usize> = None;
    let mut sort_by: Option<(String, bool)> = None;

    // Peel optional Limit then optional Sort from the outermost layers.
    loop {
        match current {
            LogicalPlan::Limit { input, n } => {
                if limit_n.is_some() {
                    return None; // multiple limits – bail
                }
                limit_n = Some(*n);
                current = input;
            }
            LogicalPlan::Sort {
                input,
                by_column,
                options,
            } => {
                if sort_by.is_some() {
                    return None; // multiple sorts – bail
                }
                sort_by = Some((by_column.clone(), options.descending));
                current = input;
            }
            _ => break,
        }
    }

    // Must see Distinct next.
    let distinct_input = match current {
        LogicalPlan::Distinct { input } => input.as_ref(),
        _ => return None,
    };

    // Below Distinct: optional Select (column refs only), then DatasetScan.
    let (scan, columns) = match distinct_input {
        LogicalPlan::Select { input, exprs } => {
            // All expressions must be bare column references.
            let cols: Option<Vec<String>> = exprs
                .iter()
                .map(|e| {
                    if let crate::expr::Expr::Column(name) = e {
                        Some(name.clone())
                    } else {
                        None
                    }
                })
                .collect();
            (input.as_ref(), cols?)
        }
        ds @ LogicalPlan::DatasetScan { .. } => {
            // No Select node — use whatever projection the DatasetScan already has.
            (ds, vec![])
        }
        _ => return None, // e.g. Filter between DatasetScan and Distinct
    };

    // The leaf must be a parquet DatasetScan with no row-level filters.
    match scan {
        LogicalPlan::DatasetScan {
            root,
            format,
            projection,
            row_filters,
            ..
        } => {
            if !matches!(format, crate::dataset::FileFormat::Parquet) {
                return None;
            }
            // Row-level filters change which values are visible — cannot use dict pages.
            if row_filters.is_some() {
                return None;
            }

            let root_path = std::path::Path::new(root);
            let files = if root_path.is_file() {
                vec![root_path.to_path_buf()]
            } else {
                collect_files(root_path, "parquet")
            };

            if files.is_empty() {
                return None;
            }

            // Determine the column list.
            let cols = if columns.is_empty() {
                // No explicit Select — require a pushed-down projection to be present.
                // (DISTINCT * on all columns is not eligible for this path.)
                projection.clone()?
            } else {
                columns
            };

            if cols.is_empty() {
                return None;
            }

            Some(DictDistinctInfo {
                files,
                columns: cols,
                sort_by,
                limit_n,
            })
        }
        _ => None,
    }
}

// ── Query result cache ────────────────────────────────────────────────────────
//
// Caches the output DataFrame of `collect_streaming` keyed on:
//   plan.describe(0)  +  for each input file: path | size_bytes | mtime_secs
//
// The first call runs the query (possibly seconds); every subsequent call with
// the same query on unchanged files returns the cached result in < 1 ms.
// The cache is automatically invalidated when any input file's mtime or size
// changes.

struct CacheEntry {
    result: DataFrame,
    /// (path, file_size_bytes, mtime_unix_secs) at cache time.
    fingerprints: Vec<(std::path::PathBuf, u64, u64)>,
}

static RESULT_CACHE: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, CacheEntry>>>
    = std::sync::OnceLock::new();

fn result_cache() -> &'static std::sync::Mutex<std::collections::HashMap<String, CacheEntry>> {
    RESULT_CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[allow(dead_code)]
fn file_mtime_secs(path: &std::path::Path) -> u64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .map(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0))
        .unwrap_or(0)
}

fn build_cache_key(plan: &LogicalPlan, files: &[std::path::PathBuf]) -> String {
    use std::fmt::Write as _;
    let mut key = plan.describe(0);
    for f in files {
        if let Ok(meta) = std::fs::metadata(f) {
            let mtime = meta.modified()
                .map(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0))
                .unwrap_or(0);
            write!(key, "\0{}|{}|{}", f.display(), meta.len(), mtime).ok();
        }
    }
    key
}

fn cache_entry_valid(entry: &CacheEntry) -> bool {
    entry.fingerprints.iter().all(|(path, expected_size, expected_mtime)| {
        std::fs::metadata(path).ok().map(|meta| {
            let mtime = meta.modified()
                .map(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0))
                .unwrap_or(0);
            meta.len() == *expected_size && mtime == *expected_mtime
        }).unwrap_or(false)
    })
}

fn collect_plan_files(plan: &LogicalPlan) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    collect_plan_files_inner(plan, &mut out);
    out
}

fn collect_plan_files_inner(plan: &LogicalPlan, out: &mut Vec<std::path::PathBuf>) {
    match plan {
        LogicalPlan::DatasetScan { root, format, .. }
            if matches!(format, crate::dataset::FileFormat::Parquet | crate::dataset::FileFormat::Csv) =>
        {
            let p = std::path::Path::new(root);
            if p.is_file() {
                out.push(p.to_path_buf());
            } else {
                out.extend(collect_files(p, "parquet"));
                out.extend(collect_files(p, "csv"));
            }
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Select { input, .. }
        | LogicalPlan::WithColumns { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::GroupBy { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. } => collect_plan_files_inner(input, out),
        LogicalPlan::Join { left, right, .. } => {
            collect_plan_files_inner(left, out);
            collect_plan_files_inner(right, out);
        }
        _ => {}
    }
}

// ── Parallel GroupBy execution ────────────────────────────────────────────────
//
// When the plan is `[Sort →] GroupBy → [transforms] → DatasetScan(parquet, ≥2 files)`,
// we can process each file independently in parallel using Rayon, compute a
// partial GroupBy per file, then merge all partial results with a final
// merge-GroupBy.  This matches DuckDB's parallel scan + partial-agg strategy
// and gives near-linear speedup with the number of files.
//
// Memory cost: O(n_groups × n_files) — typically tiny.

struct ParallelGroupByInfo {
    files: Vec<std::path::PathBuf>,
    /// Columns to read from parquet (pushed-down projection).
    projection: Option<Vec<String>>,
    /// The plan BELOW the GroupBy node (e.g. WithColumns → DatasetScan template).
    /// The DatasetScan leaf will be replaced with a DataFrameScan per sub-chunk.
    gb_input_template: LogicalPlan,
    /// GroupBy keys (used for the final merge GroupBy).
    keys: Vec<Expr>,
    /// Original GroupBy aggs (used for the per-file partial GroupBy).
    aggs: Vec<Expr>,
    /// Optional post-GroupBy sort (applied to the merged result).
    sort_by: Option<(String, bool)>,
}

/// Detect whether `plan` can be executed with parallel per-file GroupBy.
///
/// Matches: `[Sort →] GroupBy → [any transforms] → DatasetScan(parquet)`
/// Requires ≥ 2 Parquet files (single-file plans fall through to normal streaming).
fn detect_parallel_group_by(plan: &LogicalPlan) -> Option<ParallelGroupByInfo> {
    let mut current = plan;
    let mut sort_by: Option<(String, bool)> = None;

    // Peel at most one Sort from the top.
    if let LogicalPlan::Sort {
        input,
        by_column,
        options,
    } = current
    {
        sort_by = Some((by_column.clone(), options.descending));
        current = input.as_ref();
    }

    // Require GroupBy at this level.
    let (keys, aggs, gb_input) = match current {
        LogicalPlan::GroupBy { keys, aggs, input } => (keys, aggs, input.as_ref()),
        _ => return None,
    };

    // Walk down to find a parquet DatasetScan (through any transformation nodes).
    let scan = find_parquet_datasetscan(gb_input)?;
    let (root, projection) = match scan {
        LogicalPlan::DatasetScan { root, projection, .. } => (root, projection.clone()),
        _ => return None,
    };

    let root_path = std::path::Path::new(root);
    let files = if root_path.is_file() {
        vec![root_path.to_path_buf()]
    } else {
        collect_files(root_path, "parquet")
    };

    // Parallel path only pays off with multiple files.
    if files.len() < 2 {
        return None;
    }

    Some(ParallelGroupByInfo {
        files,
        projection,
        gb_input_template: gb_input.clone(), // plan below GroupBy (e.g. WithColumns → DatasetScan)
        keys: keys.clone(),
        aggs: aggs.clone(),
        sort_by,
    })
}

/// Walk `plan` looking for a `DatasetScan(Parquet)` through any chain of
/// single-input transformation nodes.  Returns `None` if a Join is encountered
/// (cross-file joins can't be parallelized this way) or if no scan is found.
fn find_parquet_datasetscan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::DatasetScan {
            format: crate::dataset::FileFormat::Parquet,
            ..
        } => Some(plan),
        // Walk through any single-input transformation node.
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Select { input, .. }
        | LogicalPlan::WithColumns { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::GroupBy { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. } => find_parquet_datasetscan(input),
        // Join or in-memory scan — cannot parallelize.
        _ => None,
    }
}

/// Recursively replace every `DatasetScan` (and `DataFrameScan`) leaf in `plan`
/// with an in-memory `DataFrameScan { df, projection: None }`.
/// Used to inject a pre-read sub-chunk DataFrame into the gb_input_template plan.
fn replace_datasetscan_with_frame_scan(plan: LogicalPlan, df: crate::dataframe::DataFrame) -> LogicalPlan {
    match plan {
        LogicalPlan::DatasetScan { .. } | LogicalPlan::DataFrameScan { .. } => {
            LogicalPlan::DataFrameScan {
                df,
                projection: None,
            }
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(replace_datasetscan_with_frame_scan(*input, df)),
            predicate,
        },
        LogicalPlan::Select { input, exprs } => LogicalPlan::Select {
            input: Box::new(replace_datasetscan_with_frame_scan(*input, df)),
            exprs,
        },
        LogicalPlan::WithColumns { input, exprs } => LogicalPlan::WithColumns {
            input: Box::new(replace_datasetscan_with_frame_scan(*input, df)),
            exprs,
        },
        LogicalPlan::Sort {
            input,
            by_column,
            options,
        } => LogicalPlan::Sort {
            input: Box::new(replace_datasetscan_with_frame_scan(*input, df)),
            by_column,
            options,
        },
        LogicalPlan::GroupBy { input, keys, aggs } => LogicalPlan::GroupBy {
            input: Box::new(replace_datasetscan_with_frame_scan(*input, df)),
            keys,
            aggs,
        },
        LogicalPlan::Limit { input, n } => LogicalPlan::Limit {
            input: Box::new(replace_datasetscan_with_frame_scan(*input, df)),
            n,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(replace_datasetscan_with_frame_scan(*input, df)),
        },
        LogicalPlan::Join { left, right, left_on, right_on, join_type } => {
            // For joins, replace only the left subtree (right is an independent source).
            LogicalPlan::Join {
                left: Box::new(replace_datasetscan_with_frame_scan(*left, df)),
                right,
                left_on,
                right_on,
                join_type,
            }
        }
    }
}

/// Execute a parallel per-file GroupBy.
///
/// Each file runs in its own rayon task. Because `rayon::current_thread_index()`
/// will be `Some()` inside these tasks, `execute_group_by` uses single-threaded
/// mode — no nested rayon. Full CPU utilisation comes from the outer `par_iter`.
fn parallel_group_by_collect(
    info: ParallelGroupByInfo,
    _budget: usize,
) -> crate::error::Result<DataFrame> {
    use rayon::prelude::*;
    use crate::io::ParquetReader;
    use crate::compute::executor::PhysicalExecutor;

    let merge_aggs = PartialAggStream::build_merge_aggs(&info.aggs);

    // ── Per-file parallel phase ─────────────────────────────────────────────
    // Each file runs in its own rayon task. Because rayon::current_thread_index()
    // will be Some() inside these tasks, execute_group_by will use single-threaded
    // mode — no nested rayon. Full CPU utilisation comes from the outer par_iter.
    let file_partials: Vec<crate::error::Result<DataFrame>> = info
        .files
        .par_iter()
        .map(|file| {
            let mut reader = ParquetReader::from_path(file)?;
            if let Some(ref proj) = info.projection {
                reader = reader.with_projection(proj.clone());
            }
            let file_df = reader.finish()?;
            if file_df.height() == 0 {
                return Ok(DataFrame::empty());
            }
            // Replace DatasetScan with the in-memory file DataFrame, then execute
            // GroupBy (includes any WithColumns transforms above the scan).
            let plan = LogicalPlan::GroupBy {
                input: Box::new(replace_datasetscan_with_frame_scan(
                    info.gb_input_template.clone(),
                    file_df,
                )),
                keys: info.keys.clone(),
                aggs: info.aggs.clone(),
            };
            PhysicalExecutor::execute(plan)
        })
        .collect();

    // ── Cross-file merge ────────────────────────────────────────────────────
    let mut all_partials: Vec<DataFrame> = Vec::with_capacity(file_partials.len());
    for res in file_partials {
        let df = res?;
        if df.height() > 0 {
            all_partials.push(df);
        }
    }
    if all_partials.is_empty() {
        return Ok(DataFrame::empty());
    }
    let mut combined = all_partials.remove(0);
    for df in all_partials {
        combined = combined.vstack(&df)?;
    }
    let merge_result = combined
        .lazy()
        .group_by(info.keys.clone())
        .agg(merge_aggs)
        .collect()?;

    // Optional sort
    if let Some((col, desc)) = info.sort_by {
        return merge_result
            .lazy()
            .sort(&col, SortOptions { descending: desc, nulls_first: false })
            .collect();
    }
    Ok(merge_result)
}

/// Execute a streaming plan, returning the collected DataFrame.
/// This is the inner implementation used by both the cache-hit-miss path and
/// the no-file-inputs path in `collect_streaming`.
fn execute_streaming_plan(plan: &LogicalPlan, budget: usize) -> Result<DataFrame> {
    // ── Dictionary-page fast path ──────────────────────────────────────────
    // When the plan is `[Limit →] [Sort →] Distinct → [Select] → DatasetScan`
    // AND every projected column is dictionary-encoded in the parquet files,
    // skip all data pages and read only the tiny dictionary pages.
    // Falls back transparently to normal streaming when any column is not
    // dict-encoded or the plan doesn't match the eligible pattern.
    if let Some(info) = detect_dict_distinct(plan) {
        match try_dict_distinct(&info.files, &info.columns)? {
            Some(mut df) => {
                // Apply post-distinct Sort if requested.
                if let Some((col, desc)) = info.sort_by {
                    df = df
                        .lazy()
                        .sort(
                            &col,
                            crate::expr::SortOptions {
                                descending: desc,
                                nulls_first: false,
                            },
                        )
                        .collect()?;
                }
                // Apply post-distinct Limit if requested.
                if let Some(n) = info.limit_n {
                    if df.height() > n {
                        let cols: Vec<_> = df
                            .columns()
                            .iter()
                            .map(|s| s.slice(0, n))
                            .collect();
                        df = DataFrame::new(cols)?;
                    }
                }
                return Ok(df);
            }
            None => {
                // Not dict-encoded — fall through to normal streaming below.
            }
        }
    }

    // ── Parallel per-file GroupBy fast path ───────────────────────────────
    // When the plan is `[Sort →] GroupBy → [transforms] → DatasetScan(parquet, ≥2 files)`,
    // fan out one streaming pipeline per file using Rayon, collect a partial
    // GroupBy result from each, then merge with a final merge-GroupBy.
    // This gives near-linear speedup with the number of files (e.g. 4 files → ~4×).
    if let Some(info) = detect_parallel_group_by(plan) {
        return parallel_group_by_collect(info, budget);
    }

    let mut pipeline = StreamingPlanner::build_pipeline(plan, budget)?;
    let mut sink = MemorySink::new();
    let source = StreamingPlanner::build_source(plan)?;

    for chunk in source {
        let chunk = chunk?;
        let outputs = pipeline.push_chunk(chunk)?;
        for out in outputs {
            sink.write_chunk(&out)?;
        }
        // Short-circuit: if a LimitStream (or any other operator) has
        // signalled it is done, stop pulling from the source.  Without
        // this break the ParquetStream iterator would keep reading every
        // row-group in every file even though we already have all the
        // rows we need.
        if pipeline.is_done() {
            break;
        }
    }
    for out in pipeline.flush()? {
        sink.write_chunk(&out)?;
    }
    sink.into_dataframe()
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

    /// Scan a Parquet file or directory as a lazy dataset.
    /// Never reads data eagerly — execution is deferred until `.collect()`.
    pub fn scan_parquet(path: &str) -> Self {
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

        // ── Cache lookup ─────────────────────────────────────────────────────
        let input_files = collect_plan_files(&plan);
        if !input_files.is_empty() {
            let cache_key = build_cache_key(&plan, &input_files);
            if let Ok(cache) = result_cache().lock() {
                if let Some(entry) = cache.get(&cache_key) {
                    if cache_entry_valid(entry) {
                        return Ok(entry.result.clone());
                    }
                }
            }
            // Cache miss — execute and store result.
            let result = execute_streaming_plan(&plan, budget)?;
            let fingerprints: Vec<_> = input_files
                .iter()
                .filter_map(|f| {
                    let meta = std::fs::metadata(f).ok()?;
                    let mtime = meta.modified()
                        .map(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0))
                        .unwrap_or(0);
                    Some((f.clone(), meta.len(), mtime))
                })
                .collect();
            if let Ok(mut cache) = result_cache().lock() {
                cache.insert(cache_key, CacheEntry { result: result.clone(), fingerprints });
            }
            return Ok(result);
        }

        // No file inputs (in-memory DataFrameScan) — skip caching.
        execute_streaming_plan(&plan, budget)
    }

    /// Clears the global streaming query result cache.
    /// After calling this, the next `collect_streaming` call for any query will
    /// re-execute the full query rather than returning a cached result.
    pub fn clear_result_cache() {
        if let Ok(mut cache) = result_cache().lock() {
            cache.clear();
        }
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

