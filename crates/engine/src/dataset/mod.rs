use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{BlazeError, Result};
use crate::expr::{Expr, LitValue, BinaryOp};
use crate::lazy::LazyFrame;
use crate::schema::Schema;

/// File format of the dataset.
#[derive(Debug, Clone)]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
}

/// A column that is encoded in the directory structure (Hive partitioning).
#[derive(Debug, Clone)]
pub struct PartitionColumn {
    pub name: String,
    pub dtype: crate::dtype::DataType,
}

/// A lazy, partitioned dataset. Never loads data until .collect() or .sink_*().
pub struct Dataset {
    pub root: PathBuf,
    pub format: FileFormat,
    pub schema: Option<Schema>,
    pub partitions: Vec<PartitionColumn>,
}

impl Dataset {
    /// Scan a directory of Parquet files (flat or Hive-partitioned).
    pub fn scan_parquet(root: &str) -> Result<Self> {
        let root_path = PathBuf::from(root);
        if !root_path.exists() {
            return Err(BlazeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Dataset root not found: {}", root),
            )));
        }

        let files = collect_files(&root_path, "parquet");
        let partitions = detect_partitions(&root_path, &files);

        let schema = if let Some(first_file) = files.first() {
            let metadata = crate::io::read_parquet_metadata(
                &mut std::io::BufReader::new(std::fs::File::open(first_file)?),
            )?;
            Some(crate::io::infer_parquet_schema(&metadata)?)
        } else {
            None
        };

        Ok(Dataset {
            root: root_path,
            format: FileFormat::Parquet,
            schema,
            partitions,
        })
    }

    /// Scan a directory of CSV files.
    pub fn scan_csv(root: &str) -> Result<Self> {
        let root_path = PathBuf::from(root);
        if !root_path.exists() {
            return Err(BlazeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Dataset root not found: {}", root),
            )));
        }

        Ok(Dataset {
            root: root_path,
            format: FileFormat::Csv,
            schema: None,
            partitions: Vec::new(),
        })
    }

    /// List all matching file paths after applying partition filters.
    pub fn matching_files(&self, partition_filters: &[Expr]) -> Result<Vec<PathBuf>> {
        let ext = match self.format {
            FileFormat::Parquet => "parquet",
            FileFormat::Csv => "csv",
            FileFormat::Json => "json",
        };

        let all_files = collect_files(&self.root, ext);

        if partition_filters.is_empty() || self.partitions.is_empty() {
            return Ok(all_files);
        }

        let mut matching = Vec::new();
        for file in all_files {
            let rel = file.strip_prefix(&self.root).unwrap_or(&file);
            let segments = extract_partition_values(rel);

            let mut passes = true;
            for filter in partition_filters {
                if !partition_matches(filter, &segments) {
                    passes = false;
                    break;
                }
            }

            if passes {
                matching.push(file);
            }
        }

        Ok(matching)
    }

    /// Entry point into the lazy API.
    pub fn lazy(&self) -> LazyFrame {
        LazyFrame::from_plan(crate::lazy::LogicalPlan::DatasetScan {
            root: self.root.to_string_lossy().to_string(),
            format: self.format.clone(),
            projection: None,
            partition_filters: Vec::new(),
            row_filters: None,
            n_rows: None,
        })
    }

    /// Infer schema from the first file found.
    pub fn infer_schema(&mut self) -> Result<&Schema> {
        if self.schema.is_none() {
            let ext = match self.format {
                FileFormat::Parquet => "parquet",
                FileFormat::Csv => "csv",
                FileFormat::Json => "json",
            };
            let files = collect_files(&self.root, ext);
            if let Some(first) = files.first() {
                let metadata = crate::io::read_parquet_metadata(
                    &mut std::io::BufReader::new(std::fs::File::open(first)?),
                )?;
                self.schema = Some(crate::io::infer_parquet_schema(&metadata)?);
            }
        }
        self.schema
            .as_ref()
            .ok_or_else(|| BlazeError::Other("No files found to infer schema".into()))
    }
}

/// Walk a directory tree and collect all files matching an extension.
pub fn collect_files(root: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_files_recursive(root, ext, &mut files);
    files.sort();
    files
}

fn collect_files_recursive(dir: &Path, ext: &str, files: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_files_recursive(&path, ext, files);
            } else if path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e == ext)
                .unwrap_or(false)
            {
                files.push(path);
            }
        }
    }
}

/// Parse Hive partition values from a path segment like "year=2024".
pub fn parse_partition_segment(seg: &str) -> Option<(String, String)> {
    let mut parts = seg.splitn(2, '=');
    let key = parts.next()?;
    let value = parts.next()?;
    if key.is_empty() || value.is_empty() {
        return None;
    }
    Some((key.to_string(), value.to_string()))
}

/// Detect partition columns by scanning directory names.
fn detect_partitions(root: &Path, files: &[PathBuf]) -> Vec<PartitionColumn> {
    let mut partition_names: Vec<String> = Vec::new();

    for file in files {
        if let Ok(rel) = file.strip_prefix(root) {
            for component in rel.parent().iter().flat_map(|p| p.components()) {
                if let std::path::Component::Normal(seg) = component {
                    if let Some((name, _)) = parse_partition_segment(seg.to_str().unwrap_or("")) {
                        if !partition_names.contains(&name) {
                            partition_names.push(name);
                        }
                    }
                }
            }
        }
    }

    partition_names
        .into_iter()
        .map(|name| PartitionColumn {
            name,
            dtype: crate::dtype::DataType::Utf8, // Default; actual type inferred from values
        })
        .collect()
}

/// Extract partition key=value pairs from a relative file path.
fn extract_partition_values(rel_path: &Path) -> Vec<(String, String)> {
    let mut values = Vec::new();
    if let Some(parent) = rel_path.parent() {
        for component in parent.components() {
            if let std::path::Component::Normal(seg) = component {
                if let Some(kv) = parse_partition_segment(seg.to_str().unwrap_or("")) {
                    values.push(kv);
                }
            }
        }
    }
    values
}

/// Check if a partition filter matches the given partition values.
fn partition_matches(filter: &Expr, segments: &[(String, String)]) -> bool {
    match filter {
        Expr::BinaryExpr { left, op, right } => {
            // Handle logical AND/OR first
            if *op == BinaryOp::And {
                return partition_matches(left, segments) && partition_matches(right, segments);
            }
            if *op == BinaryOp::Or {
                return partition_matches(left, segments) || partition_matches(right, segments);
            }

            // Extract column name from left
            let col_name = match left.as_ref() {
                Expr::Column(name) => name.as_str(),
                _ => return true, // Can't evaluate; assume match
            };

            // Find partition value for this column
            let part_value = segments.iter().find(|(k, _)| k == col_name);
            let part_value = match part_value {
                Some((_, v)) => v,
                None => return true, // Column not a partition key; assume match
            };

            // Extract literal value from right
            match right.as_ref() {
                Expr::Literal(LitValue::Int64(v)) => {
                    let pv: i64 = part_value.parse().unwrap_or(0);
                    match op {
                        BinaryOp::Eq => pv == *v,
                        BinaryOp::NotEq => pv != *v,
                        BinaryOp::Lt => pv < *v,
                        BinaryOp::LtEq => pv <= *v,
                        BinaryOp::Gt => pv > *v,
                        BinaryOp::GtEq => pv >= *v,
                        _ => true,
                    }
                }
                Expr::Literal(LitValue::Utf8(v)) => {
                    match op {
                        BinaryOp::Eq => part_value == v,
                        BinaryOp::NotEq => part_value != v,
                        _ => true,
                    }
                }
                Expr::Literal(LitValue::Float64(v)) => {
                    let pv: f64 = part_value.parse().unwrap_or(0.0);
                    match op {
                        BinaryOp::Eq => (pv - v).abs() < f64::EPSILON,
                        BinaryOp::Lt => pv < *v,
                        BinaryOp::Gt => pv > *v,
                        _ => true,
                    }
                }
                _ => true,
            }
        }
        _ => true,
    }
}
