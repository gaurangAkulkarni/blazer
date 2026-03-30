use blazer_engine::dataframe::DataFrame;
use blazer_engine::dtype::DataType;
use blazer_engine::expr::{col as engine_col, lit as engine_lit, Expr as EngineExpr};
use blazer_engine::io::{CsvReader, ParquetReader};
use blazer_engine::lazy::LazyFrame;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::panic;

// ── JSON DSL types ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct Query {
    pub source: Source,
    #[serde(default)]
    pub ops: Vec<Op>,
}

#[derive(Debug, Deserialize)]
pub struct Source {
    #[serde(rename = "type")]
    pub kind: String, // "parquet" | "csv" | "parquet_dir"
    pub path: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    Filter {
        conditions: Vec<Condition>,
    },
    Select {
        columns: Vec<String>,
    },
    WithColumn {
        name: String,
        expr: ComputeExpr,
    },
    GroupBy {
        keys: Vec<String>,
        aggs: Vec<Agg>,
    },
    Sort {
        by: String,
        #[serde(default)]
        desc: bool,
    },
    Limit {
        n: u32,
    },
    Distinct,
}

/// A filter condition on a column.
#[derive(Debug, Deserialize)]
pub struct Condition {
    pub col: String,
    pub cast: Option<String>,
    // comparison operators (at most one present)
    pub gt: Option<Value>,
    pub lt: Option<Value>,
    pub gte: Option<Value>,
    pub lte: Option<Value>,
    pub eq: Option<Value>,
    pub neq: Option<Value>,
    pub is_null: Option<bool>,
    pub is_not_null: Option<bool>,
}

/// Aggregation spec.
#[derive(Debug, Deserialize)]
pub struct Agg {
    pub func: String, // "sum" | "mean" | "min" | "max" | "count" | "n_unique" | "first" | "last"
    pub col: String,
    pub alias: Option<String>,
}

/// Expression for computed columns (with_column).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ComputeExpr {
    // Arithmetic: {"add": [left_expr, right_expr]}
    Add { add: [Box<ComputeExpr>; 2] },
    Sub { sub: [Box<ComputeExpr>; 2] },
    Mul { mul: [Box<ComputeExpr>; 2] },
    Div { div: [Box<ComputeExpr>; 2] },
    // Column ref: {"col": "name"} or {"col": "name", "cast": "Float64"}
    Col {
        col: String,
        cast: Option<String>,
    },
    // Literals
    LitInt { lit_int: i64 },
    LitFloat { lit_float: f64 },
    LitStr { lit_str: String },
    LitBool { lit_bool: bool },
}

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct QueryResult {
    pub success: bool,
    pub error: Option<String>,
    pub data: Vec<serde_json::Map<String, Value>>,
    pub columns: Vec<String>,
    pub shape: [usize; 2],
    pub duration_ms: u64,
}

#[derive(Serialize)]
pub struct SchemaField {
    pub name: String,
    pub dtype: String,
}

// ── Commands ──────────────────────────────────────────────────────────────────

#[tauri::command]
pub fn run_query(query: Value) -> QueryResult {
    let start = std::time::Instant::now();

    let result = panic::catch_unwind(|| execute_query(query));

    let duration_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(Ok(df)) => dataframe_to_result(df, duration_ms),
        Ok(Err(e)) => QueryResult {
            success: false,
            error: Some(e),
            data: vec![],
            columns: vec![],
            shape: [0, 0],
            duration_ms,
        },
        Err(_) => QueryResult {
            success: false,
            error: Some(
                "Engine panic — likely a stack overflow or out-of-memory. \
                 Try using a parquet_dir source with .limit() to read a subset first."
                    .to_string(),
            ),
            data: vec![],
            columns: vec![],
            shape: [0, 0],
            duration_ms,
        },
    }
}

#[tauri::command]
pub fn get_schema(path: String, kind: String) -> Result<Vec<SchemaField>, String> {
    let result = panic::catch_unwind(|| infer_schema(&path, &kind));
    match result {
        Ok(Ok(fields)) => Ok(fields),
        Ok(Err(e)) => Err(e),
        Err(_) => Err("Panic while reading schema".to_string()),
    }
}

// ── Core interpreter ──────────────────────────────────────────────────────────

fn execute_query(raw: Value) -> Result<DataFrame, String> {
    let query: Query = serde_json::from_value(raw)
        .map_err(|e| format!("Invalid query JSON: {e}"))?;

    // Load source into a LazyFrame
    let mut lf = load_source(&query.source)?;

    // Apply ops
    for op in &query.ops {
        lf = apply_op(lf, op)?;
    }

    lf.collect().map_err(|e| e.to_string())
}

fn load_source(src: &Source) -> Result<LazyFrame, String> {
    match src.kind.as_str() {
        "csv" => {
            let df = CsvReader::from_path(&src.path)
                .map_err(|e| format!("CSV read error: {e}"))?
                .finish()
                .map_err(|e| format!("CSV parse error: {e}"))?;
            Ok(df.lazy())
        }
        "parquet" => {
            let df = ParquetReader::from_path(&src.path)
                .map_err(|e| format!("Parquet open error: {e}"))?
                .finish()
                .map_err(|e| format!("Parquet read error: {e}"))?;
            Ok(df.lazy())
        }
        "parquet_dir" => Ok(LazyFrame::scan_parquet(&src.path)),
        other => Err(format!("Unknown source type '{other}'. Use parquet, csv, or parquet_dir.")),
    }
}

fn apply_op(lf: LazyFrame, op: &Op) -> Result<LazyFrame, String> {
    match op {
        Op::Filter { conditions } => {
            let mut combined: Option<EngineExpr> = None;
            for cond in conditions {
                let expr = build_condition(cond)?;
                combined = Some(match combined {
                    None => expr,
                    Some(prev) => prev.and(expr),
                });
            }
            if let Some(expr) = combined {
                Ok(lf.filter(expr))
            } else {
                Ok(lf)
            }
        }

        Op::Select { columns } => {
            let exprs: Vec<EngineExpr> = columns.iter().map(|c| engine_col(c)).collect();
            Ok(lf.select(exprs))
        }

        Op::WithColumn { name, expr } => {
            let e = build_compute_expr(expr)?.alias(name);
            Ok(lf.with_columns(vec![e]))
        }

        Op::GroupBy { keys, aggs } => {
            let key_exprs: Vec<EngineExpr> = keys.iter().map(|k| engine_col(k)).collect();
            let agg_exprs: Vec<EngineExpr> = aggs
                .iter()
                .map(|a| build_agg(a))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(lf.group_by(key_exprs).agg(agg_exprs))
        }

        Op::Sort { by, desc } => {
            let opts = if *desc {
                blazer_engine::expr::SortOptions::descending()
            } else {
                blazer_engine::expr::SortOptions::ascending()
            };
            Ok(lf.sort(by, opts))
        }

        Op::Limit { n } => Ok(lf.limit(*n as usize)),

        Op::Distinct => Ok(lf.distinct()),
    }
}

fn build_condition(cond: &Condition) -> Result<EngineExpr, String> {
    let mut base = engine_col(&cond.col);

    if let Some(dtype_str) = &cond.cast {
        base = base.cast(parse_dtype(dtype_str)?);
    }

    if let Some(true) = cond.is_null {
        return Ok(base.is_null());
    }
    if let Some(true) = cond.is_not_null {
        return Ok(base.is_not_null());
    }

    if let Some(v) = &cond.gt {
        return Ok(base.gt(json_to_lit(v)?));
    }
    if let Some(v) = &cond.lt {
        return Ok(base.lt(json_to_lit(v)?));
    }
    if let Some(v) = &cond.gte {
        return Ok(base.gt_eq(json_to_lit(v)?));
    }
    if let Some(v) = &cond.lte {
        return Ok(base.lt_eq(json_to_lit(v)?));
    }
    if let Some(v) = &cond.eq {
        return Ok(base.eq(json_to_lit(v)?));
    }
    if let Some(v) = &cond.neq {
        return Ok(base.neq(json_to_lit(v)?));
    }

    Err(format!("Condition on '{}' has no comparison operator", cond.col))
}

fn build_agg(agg: &Agg) -> Result<EngineExpr, String> {
    let base = engine_col(&agg.col);
    let expr = match agg.func.as_str() {
        "sum" => base.sum(),
        "mean" | "avg" => base.mean(),
        "min" => base.min(),
        "max" => base.max(),
        "count" => base.count(),
        "n_unique" => base.n_unique(),
        "first" => base.first(),
        "last" => base.last(),
        other => return Err(format!("Unknown agg func '{other}'. Use: sum, mean, min, max, count, n_unique, first, last")),
    };
    Ok(if let Some(alias) = &agg.alias {
        expr.alias(alias)
    } else {
        expr
    })
}

fn build_compute_expr(expr: &ComputeExpr) -> Result<EngineExpr, String> {
    match expr {
        ComputeExpr::Col { col, cast } => {
            let mut e = engine_col(col);
            if let Some(dtype_str) = cast {
                e = e.cast(parse_dtype(dtype_str)?);
            }
            Ok(e)
        }
        ComputeExpr::LitInt { lit_int } => Ok(engine_lit(*lit_int)),
        ComputeExpr::LitFloat { lit_float } => Ok(engine_lit(*lit_float)),
        ComputeExpr::LitStr { lit_str } => Ok(engine_lit(lit_str.as_str())),
        ComputeExpr::LitBool { lit_bool } => Ok(engine_lit(*lit_bool)),
        ComputeExpr::Add { add } => {
            Ok(build_compute_expr(&add[0])?.add(build_compute_expr(&add[1])?))
        }
        ComputeExpr::Sub { sub } => {
            Ok(build_compute_expr(&sub[0])?.sub(build_compute_expr(&sub[1])?))
        }
        ComputeExpr::Mul { mul } => {
            Ok(build_compute_expr(&mul[0])?.mul(build_compute_expr(&mul[1])?))
        }
        ComputeExpr::Div { div } => {
            Ok(build_compute_expr(&div[0])?.div(build_compute_expr(&div[1])?))
        }
    }
}

fn json_to_lit(v: &Value) -> Result<EngineExpr, String> {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(engine_lit(i))
            } else if let Some(f) = n.as_f64() {
                Ok(engine_lit(f))
            } else {
                Err(format!("Cannot convert number {n} to literal"))
            }
        }
        Value::String(s) => Ok(engine_lit(s.as_str())),
        Value::Bool(b) => Ok(engine_lit(*b)),
        Value::Null => Err("Cannot use null as a comparison value".to_string()),
        other => Err(format!("Cannot use {other} as a literal")),
    }
}

fn parse_dtype(s: &str) -> Result<DataType, String> {
    match s {
        "Float64" | "f64" | "float64" => Ok(DataType::Float64),
        "Float32" | "f32" | "float32" => Ok(DataType::Float32),
        "Int64" | "i64" | "int64" => Ok(DataType::Int64),
        "Int32" | "i32" | "int32" => Ok(DataType::Int32),
        "Utf8" | "str" | "utf8" | "String" | "string" => Ok(DataType::Utf8),
        "Boolean" | "bool" | "boolean" => Ok(DataType::Boolean),
        other => Err(format!("Unknown dtype '{other}'")),
    }
}

fn infer_schema(path: &str, kind: &str) -> Result<Vec<SchemaField>, String> {
    let df = match kind {
        "csv" => CsvReader::from_path(path)
            .map_err(|e| e.to_string())?
            .finish()
            .map_err(|e| e.to_string())?,
        "parquet" => ParquetReader::from_path(path)
            .map_err(|e| e.to_string())?
            .with_n_rows(0)
            .finish()
            .map_err(|e| e.to_string())?,
        "parquet_dir" => LazyFrame::scan_parquet(path)
            .limit(0)
            .collect()
            .map_err(|e| e.to_string())?,
        _ => return Err(format!("Unknown source type '{kind}'")),
    };

    Ok(df
        .schema()
        .fields()
        .iter()
        .map(|f| SchemaField {
            name: f.name.clone(),
            dtype: format!("{}", f.dtype),
        })
        .collect())
}

// ── DataFrame → JSON ──────────────────────────────────────────────────────────

fn dataframe_to_result(df: DataFrame, duration_ms: u64) -> QueryResult {
    let height = df.height();
    let width = df.width();
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    let max_rows = height.min(10_000);
    let mut data: Vec<serde_json::Map<String, Value>> = Vec::with_capacity(max_rows);

    for row in 0..max_rows {
        let mut obj = serde_json::Map::new();
        for col in df.columns() {
            let arr = col.to_array();
            let val = if arr.is_null(row) {
                Value::Null
            } else {
                match col.dtype() {
                    DataType::Int64 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<i64>>()
                            .unwrap();
                        Value::Number(serde_json::Number::from(p.value(row)))
                    }
                    DataType::Int32 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<i32>>()
                            .unwrap();
                        Value::Number(serde_json::Number::from(p.value(row)))
                    }
                    DataType::Float64 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<f64>>()
                            .unwrap();
                        serde_json::json!(p.value(row))
                    }
                    DataType::Float32 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<f32>>()
                            .unwrap();
                        serde_json::json!(p.value(row) as f64)
                    }
                    DataType::Boolean => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<arrow2::array::BooleanArray>()
                            .unwrap();
                        Value::Bool(p.value(row))
                    }
                    DataType::Utf8 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<arrow2::array::Utf8Array<i32>>()
                            .unwrap();
                        Value::String(p.value(row).to_string())
                    }
                    _ => Value::String(format!("unsupported({})", col.dtype())),
                }
            };
            obj.insert(col.name().to_string(), val);
        }
        data.push(obj);
    }

    QueryResult {
        success: true,
        error: None,
        data,
        columns,
        shape: [height, width],
        duration_ms,
    }
}
