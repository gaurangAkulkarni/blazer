use napi_derive::napi;

// Engine types aliased to avoid name conflicts with the napi structs below
use blazer_engine::dataframe::DataFrame as EngineDataFrame;
use blazer_engine::lazy::{LazyFrame as EngineLazyFrame, JoinType};
use blazer_engine::expr::{col as engine_col, lit as engine_lit, Expr as EngineExpr, SortOptions};
use blazer_engine::dtype::DataType;

fn to_napi<T>(r: blazer_engine::error::Result<T>) -> napi::Result<T> {
    r.map_err(|e| napi::Error::from_reason(e.to_string()))
}

// ──────────────────────────────── Expr ────────────────────────────────

#[napi]
pub struct Expr {
    inner: EngineExpr,
}

#[napi]
impl Expr {
    #[napi]
    pub fn sum(&self) -> Expr {
        Expr { inner: self.inner.clone().sum() }
    }

    #[napi]
    pub fn mean(&self) -> Expr {
        Expr { inner: self.inner.clone().mean() }
    }

    #[napi]
    pub fn min(&self) -> Expr {
        Expr { inner: self.inner.clone().min() }
    }

    #[napi]
    pub fn max(&self) -> Expr {
        Expr { inner: self.inner.clone().max() }
    }

    #[napi]
    pub fn count(&self) -> Expr {
        Expr { inner: self.inner.clone().count() }
    }

    #[napi(js_name = "nUnique")]
    pub fn n_unique(&self) -> Expr {
        Expr { inner: self.inner.clone().n_unique() }
    }

    #[napi]
    pub fn first(&self) -> Expr {
        Expr { inner: self.inner.clone().first() }
    }

    #[napi]
    pub fn last(&self) -> Expr {
        Expr { inner: self.inner.clone().last() }
    }

    #[napi]
    pub fn alias(&self, name: String) -> Expr {
        Expr { inner: self.inner.clone().alias(&name) }
    }

    #[napi]
    pub fn gt(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().gt(other.inner.clone()) }
    }

    #[napi]
    pub fn lt(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().lt(other.inner.clone()) }
    }

    #[napi]
    pub fn eq(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().eq(other.inner.clone()) }
    }

    #[napi]
    pub fn neq(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().neq(other.inner.clone()) }
    }

    #[napi(js_name = "gtEq")]
    pub fn gt_eq(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().gt_eq(other.inner.clone()) }
    }

    #[napi(js_name = "ltEq")]
    pub fn lt_eq(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().lt_eq(other.inner.clone()) }
    }

    #[napi]
    pub fn add(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().add(other.inner.clone()) }
    }

    #[napi]
    pub fn sub(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().sub(other.inner.clone()) }
    }

    #[napi]
    pub fn mul(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().mul(other.inner.clone()) }
    }

    #[napi]
    pub fn div(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().div(other.inner.clone()) }
    }

    #[napi]
    pub fn and(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().and(other.inner.clone()) }
    }

    #[napi]
    pub fn or(&self, other: &Expr) -> Expr {
        Expr { inner: self.inner.clone().or(other.inner.clone()) }
    }

    #[napi]
    pub fn not(&self) -> Expr {
        Expr { inner: self.inner.clone().not() }
    }

    #[napi(js_name = "isNull")]
    pub fn is_null(&self) -> Expr {
        Expr { inner: self.inner.clone().is_null() }
    }

    #[napi(js_name = "isNotNull")]
    pub fn is_not_null(&self) -> Expr {
        Expr { inner: self.inner.clone().is_not_null() }
    }

    /// Cast to a named DataType. Accepted strings: "Float64", "Float32",
    /// "Int64", "Int32", "Utf8", "Boolean".
    #[napi]
    pub fn cast(&self, dtype: String) -> napi::Result<Expr> {
        let dt = match dtype.as_str() {
            "Float64" | "f64" | "float64" => DataType::Float64,
            "Float32" | "f32" | "float32" => DataType::Float32,
            "Int64"   | "i64" | "int64"   => DataType::Int64,
            "Int32"   | "i32" | "int32"   => DataType::Int32,
            "Utf8"    | "str" | "utf8" | "String" | "string" => DataType::Utf8,
            "Boolean" | "bool" | "boolean" => DataType::Boolean,
            other => return Err(napi::Error::from_reason(
                format!("Unknown dtype '{}'. Use Float64, Float32, Int64, Int32, Utf8, or Boolean", other)
            )),
        };
        Ok(Expr { inner: self.inner.clone().cast(dt) })
    }

    #[napi(js_name = "toString")]
    pub fn to_string_js(&self) -> String {
        format!("{}", self.inner)
    }

    // ── Window / rolling ───────────────────────────────────────────────────

    /// Broadcast an aggregate over partitions (window function).
    ///
    /// Example: `col("salary").mean().over([col("dept")])`
    #[napi]
    pub fn over(&self, partition_by: Vec<&Expr>) -> Expr {
        let exprs: Vec<EngineExpr> = partition_by.into_iter().map(|e| e.inner.clone()).collect();
        Expr { inner: self.inner.clone().over(exprs) }
    }

    /// Rolling mean over `windowSize` rows.
    #[napi(js_name = "rollingMean")]
    pub fn rolling_mean(&self, window_size: u32) -> Expr {
        Expr { inner: self.inner.clone().rolling_mean(window_size as usize) }
    }

    // ── String operations ──────────────────────────────────────────────────

    /// True where the string column contains `pattern` (substring).
    #[napi(js_name = "strContains")]
    pub fn str_contains(&self, pattern: String) -> Expr {
        Expr { inner: self.inner.clone().str().contains(&pattern) }
    }

    /// True where the string column starts with `prefix`.
    #[napi(js_name = "strStartsWith")]
    pub fn str_starts_with(&self, prefix: String) -> Expr {
        Expr { inner: self.inner.clone().str().starts_with(&prefix) }
    }

    /// True where the string column ends with `suffix`.
    #[napi(js_name = "strEndsWith")]
    pub fn str_ends_with(&self, suffix: String) -> Expr {
        Expr { inner: self.inner.clone().str().ends_with(&suffix) }
    }

    /// Convert string column to UPPER CASE.
    #[napi(js_name = "strToUppercase")]
    pub fn str_to_uppercase(&self) -> Expr {
        Expr { inner: self.inner.clone().str().to_uppercase() }
    }

    /// Convert string column to lower case.
    #[napi(js_name = "strToLowercase")]
    pub fn str_to_lowercase(&self) -> Expr {
        Expr { inner: self.inner.clone().str().to_lowercase() }
    }
}

// ──────────────────────────────── LazyFrame ───────────────────────────

#[napi]
pub struct LazyFrame {
    inner: EngineLazyFrame,
}

#[napi]
impl LazyFrame {
    #[napi]
    pub fn filter(&self, predicate: &Expr) -> LazyFrame {
        LazyFrame { inner: self.inner.clone().filter(predicate.inner.clone()) }
    }

    #[napi]
    pub fn select(&self, exprs: Vec<&Expr>) -> LazyFrame {
        let exprs: Vec<EngineExpr> = exprs.into_iter().map(|e| e.inner.clone()).collect();
        LazyFrame { inner: self.inner.clone().select(exprs) }
    }

    #[napi(js_name = "withColumns")]
    pub fn with_columns(&self, exprs: Vec<&Expr>) -> LazyFrame {
        let exprs: Vec<EngineExpr> = exprs.into_iter().map(|e| e.inner.clone()).collect();
        LazyFrame { inner: self.inner.clone().with_columns(exprs) }
    }

    #[napi(js_name = "groupBy")]
    pub fn group_by(&self, keys: Vec<&Expr>, aggs: Vec<&Expr>) -> LazyFrame {
        let keys: Vec<EngineExpr> = keys.into_iter().map(|e| e.inner.clone()).collect();
        let aggs: Vec<EngineExpr> = aggs.into_iter().map(|e| e.inner.clone()).collect();
        LazyFrame { inner: self.inner.clone().group_by(keys).agg(aggs) }
    }

    #[napi]
    pub fn sort(&self, by: String, descending: Option<bool>) -> LazyFrame {
        let opts = if descending.unwrap_or(false) { SortOptions::descending() } else { SortOptions::ascending() };
        LazyFrame { inner: self.inner.clone().sort(&by, opts) }
    }

    #[napi]
    pub fn limit(&self, n: u32) -> LazyFrame {
        LazyFrame { inner: self.inner.clone().limit(n as usize) }
    }

    #[napi]
    pub fn distinct(&self) -> LazyFrame {
        LazyFrame { inner: self.inner.clone().distinct() }
    }

    /// Join with `other`.
    ///
    /// `how` must be one of `"inner"` (default), `"left"`, `"right"`,
    /// `"outer"`, or `"cross"`.
    #[napi]
    pub fn join(
        &self,
        other: &LazyFrame,
        left_on: Vec<&Expr>,
        right_on: Vec<&Expr>,
        how: Option<String>,
    ) -> napi::Result<LazyFrame> {
        let join_type = match how.as_deref().unwrap_or("inner") {
            "inner"          => JoinType::Inner,
            "left"           => JoinType::Left,
            "right"          => JoinType::Right,
            "outer" | "full" => JoinType::Outer,
            "cross"          => JoinType::Cross,
            other => return Err(napi::Error::from_reason(format!(
                "Unknown join type '{}'. Use: inner, left, right, outer, cross", other
            ))),
        };
        let left_on:  Vec<EngineExpr> = left_on.into_iter().map(|e| e.inner.clone()).collect();
        let right_on: Vec<EngineExpr> = right_on.into_iter().map(|e| e.inner.clone()).collect();
        Ok(LazyFrame {
            inner: self.inner.clone().join(other.inner.clone(), left_on, right_on, join_type),
        })
    }

    #[napi]
    pub fn collect(&self) -> napi::Result<DataFrame> {
        let df = to_napi(self.inner.clone().collect())?;
        Ok(DataFrame { inner: df })
    }

    #[napi(js_name = "collectStreaming")]
    pub fn collect_streaming(&self) -> napi::Result<DataFrame> {
        let df = to_napi(self.inner.clone().collect_streaming())?;
        Ok(DataFrame { inner: df })
    }

    #[napi]
    pub fn explain(&self, optimized: Option<bool>) -> String {
        self.inner.clone().explain(optimized.unwrap_or(true))
    }

    #[napi(js_name = "explainStreaming")]
    pub fn explain_streaming(&self) -> String {
        self.inner.explain_streaming()
    }

    #[napi(js_name = "sinkParquet")]
    pub fn sink_parquet(&self, path: String) -> napi::Result<u32> {
        let rows = to_napi(self.inner.clone().sink_parquet(&path))?;
        Ok(rows as u32)
    }

    #[napi(js_name = "sinkCsv")]
    pub fn sink_csv(&self, path: String) -> napi::Result<u32> {
        let rows = to_napi(self.inner.clone().sink_csv(&path))?;
        Ok(rows as u32)
    }

    #[napi(js_name = "withStreamingBudget")]
    pub fn with_streaming_budget(&self, bytes: u32) -> LazyFrame {
        LazyFrame { inner: self.inner.clone().with_streaming_budget(bytes as usize) }
    }
}

// ──────────────────────────────── DataFrame ───────────────────────────

#[napi]
pub struct DataFrame {
    inner: EngineDataFrame,
}

#[napi]
impl DataFrame {
    #[napi]
    pub fn height(&self) -> u32 {
        self.inner.height() as u32
    }

    #[napi]
    pub fn width(&self) -> u32 {
        self.inner.width() as u32
    }

    #[napi(js_name = "toString")]
    pub fn to_string_js(&self) -> String {
        format!("{}", self.inner)
    }

    #[napi]
    pub fn columns(&self) -> Vec<String> {
        self.inner.get_column_names().into_iter().map(|s| s.to_string()).collect()
    }

    #[napi]
    pub fn lazy(&self) -> LazyFrame {
        LazyFrame { inner: self.inner.clone().lazy() }
    }

    #[napi]
    pub fn head(&self, n: Option<u32>) -> DataFrame {
        DataFrame { inner: self.inner.head(n.unwrap_or(5) as usize) }
    }

    #[napi]
    pub fn tail(&self, n: Option<u32>) -> DataFrame {
        DataFrame { inner: self.inner.tail(n.unwrap_or(5) as usize) }
    }

    #[napi]
    pub fn sort(&self, by: String, descending: Option<bool>) -> napi::Result<DataFrame> {
        let df = to_napi(self.inner.sort(&by, descending.unwrap_or(false)))?;
        Ok(DataFrame { inner: df })
    }

    #[napi(js_name = "selectColumns")]
    pub fn select_columns(&self, names: Vec<String>) -> napi::Result<DataFrame> {
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let df = to_napi(self.inner.select_columns(&refs))?;
        Ok(DataFrame { inner: df })
    }

    #[napi(js_name = "toJSON")]
    pub fn to_json(&self) -> napi::Result<String> {
        let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        let height = self.inner.height();
        let max_rows = height.min(10_000);

        for row in 0..max_rows {
            let mut obj = serde_json::Map::new();
            for col in self.inner.columns() {
                let arr = col.to_array();
                let val = if arr.is_null(row) {
                    serde_json::Value::Null
                } else {
                    match col.dtype() {
                        DataType::Int64 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::PrimitiveArray<i64>>().unwrap();
                            serde_json::Value::Number(serde_json::Number::from(p.value(row)))
                        }
                        DataType::Int32 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::PrimitiveArray<i32>>().unwrap();
                            serde_json::Value::Number(serde_json::Number::from(p.value(row)))
                        }
                        DataType::Float64 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::PrimitiveArray<f64>>().unwrap();
                            serde_json::json!(p.value(row))
                        }
                        DataType::Float32 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::PrimitiveArray<f32>>().unwrap();
                            serde_json::json!(p.value(row) as f64)
                        }
                        DataType::Boolean => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::BooleanArray>().unwrap();
                            serde_json::Value::Bool(p.value(row))
                        }
                        DataType::Utf8 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::Utf8Array<i32>>().unwrap();
                            serde_json::Value::String(p.value(row).to_string())
                        }
                        _ => serde_json::Value::String(format!("unsupported({})", col.dtype())),
                    }
                };
                obj.insert(col.name().to_string(), val);
            }
            rows.push(obj);
        }

        serde_json::to_string(&rows).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    #[napi]
    pub fn vstack(&self, other: &DataFrame) -> napi::Result<DataFrame> {
        let df = to_napi(self.inner.vstack(&other.inner))?;
        Ok(DataFrame { inner: df })
    }

    /// Write to a Parquet file; returns the number of rows written.
    #[napi(js_name = "writeParquet")]
    pub fn write_parquet(&self, path: String) -> napi::Result<u32> {
        let rows = to_napi(self.inner.clone().lazy().sink_parquet(&path))?;
        Ok(rows as u32)
    }

    /// Write to a CSV file; returns the number of rows written.
    #[napi(js_name = "writeCsv")]
    pub fn write_csv(&self, path: String) -> napi::Result<u32> {
        let rows = to_napi(self.inner.clone().lazy().sink_csv(&path))?;
        Ok(rows as u32)
    }

    #[napi(js_name = "getSchema")]
    pub fn get_schema(&self) -> Vec<SchemaField> {
        self.inner
            .schema()
            .fields()
            .iter()
            .map(|f| SchemaField { name: f.name.clone(), dtype: format!("{}", f.dtype) })
            .collect()
    }
}

#[napi(object)]
pub struct SchemaField {
    pub name: String,
    pub dtype: String,
}

// ──────────────────────────────── Top-level functions ────────────────────

#[napi(js_name = "col")]
pub fn js_col(name: String) -> Expr {
    Expr { inner: engine_col(&name) }
}

#[napi(js_name = "litInt")]
pub fn js_lit_int(value: i64) -> Expr {
    Expr { inner: engine_lit(value) }
}

#[napi(js_name = "litFloat")]
pub fn js_lit_float(value: f64) -> Expr {
    Expr { inner: engine_lit(value) }
}

#[napi(js_name = "litStr")]
pub fn js_lit_str(value: String) -> Expr {
    Expr { inner: engine_lit(value.as_str()) }
}

#[napi(js_name = "litBool")]
pub fn js_lit_bool(value: bool) -> Expr {
    Expr { inner: engine_lit(value) }
}

#[napi(js_name = "readCsv")]
pub fn read_csv(path: String) -> napi::Result<DataFrame> {
    let df = blazer_engine::io::CsvReader::from_path(&path)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?
        .finish()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(DataFrame { inner: df })
}

#[napi(js_name = "readParquet")]
pub fn read_parquet(path: String) -> napi::Result<DataFrame> {
    let df = blazer_engine::io::ParquetReader::from_path(&path)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?
        .finish()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(DataFrame { inner: df })
}

#[napi(js_name = "scanParquet")]
pub fn scan_parquet(path: String) -> LazyFrame {
    LazyFrame { inner: EngineLazyFrame::scan_parquet(&path) }
}

/// Create a lazy scan over a CSV file. Nothing is read until `.collect()` is called.
#[napi(js_name = "scanCsv")]
pub fn scan_csv(path: String) -> LazyFrame {
    use blazer_engine::lazy::{LogicalPlan};
    use blazer_engine::dataset::FileFormat;
    LazyFrame {
        inner: EngineLazyFrame::from_plan(LogicalPlan::DatasetScan {
            root: path,
            format: FileFormat::Csv,
            projection: None,
            partition_filters: vec![],
            row_filters: None,
            n_rows: None,
        }),
    }
}

#[napi(js_name = "writeParquet")]
pub fn write_parquet(df: &DataFrame, path: String) -> napi::Result<()> {
    blazer_engine::io::ParquetWriter::from_path(&path)
        .finish(&df.inner)
        .map_err(|e| napi::Error::from_reason(e.to_string()))
}
