use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList};
use arrow2::array::{Array, BooleanArray, PrimitiveArray, Utf8Array};

use blazer_engine::dataset::FileFormat;
use blazer_engine::dtype::DataType;
use blazer_engine::lazy::{JoinType, LogicalPlan};
use blazer_engine::prelude::{DataFrame, Expr, LazyFrame, Series, SortOptions};

// ── Error helper ──────────────────────────────────────────────────────────────

#[inline]
fn be(e: blazer_engine::error::BlazeError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

// ── DataType helpers ──────────────────────────────────────────────────────────

fn dtype_from_str(s: &str) -> PyResult<DataType> {
    match s {
        "Boolean" | "bool"       => Ok(DataType::Boolean),
        "Int8"    | "int8"       => Ok(DataType::Int8),
        "Int16"   | "int16"      => Ok(DataType::Int16),
        "Int32"   | "int32"      => Ok(DataType::Int32),
        "Int64"   | "int64"      => Ok(DataType::Int64),
        "UInt8"   | "uint8"      => Ok(DataType::UInt8),
        "UInt16"  | "uint16"     => Ok(DataType::UInt16),
        "UInt32"  | "uint32"     => Ok(DataType::UInt32),
        "UInt64"  | "uint64"     => Ok(DataType::UInt64),
        "Float32" | "float32"    => Ok(DataType::Float32),
        "Float64" | "float64"    => Ok(DataType::Float64),
        "Utf8"    | "str" | "String" | "utf8" => Ok(DataType::Utf8),
        "Date32"  | "date32"     => Ok(DataType::Date32),
        "Date64"  | "date64"     => Ok(DataType::Date64),
        other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!(
                "Unknown dtype '{}'. Use: Boolean, Int8/16/32/64, UInt8/16/32/64, \
                 Float32/64, Utf8, Date32/64",
                other
            ),
        )),
    }
}

fn dtype_to_str(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean   => "Boolean",
        DataType::Int8      => "Int8",
        DataType::Int16     => "Int16",
        DataType::Int32     => "Int32",
        DataType::Int64     => "Int64",
        DataType::UInt8     => "UInt8",
        DataType::UInt16    => "UInt16",
        DataType::UInt32    => "UInt32",
        DataType::UInt64    => "UInt64",
        DataType::Float32   => "Float32",
        DataType::Float64   => "Float64",
        DataType::Utf8      => "Utf8",
        DataType::LargeUtf8 => "Utf8",
        DataType::Date32    => "Date32",
        DataType::Date64    => "Date64",
        DataType::Timestamp => "Timestamp",
        _                   => "Unknown",
    }
}

// ── Series → Python list ──────────────────────────────────────────────────────

/// Convert a blazer `Series` into a Python list, preserving nulls as `None`.
fn series_to_pylist(py: Python<'_>, series: &Series) -> PyResult<Py<PyList>> {
    let arr = series.to_array();
    let len = arr.len();
    let list = PyList::empty_bound(py);

    macro_rules! push_primitive {
        ($T:ty, $cast:ty) => {{
            let a = arr.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap();
            for i in 0..len {
                if a.is_null(i) {
                    list.append(py.None())?;
                } else {
                    list.append(a.value(i) as $cast)?;
                }
            }
        }};
    }

    match series.dtype() {
        DataType::Int64  => push_primitive!(i64, i64),
        DataType::Int32  => push_primitive!(i32, i64),
        DataType::Int16  => push_primitive!(i16, i64),
        DataType::Int8   => push_primitive!(i8,  i64),
        DataType::UInt64 => push_primitive!(u64, u64),
        DataType::UInt32 => push_primitive!(u32, u64),
        DataType::UInt16 => push_primitive!(u16, u64),
        DataType::UInt8  => push_primitive!(u8,  u64),
        DataType::Float64 => push_primitive!(f64, f64),
        DataType::Float32 => push_primitive!(f32, f64),
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            for i in 0..len {
                if a.is_null(i) {
                    list.append(py.None())?;
                } else {
                    list.append(a.value(i))?;
                }
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            if let Some(a) = arr.as_any().downcast_ref::<Utf8Array<i32>>() {
                for i in 0..len {
                    if a.is_null(i) {
                        list.append(py.None())?;
                    } else {
                        list.append(a.value(i))?;
                    }
                }
            } else if let Some(a) = arr.as_any().downcast_ref::<Utf8Array<i64>>() {
                for i in 0..len {
                    if a.is_null(i) {
                        list.append(py.None())?;
                    } else {
                        list.append(a.value(i))?;
                    }
                }
            }
        }
        _ => {
            // Fallback: emit None for unknown types
            for _ in 0..len {
                list.append(py.None())?;
            }
        }
    }

    Ok(list.unbind())
}

// ── PyDataFrame ───────────────────────────────────────────────────────────────

/// A blazer DataFrame: an immutable, columnar in-memory table.
///
/// Create from a dict of lists::
///
///     df = blazer.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
///
/// Or by reading a file::
///
///     df = blazer.read_parquet("/path/to/file.parquet")
///     df = blazer.read_csv("/path/to/file.csv")
#[pyclass(name = "DataFrame")]
#[derive(Clone)]
struct PyDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    /// Construct a DataFrame from a ``dict[str, list]``.
    ///
    /// Supported element types: ``int``, ``float``, ``str``, ``bool``.
    /// Mixed-type lists raise ``TypeError``.
    #[new]
    fn new(data: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dict = data.downcast::<PyDict>()?;
        let mut columns = Vec::new();

        for (key, value) in dict.iter() {
            let name: String = key.extract()?;
            let list = value.downcast::<PyList>()?;
            if list.is_empty() {
                continue;
            }
            let first = list.get_item(0)?;

            // Check bool FIRST: Python bool is a subclass of int, so
            // `extract::<i64>()` would succeed for `True`/`False` unless we
            // test is_instance first.
            if first.is_instance_of::<PyBool>() {
                let values: Vec<bool> = list.extract()?;
                columns.push(Series::from_bool(&name, values));
            } else if first.extract::<i64>().is_ok() {
                let values: Vec<i64> = list.extract()?;
                columns.push(Series::from_i64(&name, values));
            } else if first.extract::<f64>().is_ok() {
                let values: Vec<f64> = list.extract()?;
                columns.push(Series::from_f64(&name, values));
            } else if first.extract::<String>().is_ok() {
                let values: Vec<String> = list.extract()?;
                let str_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                columns.push(Series::from_str(&name, str_refs));
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Column '{}': unsupported element type. Use int, float, str, or bool.",
                    name
                )));
            }
        }

        let df = DataFrame::new(columns).map_err(be)?;
        Ok(PyDataFrame { inner: df })
    }

    /// Number of rows.
    fn height(&self) -> usize {
        self.inner.height()
    }

    /// Number of columns.
    fn width(&self) -> usize {
        self.inner.width()
    }

    /// Ordered list of column names.
    fn columns(&self) -> Vec<String> {
        self.inner.get_column_names().iter().map(|s| s.to_string()).collect()
    }

    /// Schema as a list of ``{"name": str, "dtype": str}`` dicts.
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for field in self.inner.schema().fields().iter() {
            let d = PyDict::new_bound(py);
            d.set_item("name", &field.name)?;
            d.set_item("dtype", dtype_to_str(&field.dtype))?;
            list.append(d)?;
        }
        Ok(list.unbind())
    }

    /// Return the first *n* rows (default 5).
    #[pyo3(signature = (n = 5))]
    fn head(&self, n: usize) -> PyDataFrame {
        PyDataFrame { inner: self.inner.head(n) }
    }

    /// Return the last *n* rows (default 5).
    #[pyo3(signature = (n = 5))]
    fn tail(&self, n: usize) -> PyDataFrame {
        PyDataFrame { inner: self.inner.tail(n) }
    }

    /// Sort by a column.
    #[pyo3(signature = (by, descending = false))]
    fn sort(&self, by: &str, descending: bool) -> PyResult<PyDataFrame> {
        let df = self.inner.sort(by, descending).map_err(be)?;
        Ok(PyDataFrame { inner: df })
    }

    /// Keep only the listed columns (by name).
    fn select_columns(&self, names: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let df = self.inner.select_columns(&refs).map_err(be)?;
        Ok(PyDataFrame { inner: df })
    }

    /// Convert to a ``dict[str, list]``, with ``None`` for null values.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new_bound(py);
        for col in self.inner.columns() {
            d.set_item(col.name(), series_to_pylist(py, col)?)?;
        }
        Ok(d.unbind())
    }

    /// Vertically stack (append rows of) another DataFrame with the same schema.
    fn vstack(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self.inner.vstack(&other.inner).map_err(be)?;
        Ok(PyDataFrame { inner: df })
    }

    /// Enter lazy mode — build a query plan without executing.
    fn lazy(&self) -> PyLazyFrame {
        PyLazyFrame { inner: self.inner.clone().lazy() }
    }

    /// Write to Parquet; returns the number of rows written.
    fn write_parquet(&self, path: &str) -> PyResult<usize> {
        self.inner.clone().lazy().sink_parquet(path).map_err(be)
    }

    /// Write to CSV; returns the number of rows written.
    fn write_csv(&self, path: &str) -> PyResult<usize> {
        self.inner.clone().lazy().sink_csv(path).map_err(be)
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.height()
    }
}

// ── PyLazyFrame ───────────────────────────────────────────────────────────────

/// A blazer LazyFrame: a query plan that executes only when ``.collect()``
/// (or ``.sink_*``) is called.
///
/// Example::
///
///     result = (
///         blazer.scan_parquet("/data/sales.parquet")
///             .filter(blazer.col("amount") > 100)
///             .group_by([blazer.col("region")], [blazer.col("amount").sum().alias("total")])
///             .sort("total", descending=True)
///             .collect()
///     )
#[pyclass(name = "LazyFrame")]
#[derive(Clone)]
struct PyLazyFrame {
    inner: LazyFrame,
}

#[pymethods]
impl PyLazyFrame {
    /// Keep only rows where *predicate* is true.
    fn filter(&self, predicate: PyExpr) -> Self {
        PyLazyFrame { inner: self.inner.clone().filter(predicate.inner) }
    }

    /// Project (select) a list of expressions as the output columns.
    fn select(&self, exprs: Vec<PyExpr>) -> Self {
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.inner).collect();
        PyLazyFrame { inner: self.inner.clone().select(exprs) }
    }

    /// Add or overwrite columns without dropping existing ones.
    fn with_columns(&self, exprs: Vec<PyExpr>) -> Self {
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.inner).collect();
        PyLazyFrame { inner: self.inner.clone().with_columns(exprs) }
    }

    /// Group by *keys* and aggregate with *aggs*.
    ///
    /// Example::
    ///
    ///     lf.group_by([col("dept")], [col("salary").mean().alias("avg_salary")])
    #[pyo3(signature = (keys, aggs))]
    fn group_by(&self, keys: Vec<PyExpr>, aggs: Vec<PyExpr>) -> Self {
        let keys: Vec<Expr> = keys.into_iter().map(|e| e.inner).collect();
        let aggs: Vec<Expr> = aggs.into_iter().map(|e| e.inner).collect();
        PyLazyFrame { inner: self.inner.clone().group_by(keys).agg(aggs) }
    }

    /// Sort by a column name.
    #[pyo3(signature = (by, descending = false))]
    fn sort(&self, by: &str, descending: bool) -> Self {
        let opts = if descending { SortOptions::descending() } else { SortOptions::ascending() };
        PyLazyFrame { inner: self.inner.clone().sort(by, opts) }
    }

    /// Keep at most *n* rows.
    fn limit(&self, n: usize) -> Self {
        PyLazyFrame { inner: self.inner.clone().limit(n) }
    }

    /// Drop duplicate rows.
    fn distinct(&self) -> Self {
        PyLazyFrame { inner: self.inner.clone().distinct() }
    }

    /// Join with *other*.
    ///
    /// *how* must be one of ``"inner"`` (default), ``"left"``, ``"right"``,
    /// ``"outer"``, or ``"cross"``.
    #[pyo3(signature = (other, left_on, right_on, how = "inner"))]
    fn join(
        &self,
        other: PyLazyFrame,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        how: &str,
    ) -> PyResult<Self> {
        let join_type = match how {
            "inner"          => JoinType::Inner,
            "left"           => JoinType::Left,
            "right"          => JoinType::Right,
            "outer" | "full" => JoinType::Outer,
            "cross"          => JoinType::Cross,
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unknown join type '{}'. Use: inner, left, right, outer, cross",
                    other
                )))
            }
        };
        let left_on: Vec<Expr> = left_on.into_iter().map(|e| e.inner).collect();
        let right_on: Vec<Expr> = right_on.into_iter().map(|e| e.inner).collect();
        Ok(PyLazyFrame {
            inner: self.inner.clone().join(other.inner, left_on, right_on, join_type),
        })
    }

    /// Execute the query plan and materialise the result.
    fn collect(&self) -> PyResult<PyDataFrame> {
        let df = self.inner.clone().collect().map_err(be)?;
        Ok(PyDataFrame { inner: df })
    }

    /// Execute using the streaming (constant-memory) engine.
    fn collect_streaming(&self) -> PyResult<PyDataFrame> {
        let df = self.inner.clone().collect_streaming().map_err(be)?;
        Ok(PyDataFrame { inner: df })
    }

    /// Stream results directly to a Parquet file; returns rows written.
    fn sink_parquet(&self, path: &str) -> PyResult<usize> {
        self.inner.clone().sink_parquet(path).map_err(be)
    }

    /// Stream results directly to a CSV file; returns rows written.
    fn sink_csv(&self, path: &str) -> PyResult<usize> {
        self.inner.clone().sink_csv(path).map_err(be)
    }

    /// Set the RAM budget (bytes) for the streaming engine.
    fn with_streaming_budget(&self, bytes: usize) -> Self {
        PyLazyFrame { inner: self.inner.clone().with_streaming_budget(bytes) }
    }

    /// Return a human-readable query plan.
    ///
    /// Pass ``optimized=False`` to see the un-optimised plan.
    #[pyo3(signature = (optimized = true))]
    fn explain(&self, optimized: bool) -> String {
        self.inner.clone().explain(optimized)
    }

    /// Return a human-readable streaming query plan.
    fn explain_streaming(&self) -> String {
        self.inner.explain_streaming()
    }

    fn __repr__(&self) -> String {
        format!("LazyFrame[\n{}\n]", self.inner.clone().explain(false))
    }
}

// ── PyExpr ────────────────────────────────────────────────────────────────────

/// A blazer expression: a lazy description of a computation over columns.
///
/// Expressions compose::
///
///     (col("price") * col("qty")).alias("revenue")
///     col("age").gt(30) & col("active").eq(True)
///     col("name").str_to_uppercase()
#[pyclass(name = "Expr")]
#[derive(Clone)]
struct PyExpr {
    inner: Expr,
}

#[pymethods]
impl PyExpr {
    // ── Naming ───────────────────────────────────────────────────────────────
    /// Rename the expression result.
    fn alias(&self, name: &str) -> PyExpr {
        PyExpr { inner: self.inner.clone().alias(name) }
    }

    // ── Aggregations ─────────────────────────────────────────────────────────
    fn sum(&self)      -> PyExpr { PyExpr { inner: self.inner.clone().sum() } }
    fn mean(&self)     -> PyExpr { PyExpr { inner: self.inner.clone().mean() } }
    fn min(&self)      -> PyExpr { PyExpr { inner: self.inner.clone().min() } }
    fn max(&self)      -> PyExpr { PyExpr { inner: self.inner.clone().max() } }
    fn count(&self)    -> PyExpr { PyExpr { inner: self.inner.clone().count() } }
    fn n_unique(&self) -> PyExpr { PyExpr { inner: self.inner.clone().n_unique() } }
    fn first(&self)    -> PyExpr { PyExpr { inner: self.inner.clone().first() } }
    fn last(&self)     -> PyExpr { PyExpr { inner: self.inner.clone().last() } }

    // ── Comparison operators ─────────────────────────────────────────────────
    fn __gt__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().gt(other.into_expr()) }
    }
    fn __lt__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().lt(other.into_expr()) }
    }
    fn __eq__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().eq(other.into_expr()) }
    }
    fn __ne__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().neq(other.into_expr()) }
    }
    fn __le__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().lt_eq(other.into_expr()) }
    }
    fn __ge__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().gt_eq(other.into_expr()) }
    }

    // ── Arithmetic operators ─────────────────────────────────────────────────
    fn __add__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().add(other.into_expr()) }
    }
    fn __sub__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().sub(other.into_expr()) }
    }
    fn __mul__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().mul(other.into_expr()) }
    }
    fn __truediv__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: self.inner.clone().div(other.into_expr()) }
    }
    fn __radd__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: other.into_expr().add(self.inner.clone()) }
    }
    fn __rsub__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: other.into_expr().sub(self.inner.clone()) }
    }
    fn __rmul__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr { inner: other.into_expr().mul(self.inner.clone()) }
    }

    // ── Logical operators ────────────────────────────────────────────────────
    fn __and__(&self, other: PyExpr) -> PyExpr {
        PyExpr { inner: self.inner.clone().and(other.inner) }
    }
    fn __or__(&self, other: PyExpr) -> PyExpr {
        PyExpr { inner: self.inner.clone().or(other.inner) }
    }
    fn __invert__(&self) -> PyExpr {
        PyExpr { inner: self.inner.clone().not() }
    }

    // ── Null checks ──────────────────────────────────────────────────────────
    fn is_null(&self)     -> PyExpr { PyExpr { inner: self.inner.clone().is_null() } }
    fn is_not_null(&self) -> PyExpr { PyExpr { inner: self.inner.clone().is_not_null() } }

    // ── Type casting ─────────────────────────────────────────────────────────
    /// Cast to *dtype* (e.g. ``"Float64"``, ``"Int32"``, ``"Utf8"``).
    fn cast(&self, dtype: &str) -> PyResult<PyExpr> {
        let dt = dtype_from_str(dtype)?;
        Ok(PyExpr { inner: self.inner.clone().cast(dt) })
    }

    // ── Window & rolling ─────────────────────────────────────────────────────
    /// Broadcast an aggregate over partitions (window function).
    ///
    /// Example::
    ///
    ///     col("salary").mean().over([col("dept")])
    fn over(&self, partition_by: Vec<PyExpr>) -> PyExpr {
        let exprs: Vec<Expr> = partition_by.into_iter().map(|e| e.inner).collect();
        PyExpr { inner: self.inner.clone().over(exprs) }
    }

    /// Rolling mean over *window_size* rows.
    fn rolling_mean(&self, window_size: usize) -> PyExpr {
        PyExpr { inner: self.inner.clone().rolling_mean(window_size) }
    }

    // ── String operations ────────────────────────────────────────────────────
    /// True where the string column contains *pattern* (substring match).
    fn str_contains(&self, pattern: &str) -> PyExpr {
        PyExpr { inner: self.inner.clone().str().contains(pattern) }
    }
    /// True where the string column starts with *prefix*.
    fn str_starts_with(&self, prefix: &str) -> PyExpr {
        PyExpr { inner: self.inner.clone().str().starts_with(prefix) }
    }
    /// True where the string column ends with *suffix*.
    fn str_ends_with(&self, suffix: &str) -> PyExpr {
        PyExpr { inner: self.inner.clone().str().ends_with(suffix) }
    }
    /// Convert string column to UPPER CASE.
    fn str_to_uppercase(&self) -> PyExpr {
        PyExpr { inner: self.inner.clone().str().to_uppercase() }
    }
    /// Convert string column to lower case.
    fn str_to_lowercase(&self) -> PyExpr {
        PyExpr { inner: self.inner.clone().str().to_lowercase() }
    }

    // ── Display ──────────────────────────────────────────────────────────────
    fn __repr__(&self) -> String { format!("{}", self.inner) }
    fn __str__(&self)  -> String { format!("{}", self.inner) }
}

// ── PyExprOrLit ───────────────────────────────────────────────────────────────

/// Accept either a `PyExpr` or a Python scalar (int/float/str/bool) wherever
/// an expression is expected in comparison and arithmetic operators.
#[derive(FromPyObject)]
enum PyExprOrLit {
    Expr(PyExpr),
    Bool(bool),  // must be before Int to capture True/False correctly
    Int(i64),
    Float(f64),
    Str(String),
}

impl PyExprOrLit {
    fn into_expr(self) -> Expr {
        match self {
            PyExprOrLit::Expr(e)  => e.inner,
            PyExprOrLit::Bool(v)  => blazer_engine::expr::lit(v),
            PyExprOrLit::Int(v)   => blazer_engine::expr::lit(v),
            PyExprOrLit::Float(v) => blazer_engine::expr::lit(v),
            PyExprOrLit::Str(v)   => blazer_engine::expr::lit(v),
        }
    }
}

// ── Module-level functions ────────────────────────────────────────────────────

/// Reference a column by name.
///
///     blazer.col("price")
#[pyfunction]
fn col(name: &str) -> PyExpr {
    PyExpr { inner: blazer_engine::expr::col(name) }
}

/// Wrap a Python scalar as a literal expression.
///
///     blazer.lit(42)
///     blazer.lit(3.14)
///     blazer.lit("hello")
///     blazer.lit(True)
#[pyfunction]
fn lit(value: PyExprOrLit) -> PyExpr {
    PyExpr { inner: value.into_expr() }
}

/// Read an entire Parquet file into memory as a DataFrame.
#[pyfunction]
fn read_parquet(path: &str) -> PyResult<PyDataFrame> {
    let df = blazer_engine::io::ParquetReader::from_path(std::path::Path::new(path))
        .map_err(be)?
        .finish()
        .map_err(be)?;
    Ok(PyDataFrame { inner: df })
}

/// Create a lazy scan over a Parquet file (or directory of Parquet files).
/// Nothing is read until ``.collect()`` is called.
#[pyfunction]
fn scan_parquet(path: &str) -> PyLazyFrame {
    PyLazyFrame { inner: LazyFrame::scan_parquet(path) }
}

/// Read an entire CSV file into memory as a DataFrame.
#[pyfunction]
fn read_csv(path: &str) -> PyResult<PyDataFrame> {
    let df = blazer_engine::io::CsvReader::from_path(std::path::Path::new(path))
        .map_err(be)?
        .finish()
        .map_err(be)?;
    Ok(PyDataFrame { inner: df })
}

/// Create a lazy scan over a CSV file. Nothing is read until ``.collect()``
/// is called.
///
/// For full optimizer support (predicate / projection pushdown) the lazy
/// DatasetScan path is used; the executor falls back to CsvReader internally.
#[pyfunction]
fn scan_csv(path: &str) -> PyLazyFrame {
    PyLazyFrame {
        inner: LazyFrame::from_plan(LogicalPlan::DatasetScan {
            root: path.to_string(),
            format: FileFormat::Csv,
            projection: None,
            partition_filters: vec![],
            row_filters: None,
            n_rows: None,
        }),
    }
}

// ── Module registration ───────────────────────────────────────────────────────

#[pymodule]
fn _blazer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyLazyFrame>()?;
    m.add_class::<PyExpr>()?;
    m.add_function(wrap_pyfunction!(col, m)?)?;
    m.add_function(wrap_pyfunction!(lit, m)?)?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(scan_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(scan_csv, m)?)?;
    Ok(())
}
