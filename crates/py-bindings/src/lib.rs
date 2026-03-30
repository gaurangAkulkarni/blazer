use pyo3::prelude::*;
use blazer_engine::prelude::{
    DataFrame, Expr, LazyFrame, Series, SortOptions,
};

#[pyclass]
#[derive(Clone)]
struct PyDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    #[new]
    fn new(data: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dict = data.downcast::<pyo3::types::PyDict>()?;
        let mut columns = Vec::new();
        for (key, value) in dict.iter() {
            let name: String = key.extract()?;
            let list = value.downcast::<pyo3::types::PyList>()?;
            if list.is_empty() {
                continue;
            }
            let first = list.get_item(0)?;
            if first.extract::<i64>().is_ok() {
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
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    format!("Unsupported type for column '{}'", name),
                ));
            }
        }
        let df = DataFrame::new(columns).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;
        Ok(PyDataFrame { inner: df })
    }

    fn height(&self) -> usize {
        self.inner.height()
    }

    fn width(&self) -> usize {
        self.inner.width()
    }

    fn lazy(&self) -> PyLazyFrame {
        PyLazyFrame {
            inner: self.inner.clone().lazy(),
        }
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }
}

#[pyclass]
#[derive(Clone)]
struct PyLazyFrame {
    inner: LazyFrame,
}

#[pymethods]
impl PyLazyFrame {
    fn filter(&self, predicate: PyExpr) -> Self {
        PyLazyFrame {
            inner: self.inner.clone().filter(predicate.inner),
        }
    }

    fn select(&self, exprs: Vec<PyExpr>) -> Self {
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.inner).collect();
        PyLazyFrame {
            inner: self.inner.clone().select(exprs),
        }
    }

    fn with_columns(&self, exprs: Vec<PyExpr>) -> Self {
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.inner).collect();
        PyLazyFrame {
            inner: self.inner.clone().with_columns(exprs),
        }
    }

    #[pyo3(signature = (keys, aggs))]
    fn group_by(&self, keys: Vec<PyExpr>, aggs: Vec<PyExpr>) -> Self {
        let keys: Vec<Expr> = keys.into_iter().map(|e| e.inner).collect();
        let aggs: Vec<Expr> = aggs.into_iter().map(|e| e.inner).collect();
        PyLazyFrame {
            inner: self.inner.clone().group_by(keys).agg(aggs),
        }
    }

    #[pyo3(signature = (by, descending = false))]
    fn sort(&self, by: &str, descending: bool) -> Self {
        let opts = if descending {
            SortOptions::descending()
        } else {
            SortOptions::ascending()
        };
        PyLazyFrame {
            inner: self.inner.clone().sort(by, opts),
        }
    }

    fn collect(&self) -> PyResult<PyDataFrame> {
        let df = self.inner.clone().collect().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
        })?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (optimized = true))]
    fn explain(&self, optimized: bool) -> String {
        self.inner.clone().explain(optimized)
    }
}

#[pyclass]
#[derive(Clone)]
struct PyExpr {
    inner: Expr,
}

#[pymethods]
impl PyExpr {
    fn alias(&self, name: &str) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().alias(name),
        }
    }

    fn sum(&self) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().sum(),
        }
    }

    fn mean(&self) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().mean(),
        }
    }

    fn min(&self) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().min(),
        }
    }

    fn max(&self) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().max(),
        }
    }

    fn count(&self) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().count(),
        }
    }

    fn __gt__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().gt(other.into_expr()),
        }
    }

    fn __lt__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().lt(other.into_expr()),
        }
    }

    fn __eq__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().eq(other.into_expr()),
        }
    }

    fn __add__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().add(other.into_expr()),
        }
    }

    fn __sub__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().sub(other.into_expr()),
        }
    }

    fn __mul__(&self, other: PyExprOrLit) -> PyExpr {
        PyExpr {
            inner: self.inner.clone().mul(other.into_expr()),
        }
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }
}

#[derive(FromPyObject)]
enum PyExprOrLit {
    Expr(PyExpr),
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

impl PyExprOrLit {
    fn into_expr(self) -> Expr {
        match self {
            PyExprOrLit::Expr(e) => e.inner,
            PyExprOrLit::Int(v) => blazer_engine::expr::lit(v),
            PyExprOrLit::Float(v) => blazer_engine::expr::lit(v),
            PyExprOrLit::Str(v) => blazer_engine::expr::lit(v),
            PyExprOrLit::Bool(v) => blazer_engine::expr::lit(v),
        }
    }
}

#[pyfunction]
fn col(name: &str) -> PyExpr {
    PyExpr {
        inner: blazer_engine::expr::col(name),
    }
}

#[pyfunction]
fn lit(value: PyExprOrLit) -> PyExpr {
    PyExpr {
        inner: value.into_expr(),
    }
}

#[pymodule]
fn blazer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyLazyFrame>()?;
    m.add_class::<PyExpr>()?;
    m.add_function(wrap_pyfunction!(col, m)?)?;
    m.add_function(wrap_pyfunction!(lit, m)?)?;
    Ok(())
}
