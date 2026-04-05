use std::fmt;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::compute::arithmetics;
use arrow2::compute::comparison;
use arrow2::datatypes::DataType as ArrowDataType;

use crate::dtype::DataType;
use crate::error::{BlazeError, Result};

/// A Series is an Arrow-backed, named, typed column.
#[derive(Clone)]
pub struct Series {
    name: String,
    dtype: DataType,
    chunks: Vec<Arc<dyn Array>>,
}

impl Series {
    /// Create a Series from a single Arrow array.
    pub fn from_arrow(name: &str, array: Arc<dyn Array>) -> Result<Self> {
        let dtype = DataType::from_arrow(array.data_type());
        Ok(Series {
            name: name.to_string(),
            dtype,
            chunks: vec![array],
        })
    }

    /// Create a Series from multiple Arrow arrays (chunks).
    pub fn from_chunks(name: &str, chunks: Vec<Arc<dyn Array>>) -> Result<Self> {
        if chunks.is_empty() {
            return Err(BlazeError::InvalidOperation(
                "Cannot create Series from empty chunks".into(),
            ));
        }
        let dtype = DataType::from_arrow(chunks[0].data_type());
        Ok(Series {
            name: name.to_string(),
            dtype,
            chunks,
        })
    }

    // ---- Constructors for common types ----

    pub fn from_i64(name: &str, values: Vec<i64>) -> Self {
        let array: Arc<dyn Array> = Arc::new(PrimitiveArray::<i64>::from_vec(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Int64,
            chunks: vec![array],
        }
    }

    pub fn from_i32(name: &str, values: Vec<i32>) -> Self {
        let array: Arc<dyn Array> = Arc::new(PrimitiveArray::<i32>::from_vec(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Int32,
            chunks: vec![array],
        }
    }

    pub fn from_f64(name: &str, values: Vec<f64>) -> Self {
        let array: Arc<dyn Array> = Arc::new(PrimitiveArray::<f64>::from_vec(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Float64,
            chunks: vec![array],
        }
    }

    pub fn from_f32(name: &str, values: Vec<f32>) -> Self {
        let array: Arc<dyn Array> = Arc::new(PrimitiveArray::<f32>::from_vec(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Float32,
            chunks: vec![array],
        }
    }

    pub fn from_bool(name: &str, values: Vec<bool>) -> Self {
        let array: Arc<dyn Array> = Arc::new(BooleanArray::from_slice(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Boolean,
            chunks: vec![array],
        }
    }

    pub fn from_str(name: &str, values: Vec<&str>) -> Self {
        let array: Arc<dyn Array> = Arc::new(Utf8Array::<i32>::from_slice(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Utf8,
            chunks: vec![array],
        }
    }

    pub fn from_opt_i64(name: &str, values: Vec<Option<i64>>) -> Self {
        let array: Arc<dyn Array> = Arc::new(PrimitiveArray::<i64>::from(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Int64,
            chunks: vec![array],
        }
    }

    pub fn from_opt_f64(name: &str, values: Vec<Option<f64>>) -> Self {
        let array: Arc<dyn Array> = Arc::new(PrimitiveArray::<f64>::from(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Float64,
            chunks: vec![array],
        }
    }

    pub fn from_opt_str(name: &str, values: Vec<Option<&str>>) -> Self {
        let array: Arc<dyn Array> = Arc::new(Utf8Array::<i32>::from(values));
        Series {
            name: name.to_string(),
            dtype: DataType::Utf8,
            chunks: vec![array],
        }
    }

    // ---- Accessors ----

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rename(&mut self, name: &str) {
        self.name = name.to_string();
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }

    pub fn len(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn null_count(&self) -> usize {
        self.chunks.iter().map(|c| c.null_count()).sum()
    }

    pub fn chunks(&self) -> &[Arc<dyn Array>] {
        &self.chunks
    }

    /// Get a single rechunked array (concatenates all chunks).
    pub fn to_array(&self) -> Arc<dyn Array> {
        if self.chunks.len() == 1 {
            return self.chunks[0].clone();
        }
        let refs: Vec<&dyn Array> = self.chunks.iter().map(|c| c.as_ref()).collect();
        arrow2::compute::concatenate::concatenate(&refs)
            .map(|a| a.into())
            .unwrap_or_else(|_| self.chunks[0].clone())
    }

    pub fn arrow_dtype(&self) -> ArrowDataType {
        self.dtype.to_arrow()
    }

    // ---- Type-specific views ----

    pub fn as_i64(&self) -> Result<I64View> {
        let arr = self.to_array();
        let prim = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| BlazeError::TypeMismatch(format!("Expected Int64, got {}", self.dtype)))?
            .clone();
        Ok(I64View(prim))
    }

    pub fn as_f64(&self) -> Result<F64View> {
        let arr = self.to_array();
        let prim = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .ok_or_else(|| {
                BlazeError::TypeMismatch(format!("Expected Float64, got {}", self.dtype))
            })?
            .clone();
        Ok(F64View(prim))
    }

    pub fn as_bool(&self) -> Result<BoolView> {
        let arr = self.to_array();
        let prim = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                BlazeError::TypeMismatch(format!("Expected Boolean, got {}", self.dtype))
            })?
            .clone();
        Ok(BoolView(prim))
    }

    pub fn as_utf8(&self) -> Result<Utf8View> {
        let arr = self.to_array();
        let prim = arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| BlazeError::TypeMismatch(format!("Expected Utf8, got {}", self.dtype)))?
            .clone();
        Ok(Utf8View(prim))
    }

    // ---- Cast ----

    /// Cast this series to Float64 (for numeric operations).
    pub fn cast_f64(&self) -> Result<Series> {
        let arr = self.to_array();
        let casted = arrow2::compute::cast::cast(arr.as_ref(), &ArrowDataType::Float64, Default::default())?;
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Float64,
            chunks: vec![casted.into()],
        })
    }

    /// Cast to Int64.
    pub fn cast_i64(&self) -> Result<Series> {
        let arr = self.to_array();
        let casted = arrow2::compute::cast::cast(arr.as_ref(), &ArrowDataType::Int64, Default::default())?;
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Int64,
            chunks: vec![casted.into()],
        })
    }

    // ---- Arithmetic ----

    pub fn add(&self, other: &Series) -> Result<Series> {
        let l = self.cast_f64()?.to_array();
        let r = other.cast_f64()?.to_array();
        let lp = l.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let rp = r.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let result = arithmetics::basic::add(lp, rp);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Float64,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn sub(&self, other: &Series) -> Result<Series> {
        let l = self.cast_f64()?.to_array();
        let r = other.cast_f64()?.to_array();
        let lp = l.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let rp = r.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let result = arithmetics::basic::sub(lp, rp);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Float64,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn mul(&self, other: &Series) -> Result<Series> {
        let l = self.cast_f64()?.to_array();
        let r = other.cast_f64()?.to_array();
        let lp = l.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let rp = r.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let result = arithmetics::basic::mul(lp, rp);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Float64,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn div(&self, other: &Series) -> Result<Series> {
        let l = self.cast_f64()?.to_array();
        let r = other.cast_f64()?.to_array();
        let lp = l.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let rp = r.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let result = arithmetics::basic::div(lp, rp);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Float64,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn modulo(&self, other: &Series) -> Result<Series> {
        let l = self.cast_f64()?.to_array();
        let r = other.cast_f64()?.to_array();
        let lp = l.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let rp = r.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        // Manual modulo since arrow2 doesn't have a mod function
        let values: Vec<f64> = lp
            .values()
            .iter()
            .zip(rp.values().iter())
            .map(|(a, b)| a % b)
            .collect();
        let result = PrimitiveArray::<f64>::from_vec(values);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Float64,
            chunks: vec![Arc::new(result)],
        })
    }

    // ---- Comparison ----

    pub fn eq_series(&self, other: &Series) -> Result<Series> {
        self.compare(other, "eq")
    }

    pub fn neq_series(&self, other: &Series) -> Result<Series> {
        self.compare(other, "neq")
    }

    pub fn lt_series(&self, other: &Series) -> Result<Series> {
        self.compare(other, "lt")
    }

    pub fn lte_series(&self, other: &Series) -> Result<Series> {
        self.compare(other, "lte")
    }

    pub fn gt_series(&self, other: &Series) -> Result<Series> {
        self.compare(other, "gt")
    }

    pub fn gte_series(&self, other: &Series) -> Result<Series> {
        self.compare(other, "gte")
    }

    fn compare(&self, other: &Series, op: &str) -> Result<Series> {
        // Cast both to f64 for numeric comparison
        if self.dtype.is_numeric() && other.dtype.is_numeric() {
            let l = self.cast_f64()?.to_array();
            let r = other.cast_f64()?.to_array();
            let lp = l.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            let rp = r.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            let result: BooleanArray = match op {
                "eq" => comparison::eq(lp, rp),
                "neq" => comparison::neq(lp, rp),
                "lt" => comparison::lt(lp, rp),
                "lte" => comparison::lt_eq(lp, rp),
                "gt" => comparison::gt(lp, rp),
                "gte" => comparison::gt_eq(lp, rp),
                _ => return Err(BlazeError::InvalidOperation(format!("Unknown op: {}", op))),
            };
            return Ok(Series {
                name: self.name.clone(),
                dtype: DataType::Boolean,
                chunks: vec![Arc::new(result)],
            });
        }
        // String comparison
        if self.dtype == DataType::Utf8 && other.dtype == DataType::Utf8 {
            let l = self.to_array();
            let r = other.to_array();
            let lp = l.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let rp = r.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let result: BooleanArray = match op {
                "eq" => comparison::eq(lp, rp),
                "neq" => comparison::neq(lp, rp),
                "lt" => comparison::lt(lp, rp),
                "lte" => comparison::lt_eq(lp, rp),
                "gt" => comparison::gt(lp, rp),
                "gte" => comparison::gt_eq(lp, rp),
                _ => return Err(BlazeError::InvalidOperation(format!("Unknown op: {}", op))),
            };
            return Ok(Series {
                name: self.name.clone(),
                dtype: DataType::Boolean,
                chunks: vec![Arc::new(result)],
            });
        }
        // Boolean equality / inequality
        if self.dtype == DataType::Boolean && other.dtype == DataType::Boolean {
            let l = self.to_array();
            let r = other.to_array();
            let lp = l.as_any().downcast_ref::<BooleanArray>().unwrap();
            let rp = r.as_any().downcast_ref::<BooleanArray>().unwrap();
            let result: BooleanArray = match op {
                "eq"  => comparison::eq(lp, rp),
                "neq" => comparison::neq(lp, rp),
                _ => return Err(BlazeError::TypeMismatch(format!(
                    "Operator '{op}' is not supported for Boolean columns"
                ))),
            };
            return Ok(Series {
                name: self.name.clone(),
                dtype: DataType::Boolean,
                chunks: vec![Arc::new(result)],
            });
        }
        Err(BlazeError::TypeMismatch(format!(
            "Cannot compare {} with {}",
            self.dtype, other.dtype
        )))
    }

    // ---- Logical ----

    pub fn and_series(&self, other: &Series) -> Result<Series> {
        let l = self.as_bool()?;
        let r = other.as_bool()?;
        let result = arrow2::compute::boolean::and(&l.0, &r.0);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Boolean,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn or_series(&self, other: &Series) -> Result<Series> {
        let l = self.as_bool()?;
        let r = other.as_bool()?;
        let result = arrow2::compute::boolean::or(&l.0, &r.0);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Boolean,
            chunks: vec![Arc::new(result)],
        })
    }

    // ---- Aggregations ----

    /// Sum all non-null values as f64.
    ///
    /// Dispatches directly on the native array type so we never allocate an
    /// intermediate f64 array (which the old `cast_f64()` approach required).
    /// The inner `sum()` call auto-vectorises with SIMD in release builds.
    pub fn sum_as_f64(&self) -> Result<f64> {
        let arr = self.to_array();
        let v: f64 = match self.dtype() {
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                a.values().iter().copied().sum()
            }
            DataType::Float32 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
                a.values().iter().map(|&v| v as f64).sum()
            }
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                a.values().iter().map(|&v| v as f64).sum()
            }
            DataType::Int32 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                a.values().iter().map(|&v| v as f64).sum()
            }
            _ => {
                // Unusual type: cast once to f64 then sum
                let f = self.cast_f64()?;
                let arr2 = f.to_array();
                let a = arr2.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                a.values().iter().copied().sum()
            }
        };
        Ok(v)
    }

    pub fn mean_as_f64(&self) -> Result<f64> {
        let s = self.sum_as_f64()?;
        let count = self.len() - self.null_count();
        if count == 0 {
            return Ok(f64::NAN);
        }
        Ok(s / count as f64)
    }

    /// Min of all non-null values as f64.
    ///
    /// Uses arrow2's `min_primitive` SIMD kernel on the native array type,
    /// falling back to a typed fold for float types not covered by that kernel.
    pub fn min_as_f64(&self) -> Result<f64> {
        use arrow2::compute::aggregate as agg;
        let arr = self.to_array();
        let v: f64 = match self.dtype() {
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                // min_primitive returns Option<T> for NativeType + PartialOrd
                a.values().iter().copied().fold(f64::INFINITY, f64::min)
            }
            DataType::Float32 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
                a.values().iter().copied().fold(f32::INFINITY, f32::min) as f64
            }
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                agg::min_primitive(a).unwrap_or(i64::MAX) as f64
            }
            DataType::Int32 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                agg::min_primitive(a).unwrap_or(i32::MAX) as f64
            }
            _ => {
                let f = self.cast_f64()?;
                let arr2 = f.to_array();
                let a = arr2.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                a.values().iter().copied().fold(f64::INFINITY, f64::min)
            }
        };
        Ok(v)
    }

    /// Max of all non-null values as f64.
    pub fn max_as_f64(&self) -> Result<f64> {
        use arrow2::compute::aggregate as agg;
        let arr = self.to_array();
        let v: f64 = match self.dtype() {
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                a.values().iter().copied().fold(f64::NEG_INFINITY, f64::max)
            }
            DataType::Float32 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
                a.values().iter().copied().fold(f32::NEG_INFINITY, f32::max) as f64
            }
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                agg::max_primitive(a).unwrap_or(i64::MIN) as f64
            }
            DataType::Int32 => {
                let a = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                agg::max_primitive(a).unwrap_or(i32::MIN) as f64
            }
            _ => {
                let f = self.cast_f64()?;
                let arr2 = f.to_array();
                let a = arr2.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
                a.values().iter().copied().fold(f64::NEG_INFINITY, f64::max)
            }
        };
        Ok(v)
    }

    pub fn count(&self) -> usize {
        self.len() - self.null_count()
    }

    // ---- Slice / Filter ----

    pub fn filter(&self, mask: &BooleanArray) -> Result<Series> {
        let arr = self.to_array();
        let filtered = arrow2::compute::filter::filter(arr.as_ref(), mask)?;
        Ok(Series {
            name: self.name.clone(),
            dtype: self.dtype.clone(),
            chunks: vec![filtered.into()],
        })
    }

    pub fn slice(&self, offset: usize, length: usize) -> Series {
        let arr = self.to_array();
        let sliced = arr.sliced(offset, length);
        Series {
            name: self.name.clone(),
            dtype: self.dtype.clone(),
            chunks: vec![sliced.into()],
        }
    }

    pub fn take(&self, indices: &PrimitiveArray<u32>) -> Result<Series> {
        let arr = self.to_array();
        let taken = arrow2::compute::take::take(arr.as_ref(), indices)?;
        Ok(Series {
            name: self.name.clone(),
            dtype: self.dtype.clone(),
            chunks: vec![taken.into()],
        })
    }

    // ---- Sort ----

    pub fn sort(&self, descending: bool) -> Result<Series> {
        let arr = self.to_array();
        let opts = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: false,
        };
        let indices = arrow2::compute::sort::sort_to_indices::<u32>(arr.as_ref(), &opts, None)?;
        self.take(&indices)
    }

    pub fn argsort(&self, descending: bool) -> Result<PrimitiveArray<u32>> {
        let arr = self.to_array();
        let opts = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: false,
        };
        let indices = arrow2::compute::sort::sort_to_indices::<u32>(arr.as_ref(), &opts, None)?;
        Ok(indices)
    }

    // ---- String ops ----

    pub fn str_to_uppercase(&self) -> Result<Series> {
        let arr = self.to_array();
        let utf8 = arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| {
                BlazeError::TypeMismatch("Expected Utf8 for string op".into())
            })?;
        let values: Vec<Option<String>> = utf8
            .iter()
            .map(|v| v.map(|s| s.to_uppercase()))
            .collect();
        let result: Utf8Array<i32> = values
            .iter()
            .map(|v| v.as_deref())
            .collect();
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Utf8,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn str_to_lowercase(&self) -> Result<Series> {
        let arr = self.to_array();
        let utf8 = arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| {
                BlazeError::TypeMismatch("Expected Utf8 for string op".into())
            })?;
        let values: Vec<Option<String>> = utf8
            .iter()
            .map(|v| v.map(|s| s.to_lowercase()))
            .collect();
        let result: Utf8Array<i32> = values
            .iter()
            .map(|v| v.as_deref())
            .collect();
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Utf8,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn str_contains(&self, pattern: &str) -> Result<Series> {
        let arr = self.to_array();
        let utf8 = arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| {
                BlazeError::TypeMismatch("Expected Utf8 for string op".into())
            })?;
        let re = regex::Regex::new(pattern)
            .map_err(|e| BlazeError::ParseError(format!("Invalid regex: {}", e)))?;
        let values: Vec<bool> = utf8
            .iter()
            .map(|v| v.map_or(false, |s| re.is_match(s)))
            .collect();
        Ok(Series::from_bool(&self.name, values))
    }

    pub fn str_starts_with(&self, prefix: &str) -> Result<Series> {
        let arr = self.to_array();
        let utf8 = arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| BlazeError::TypeMismatch("Expected Utf8 for starts_with".into()))?;
        let values: Vec<bool> = utf8
            .iter()
            .map(|v| v.map_or(false, |s| s.starts_with(prefix)))
            .collect();
        Ok(Series::from_bool(&self.name, values))
    }

    pub fn str_ends_with(&self, suffix: &str) -> Result<Series> {
        let arr = self.to_array();
        let utf8 = arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| BlazeError::TypeMismatch("Expected Utf8 for ends_with".into()))?;
        let values: Vec<bool> = utf8
            .iter()
            .map(|v| v.map_or(false, |s| s.ends_with(suffix)))
            .collect();
        Ok(Series::from_bool(&self.name, values))
    }

    // ---- Rolling ----

    pub fn rolling_mean(&self, window: usize) -> Result<Series> {
        let arr = self.cast_f64()?.to_array();
        let prim = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let values = prim.values();
        let n = values.len();
        let mut result: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            if i + 1 < window {
                result.push(f64::NAN);
            } else {
                let start = i + 1 - window;
                let sum: f64 = values[start..=i].iter().sum();
                result.push(sum / window as f64);
            }
        }
        Ok(Series::from_f64(&self.name, result))
    }

    pub fn rolling_sum(&self, window: usize) -> Result<Series> {
        let arr = self.cast_f64()?.to_array();
        let prim = arr
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let values = prim.values();
        let n = values.len();
        let mut result: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            if i + 1 < window {
                result.push(f64::NAN);
            } else {
                let start = i + 1 - window;
                let sum: f64 = values[start..=i].iter().sum();
                result.push(sum);
            }
        }
        Ok(Series::from_f64(&self.name, result))
    }

    // ---- Scalar comparison helpers (for expression evaluation) ----

    pub fn gt_scalar_i64(&self, scalar: i64) -> Result<Series> {
        let arr = self.to_array();
        if let Some(prim) = arr.as_any().downcast_ref::<PrimitiveArray<i64>>() {
            let scalar_val = arrow2::scalar::PrimitiveScalar::new(ArrowDataType::Int64, Some(scalar));
            let result = comparison::gt_scalar(prim, &scalar_val);
            return Ok(Series {
                name: self.name.clone(),
                dtype: DataType::Boolean,
                chunks: vec![Arc::new(result)],
            });
        }
        let arr = self.cast_f64()?.to_array();
        let prim = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let scalar_val = arrow2::scalar::PrimitiveScalar::new(ArrowDataType::Float64, Some(scalar as f64));
        let result = comparison::gt_scalar(prim, &scalar_val);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Boolean,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn gt_scalar_f64(&self, scalar: f64) -> Result<Series> {
        let arr = self.cast_f64()?.to_array();
        let prim = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let scalar_val = arrow2::scalar::PrimitiveScalar::new(ArrowDataType::Float64, Some(scalar));
        let result = comparison::gt_scalar(prim, &scalar_val);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Boolean,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn lt_scalar_f64(&self, scalar: f64) -> Result<Series> {
        let arr = self.cast_f64()?.to_array();
        let prim = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let scalar_val = arrow2::scalar::PrimitiveScalar::new(ArrowDataType::Float64, Some(scalar));
        let result = comparison::lt_scalar(prim, &scalar_val);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Boolean,
            chunks: vec![Arc::new(result)],
        })
    }

    pub fn eq_scalar_i64(&self, scalar: i64) -> Result<Series> {
        let arr = self.to_array();
        if let Some(prim) = arr.as_any().downcast_ref::<PrimitiveArray<i64>>() {
            let scalar_val = arrow2::scalar::PrimitiveScalar::new(ArrowDataType::Int64, Some(scalar));
            let result = comparison::eq_scalar(prim, &scalar_val);
            return Ok(Series {
                name: self.name.clone(),
                dtype: DataType::Boolean,
                chunks: vec![Arc::new(result)],
            });
        }
        let arr = self.cast_f64()?.to_array();
        let prim = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        let scalar_val = arrow2::scalar::PrimitiveScalar::new(ArrowDataType::Float64, Some(scalar as f64));
        let result = comparison::eq_scalar(prim, &scalar_val);
        Ok(Series {
            name: self.name.clone(),
            dtype: DataType::Boolean,
            chunks: vec![Arc::new(result)],
        })
    }

    /// Construct a scalar series (single value repeated n times) for broadcast operations.
    pub fn new_scalar_i64(name: &str, value: i64, len: usize) -> Self {
        Series::from_i64(name, vec![value; len])
    }

    pub fn new_scalar_f64(name: &str, value: f64, len: usize) -> Self {
        Series::from_f64(name, vec![value; len])
    }

    pub fn new_scalar_str(name: &str, value: &str, len: usize) -> Self {
        let values: Vec<&str> = vec![value; len];
        Series::from_str(name, values)
    }

    pub fn new_scalar_bool(name: &str, value: bool, len: usize) -> Self {
        Series::from_bool(name, vec![value; len])
    }
}

// ---- Typed views ----

pub struct I64View(pub PrimitiveArray<i64>);
impl I64View {
    pub fn value(&self, i: usize) -> i64 {
        self.0.value(i)
    }
    pub fn values(&self) -> &[i64] {
        self.0.values().as_slice()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct F64View(pub PrimitiveArray<f64>);
impl F64View {
    pub fn value(&self, i: usize) -> f64 {
        self.0.value(i)
    }
    pub fn values(&self) -> &[f64] {
        self.0.values().as_slice()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct BoolView(pub BooleanArray);
impl BoolView {
    pub fn value(&self, i: usize) -> bool {
        self.0.value(i)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct Utf8View(pub Utf8Array<i32>);
impl Utf8View {
    pub fn value(&self, i: usize) -> &str {
        self.0.value(i)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Debug for Series {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Series({:?}, {}, len={})",
            self.name,
            self.dtype,
            self.len()
        )
    }
}

impl fmt::Display for Series {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Series: {} [{}]\n", self.name, self.dtype)?;
        let arr = self.to_array();
        let n = arr.len().min(10);
        for i in 0..n {
            if arr.is_null(i) {
                writeln!(f, "  null")?;
            } else {
                match self.dtype {
                    DataType::Int64 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i64>>()
                            .unwrap();
                        writeln!(f, "  {}", p.value(i))?;
                    }
                    DataType::Float64 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<PrimitiveArray<f64>>()
                            .unwrap();
                        writeln!(f, "  {}", p.value(i))?;
                    }
                    DataType::Utf8 => {
                        let p = arr
                            .as_any()
                            .downcast_ref::<Utf8Array<i32>>()
                            .unwrap();
                        writeln!(f, "  {:?}", p.value(i))?;
                    }
                    DataType::Boolean => {
                        let p = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                        writeln!(f, "  {}", p.value(i))?;
                    }
                    _ => writeln!(f, "  ...")?,
                }
            }
        }
        if arr.len() > 10 {
            writeln!(f, "  ... ({} more)", arr.len() - 10)?;
        }
        Ok(())
    }
}
