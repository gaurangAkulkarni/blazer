//! Apple MLX compute backend.
//!
//! Available only on macOS and only when the `mlx` feature is enabled.
//! Provides GPU-accelerated (Metal / ANE) implementations of the
//! `ComputeBackend` trait for the most compute-intensive operations.
//!
//! # Strategy
//!
//! * All numeric series are cast to **f64** before being uploaded to MLX.
//!   This keeps the shim surface small and avoids type-dispatch in Rust.
//!   The cost of an extra cast is tiny compared to GPU dispatch overhead.
//!
//! * Boolean series are represented as **u8** (0 / 1) on the MLX side
//!   and converted back to Arrow2 `BooleanArray` on the way out.
//!
//! * Null values are filled with `0.0` / `0` before upload and are
//!   re-masked with the original validity bitmap after the result is
//!   brought back.  (MLX has no notion of nullability.)
//!
//! * Sort / argsort preserve the original element dtype on the way back.

use std::sync::Arc;

use arrow2::array::{Array, BooleanArray, PrimitiveArray as ArrowPrimArray};
use arrow2::bitmap::Bitmap;

use mlx_sys::{
    MlxArrayRaw,
    blazer_mlx_add, blazer_mlx_sub, blazer_mlx_mul, blazer_mlx_div, blazer_mlx_rem,
    blazer_mlx_eq, blazer_mlx_neq, blazer_mlx_lt, blazer_mlx_lte, blazer_mlx_gt, blazer_mlx_gte,
    blazer_mlx_logical_and, blazer_mlx_logical_or,
    blazer_mlx_sum_f64, blazer_mlx_mean_f64, blazer_mlx_min_f64, blazer_mlx_max_f64,
    blazer_mlx_sort, blazer_mlx_sort_desc, blazer_mlx_argsort, blazer_mlx_argsort_desc,
    blazer_mlx_from_f64, blazer_mlx_from_bool,
    blazer_mlx_copy_f64, blazer_mlx_copy_bool, blazer_mlx_copy_u32,
    blazer_mlx_array_free, blazer_mlx_array_size,
};

use crate::dtype::DataType;
use crate::error::{BlazeError, Result};
use crate::series::Series;

use super::backend::ComputeBackend;

// ------------------------------------------------------------------ //
// Safe RAII wrapper around MlxArrayRaw                                //
// ------------------------------------------------------------------ //

struct MlxArray(MlxArrayRaw);

impl MlxArray {
    fn as_raw(&self) -> MlxArrayRaw {
        self.0
    }

    fn size(&self) -> usize {
        unsafe { blazer_mlx_array_size(self.0) }
    }
}

impl Drop for MlxArray {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { blazer_mlx_array_free(self.0) };
        }
    }
}

// ------------------------------------------------------------------ //
// Series → MLX conversion helpers                                     //
// ------------------------------------------------------------------ //

/// Extract values from a Series as a flat Vec<f64>, replacing nulls with 0.0.
fn series_to_f64_vec(s: &Series) -> Result<(Vec<f64>, Option<Bitmap>)> {
    let arr = s.to_array();
    let n = arr.len();

    // Collect the combined validity bitmap (if any).
    let bitmap = arr.validity().cloned();

    let values: Vec<f64> = match s.dtype() {
        DataType::Float64 => {
            let prim = arr.as_any().downcast_ref::<ArrowPrimArray<f64>>()
                .ok_or_else(|| BlazeError::TypeMismatch("Expected Float64".into()))?;
            (0..n).map(|i| if prim.is_null(i) { 0.0 } else { prim.value(i) }).collect()
        }
        DataType::Float32 => {
            let prim = arr.as_any().downcast_ref::<ArrowPrimArray<f32>>()
                .ok_or_else(|| BlazeError::TypeMismatch("Expected Float32".into()))?;
            (0..n).map(|i| if prim.is_null(i) { 0.0 } else { prim.value(i) as f64 }).collect()
        }
        DataType::Int64 => {
            let prim = arr.as_any().downcast_ref::<ArrowPrimArray<i64>>()
                .ok_or_else(|| BlazeError::TypeMismatch("Expected Int64".into()))?;
            (0..n).map(|i| if prim.is_null(i) { 0.0 } else { prim.value(i) as f64 }).collect()
        }
        DataType::Int32 => {
            let prim = arr.as_any().downcast_ref::<ArrowPrimArray<i32>>()
                .ok_or_else(|| BlazeError::TypeMismatch("Expected Int32".into()))?;
            (0..n).map(|i| if prim.is_null(i) { 0.0 } else { prim.value(i) as f64 }).collect()
        }
        DataType::UInt32 => {
            let prim = arr.as_any().downcast_ref::<ArrowPrimArray<u32>>()
                .ok_or_else(|| BlazeError::TypeMismatch("Expected UInt32".into()))?;
            (0..n).map(|i| if prim.is_null(i) { 0.0 } else { prim.value(i) as f64 }).collect()
        }
        DataType::UInt64 => {
            let prim = arr.as_any().downcast_ref::<ArrowPrimArray<u64>>()
                .ok_or_else(|| BlazeError::TypeMismatch("Expected UInt64".into()))?;
            (0..n).map(|i| if prim.is_null(i) { 0.0 } else { prim.value(i) as f64 }).collect()
        }
        DataType::Boolean => {
            let prim = arr.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| BlazeError::TypeMismatch("Expected Boolean".into()))?;
            (0..n).map(|i| if prim.is_null(i) { 0.0 } else { prim.value(i) as i32 as f64 }).collect()
        }
        other => return Err(BlazeError::TypeMismatch(
            format!("MLX backend: unsupported dtype {:?} for f64 conversion", other)
        )),
    };

    Ok((values, bitmap))
}

/// Extract boolean values from a Series as a flat Vec<u8> (0/1).
fn series_to_bool_vec(s: &Series) -> Result<(Vec<u8>, Option<Bitmap>)> {
    let arr = s.to_array();
    let n = arr.len();
    let bitmap = arr.validity().cloned();
    let bool_arr = arr.as_any().downcast_ref::<BooleanArray>()
        .ok_or_else(|| BlazeError::TypeMismatch(
            format!("MLX logical op requires Boolean series, got {:?}", s.dtype())
        ))?;
    let values: Vec<u8> = (0..n).map(|i| {
        if bool_arr.is_null(i) { 0u8 } else { bool_arr.value(i) as u8 }
    }).collect();
    Ok((values, bitmap))
}

/// Upload a Vec<f64> to MLX.  Returns an `MlxArray` handle.
fn upload_f64(values: &[f64]) -> MlxArray {
    let raw = unsafe { blazer_mlx_from_f64(values.as_ptr(), values.len()) };
    MlxArray(raw)
}

/// Upload a Vec<u8> (bool) to MLX.
fn upload_bool(values: &[u8]) -> MlxArray {
    let raw = unsafe { blazer_mlx_from_bool(values.as_ptr(), values.len()) };
    MlxArray(raw)
}

/// Download f64 values from MLX into a new Vec.
fn download_f64(arr: &MlxArray) -> Vec<f64> {
    let n = arr.size();
    let mut out = vec![0f64; n];
    unsafe { blazer_mlx_copy_f64(arr.as_raw(), out.as_mut_ptr(), n) };
    out
}

/// Download bool (u8) values from MLX into a new Vec.
fn download_bool(arr: &MlxArray) -> Vec<u8> {
    let n = arr.size();
    let mut out = vec![0u8; n];
    unsafe { blazer_mlx_copy_bool(arr.as_raw(), out.as_mut_ptr(), n) };
    out
}

/// Download u32 values (argsort indices) from MLX.
fn download_u32(arr: &MlxArray) -> Vec<u32> {
    let n = arr.size();
    let mut out = vec![0u32; n];
    unsafe { blazer_mlx_copy_u32(arr.as_raw(), out.as_mut_ptr(), n) };
    out
}

/// Build an f64 Series from a Vec<f64> with an optional validity bitmap.
fn f64_to_series(name: &str, values: Vec<f64>, bitmap: Option<Bitmap>) -> Series {
    let arr: Arc<dyn Array> = if let Some(bm) = bitmap {
        Arc::new(ArrowPrimArray::<f64>::from_vec(values).with_validity(Some(bm)))
    } else {
        Arc::new(ArrowPrimArray::<f64>::from_vec(values))
    };
    Series::from_arrow(name, arr).expect("f64_to_series: infallible")
}

/// Build a boolean Series from a Vec<u8> (0/1) with an optional bitmap.
fn bool_to_series(name: &str, values: Vec<u8>, bitmap: Option<Bitmap>) -> Series {
    let bools: Vec<bool> = values.iter().map(|&v| v != 0).collect();
    let arr: Arc<dyn Array> = if let Some(bm) = bitmap {
        Arc::new(BooleanArray::from_slice(bools).with_validity(Some(bm)))
    } else {
        Arc::new(BooleanArray::from_slice(bools))
    };
    Series::from_arrow(name, arr).expect("bool_to_series: infallible")
}

// ------------------------------------------------------------------ //
// Helper: apply a binary op (f64 path)                                //
// ------------------------------------------------------------------ //

type BinaryOp = unsafe extern "C" fn(MlxArrayRaw, MlxArrayRaw) -> MlxArrayRaw;

/// Generic f64 binary op: upload both series as f64, run op, download result.
fn bin_op_f64(
    left: &Series,
    right: &Series,
    op: BinaryOp,
) -> Result<Series> {
    let (lv, lbm) = series_to_f64_vec(left)?;
    let (rv, _rbm) = series_to_f64_vec(right)?;

    let la = upload_f64(&lv);
    let ra = upload_f64(&rv);

    let res = MlxArray(unsafe { op(la.as_raw(), ra.as_raw()) });
    let out = download_f64(&res);
    // Propagate left nulls to output.
    Ok(f64_to_series(left.name(), out, lbm))
}

/// Generic f64 → bool comparison op.
fn cmp_op_f64(
    left: &Series,
    right: &Series,
    op: BinaryOp,
) -> Result<Series> {
    let (lv, lbm) = series_to_f64_vec(left)?;
    let (rv, _rbm) = series_to_f64_vec(right)?;

    let la = upload_f64(&lv);
    let ra = upload_f64(&rv);

    let res = MlxArray(unsafe { op(la.as_raw(), ra.as_raw()) });
    let out = download_bool(&res);
    Ok(bool_to_series(left.name(), out, lbm))
}

/// Generic bool → bool logical op.
fn logical_op(
    left: &Series,
    right: &Series,
    op: BinaryOp,
) -> Result<Series> {
    let (lv, lbm) = series_to_bool_vec(left)?;
    let (rv, _rbm) = series_to_bool_vec(right)?;

    let la = upload_bool(&lv);
    let ra = upload_bool(&rv);

    let res = MlxArray(unsafe { op(la.as_raw(), ra.as_raw()) });
    let out = download_bool(&res);
    Ok(bool_to_series(left.name(), out, lbm))
}

// ------------------------------------------------------------------ //
// MlxBackend                                                          //
// ------------------------------------------------------------------ //

pub struct MlxBackend;

impl ComputeBackend for MlxBackend {
    fn name(&self) -> &str {
        "mlx"
    }

    // ---- Arithmetic ----

    fn add(&self, left: &Series, right: &Series) -> Result<Series> {
        bin_op_f64(left, right, blazer_mlx_add)
    }

    fn sub(&self, left: &Series, right: &Series) -> Result<Series> {
        bin_op_f64(left, right, blazer_mlx_sub)
    }

    fn mul(&self, left: &Series, right: &Series) -> Result<Series> {
        bin_op_f64(left, right, blazer_mlx_mul)
    }

    fn div(&self, left: &Series, right: &Series) -> Result<Series> {
        bin_op_f64(left, right, blazer_mlx_div)
    }

    fn modulo(&self, left: &Series, right: &Series) -> Result<Series> {
        bin_op_f64(left, right, blazer_mlx_rem)
    }

    // ---- Comparison ----

    fn eq_series(&self, left: &Series, right: &Series) -> Result<Series> {
        cmp_op_f64(left, right, blazer_mlx_eq)
    }

    fn neq_series(&self, left: &Series, right: &Series) -> Result<Series> {
        cmp_op_f64(left, right, blazer_mlx_neq)
    }

    fn lt_series(&self, left: &Series, right: &Series) -> Result<Series> {
        cmp_op_f64(left, right, blazer_mlx_lt)
    }

    fn lte_series(&self, left: &Series, right: &Series) -> Result<Series> {
        cmp_op_f64(left, right, blazer_mlx_lte)
    }

    fn gt_series(&self, left: &Series, right: &Series) -> Result<Series> {
        cmp_op_f64(left, right, blazer_mlx_gt)
    }

    fn gte_series(&self, left: &Series, right: &Series) -> Result<Series> {
        cmp_op_f64(left, right, blazer_mlx_gte)
    }

    // ---- Logical ----

    fn and_series(&self, left: &Series, right: &Series) -> Result<Series> {
        logical_op(left, right, blazer_mlx_logical_and)
    }

    fn or_series(&self, left: &Series, right: &Series) -> Result<Series> {
        logical_op(left, right, blazer_mlx_logical_or)
    }

    // ---- Aggregation ----

    fn sum(&self, series: &Series) -> Result<f64> {
        let (vals, _) = series_to_f64_vec(series)?;
        let arr = upload_f64(&vals);
        Ok(unsafe { blazer_mlx_sum_f64(arr.as_raw()) })
    }

    fn mean(&self, series: &Series) -> Result<f64> {
        let (vals, _) = series_to_f64_vec(series)?;
        let arr = upload_f64(&vals);
        Ok(unsafe { blazer_mlx_mean_f64(arr.as_raw()) })
    }

    fn min(&self, series: &Series) -> Result<f64> {
        let (vals, _) = series_to_f64_vec(series)?;
        let arr = upload_f64(&vals);
        Ok(unsafe { blazer_mlx_min_f64(arr.as_raw()) })
    }

    fn max(&self, series: &Series) -> Result<f64> {
        let (vals, _) = series_to_f64_vec(series)?;
        let arr = upload_f64(&vals);
        Ok(unsafe { blazer_mlx_max_f64(arr.as_raw()) })
    }

    // ---- Sort / argsort ----

    fn sort(&self, series: &Series, descending: bool) -> Result<Series> {
        let (vals, _bitmap) = series_to_f64_vec(series)?;
        let arr = upload_f64(&vals);
        let sorted = if descending {
            MlxArray(unsafe { blazer_mlx_sort_desc(arr.as_raw()) })
        } else {
            MlxArray(unsafe { blazer_mlx_sort(arr.as_raw()) })
        };
        let out = download_f64(&sorted);
        // Note: sorting strips null positions — we drop the bitmap here.
        Ok(f64_to_series(series.name(), out, None))
    }

    fn argsort(&self, series: &Series, descending: bool) -> Result<ArrowPrimArray<u32>> {
        let (vals, _) = series_to_f64_vec(series)?;
        let arr = upload_f64(&vals);
        let indices = if descending {
            MlxArray(unsafe { blazer_mlx_argsort_desc(arr.as_raw()) })
        } else {
            MlxArray(unsafe { blazer_mlx_argsort(arr.as_raw()) })
        };
        let out = download_u32(&indices);
        Ok(ArrowPrimArray::<u32>::from_vec(out))
    }
}

