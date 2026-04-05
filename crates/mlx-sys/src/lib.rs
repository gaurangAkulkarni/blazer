//! Low-level Rust FFI bindings to the `blazer_mlx_shim` C library.
//!
//! All functions are `unsafe`.  Higher-level safe wrappers live in
//! `blazer-engine`'s `mlx_backend` module.

#![allow(non_camel_case_types)]

use std::os::raw::{c_double, c_float};

/// Opaque pointer to a heap-allocated `mlx::core::array`.
pub type MlxArrayRaw = *mut std::ffi::c_void;

#[link(name = "blazer_mlx_shim", kind = "static")]
extern "C" {
    // ----------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------
    pub fn blazer_mlx_from_f32(data: *const c_float, n: usize) -> MlxArrayRaw;
    pub fn blazer_mlx_from_f64(data: *const c_double, n: usize) -> MlxArrayRaw;
    pub fn blazer_mlx_from_i32(data: *const i32, n: usize) -> MlxArrayRaw;
    pub fn blazer_mlx_from_i64(data: *const i64, n: usize) -> MlxArrayRaw;
    pub fn blazer_mlx_from_bool(data: *const u8, n: usize) -> MlxArrayRaw;
    pub fn blazer_mlx_from_u32(data: *const u32, n: usize) -> MlxArrayRaw;

    pub fn blazer_mlx_array_free(arr: MlxArrayRaw);
    pub fn blazer_mlx_array_size(arr: MlxArrayRaw) -> usize;

    // ----------------------------------------------------------------
    // Copy back
    // ----------------------------------------------------------------
    pub fn blazer_mlx_copy_f32(arr: MlxArrayRaw, out: *mut c_float, n: usize);
    pub fn blazer_mlx_copy_f64(arr: MlxArrayRaw, out: *mut c_double, n: usize);
    pub fn blazer_mlx_copy_i32(arr: MlxArrayRaw, out: *mut i32, n: usize);
    pub fn blazer_mlx_copy_i64(arr: MlxArrayRaw, out: *mut i64, n: usize);
    pub fn blazer_mlx_copy_bool(arr: MlxArrayRaw, out: *mut u8, n: usize);
    pub fn blazer_mlx_copy_u32(arr: MlxArrayRaw, out: *mut u32, n: usize);

    // ----------------------------------------------------------------
    // Arithmetic
    // ----------------------------------------------------------------
    pub fn blazer_mlx_add(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_sub(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_mul(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_div(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_rem(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;

    // ----------------------------------------------------------------
    // Comparison
    // ----------------------------------------------------------------
    pub fn blazer_mlx_eq(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_neq(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_lt(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_lte(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_gt(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_gte(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;

    // ----------------------------------------------------------------
    // Logical
    // ----------------------------------------------------------------
    pub fn blazer_mlx_logical_and(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_logical_or(a: MlxArrayRaw, b: MlxArrayRaw) -> MlxArrayRaw;

    // ----------------------------------------------------------------
    // Aggregation (scalar result as f64)
    // ----------------------------------------------------------------
    pub fn blazer_mlx_sum_f64(a: MlxArrayRaw) -> c_double;
    pub fn blazer_mlx_mean_f64(a: MlxArrayRaw) -> c_double;
    pub fn blazer_mlx_min_f64(a: MlxArrayRaw) -> c_double;
    pub fn blazer_mlx_max_f64(a: MlxArrayRaw) -> c_double;

    // ----------------------------------------------------------------
    // Sort / argsort
    // ----------------------------------------------------------------
    pub fn blazer_mlx_sort(a: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_sort_desc(a: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_argsort(a: MlxArrayRaw) -> MlxArrayRaw;
    pub fn blazer_mlx_argsort_desc(a: MlxArrayRaw) -> MlxArrayRaw;

    pub fn blazer_mlx_astype_f64(a: MlxArrayRaw) -> MlxArrayRaw;
}
