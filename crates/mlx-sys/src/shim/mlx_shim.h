#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle to an mlx::core::array heap-allocated object. */
typedef void* mlx_array_t;

/* ------------------------------------------------------------------ */
/* Construction / destruction                                           */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_from_f32(const float* data, size_t n);
mlx_array_t blazer_mlx_from_f64(const double* data, size_t n);
mlx_array_t blazer_mlx_from_i32(const int32_t* data, size_t n);
mlx_array_t blazer_mlx_from_i64(const int64_t* data, size_t n);
mlx_array_t blazer_mlx_from_bool(const uint8_t* data, size_t n);
mlx_array_t blazer_mlx_from_u32(const uint32_t* data, size_t n);

void blazer_mlx_array_free(mlx_array_t arr);

/* Number of elements in the array. */
size_t blazer_mlx_array_size(mlx_array_t arr);

/* ------------------------------------------------------------------ */
/* Copy results back to host buffers                                   */
/* ------------------------------------------------------------------ */

void blazer_mlx_copy_f32(mlx_array_t arr, float* out, size_t n);
void blazer_mlx_copy_f64(mlx_array_t arr, double* out, size_t n);
void blazer_mlx_copy_i32(mlx_array_t arr, int32_t* out, size_t n);
void blazer_mlx_copy_i64(mlx_array_t arr, int64_t* out, size_t n);
void blazer_mlx_copy_bool(mlx_array_t arr, uint8_t* out, size_t n);
void blazer_mlx_copy_u32(mlx_array_t arr, uint32_t* out, size_t n);

/* ------------------------------------------------------------------ */
/* Arithmetic (element-wise)                                           */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_add(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_sub(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_mul(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_div(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_rem(mlx_array_t a, mlx_array_t b);

/* ------------------------------------------------------------------ */
/* Comparison (element-wise → bool array)                              */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_eq(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_neq(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_lt(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_lte(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_gt(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_gte(mlx_array_t a, mlx_array_t b);

/* ------------------------------------------------------------------ */
/* Logical (element-wise → bool array)                                 */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_logical_and(mlx_array_t a, mlx_array_t b);
mlx_array_t blazer_mlx_logical_or(mlx_array_t a, mlx_array_t b);

/* ------------------------------------------------------------------ */
/* Aggregation (returns scalar as double)                              */
/* ------------------------------------------------------------------ */

double blazer_mlx_sum_f64(mlx_array_t a);
double blazer_mlx_mean_f64(mlx_array_t a);
double blazer_mlx_min_f64(mlx_array_t a);
double blazer_mlx_max_f64(mlx_array_t a);

/* ------------------------------------------------------------------ */
/* Sort / argsort                                                       */
/* ------------------------------------------------------------------ */

/* Returns a new sorted array (ascending). Caller must free. */
mlx_array_t blazer_mlx_sort(mlx_array_t a);
/* Returns a new sorted array (descending). Caller must free. */
mlx_array_t blazer_mlx_sort_desc(mlx_array_t a);
/* Returns uint32 index array (ascending argsort). Caller must free. */
mlx_array_t blazer_mlx_argsort(mlx_array_t a);
/* Returns uint32 index array (descending argsort). Caller must free. */
mlx_array_t blazer_mlx_argsort_desc(mlx_array_t a);

/* Cast the array to float64 (for aggregations on integer arrays). */
mlx_array_t blazer_mlx_astype_f64(mlx_array_t a);

#ifdef __cplusplus
} /* extern "C" */
#endif
