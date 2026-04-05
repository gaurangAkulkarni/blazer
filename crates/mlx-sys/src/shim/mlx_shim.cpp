/**
 * blazer MLX shim — C++ wrappers around mlx::core so Rust can call them
 * via a plain C ABI (no name mangling, no exceptions crossing the boundary).
 *
 * Convention:
 *   - mlx_array_t  = new-heap-allocated mlx::core::array*
 *   - Every returned mlx_array_t must be released with blazer_mlx_array_free.
 *   - All ops call array.eval() (or mlx::core::eval()) before copying data
 *     back to ensure lazy computation has been dispatched to Metal / CPU.
 */

#include "mlx_shim.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

/* Pull in the full MLX public API. */
#include <mlx/mlx.h>

namespace mx = mlx::core;

/* Helper: wrap a stack array into a heap-allocated handle. */
static inline mlx_array_t wrap(mx::array arr) {
    return static_cast<mlx_array_t>(new mx::array(std::move(arr)));
}

static inline mx::array& ref(mlx_array_t h) {
    return *static_cast<mx::array*>(h);
}

/* ------------------------------------------------------------------ */
/* Construction                                                         */
/* ------------------------------------------------------------------ */

extern "C" {

mlx_array_t blazer_mlx_from_f32(const float* data, size_t n) {
    std::vector<float> v(data, data + n);
    return wrap(mx::array(v.data(), {static_cast<int>(n)}, mx::float32));
}

mlx_array_t blazer_mlx_from_f64(const double* data, size_t n) {
    std::vector<double> v(data, data + n);
    return wrap(mx::array(v.data(), {static_cast<int>(n)}, mx::float64));
}

mlx_array_t blazer_mlx_from_i32(const int32_t* data, size_t n) {
    std::vector<int32_t> v(data, data + n);
    return wrap(mx::array(v.data(), {static_cast<int>(n)}, mx::int32));
}

mlx_array_t blazer_mlx_from_i64(const int64_t* data, size_t n) {
    std::vector<int64_t> v(data, data + n);
    return wrap(mx::array(v.data(), {static_cast<int>(n)}, mx::int64));
}

mlx_array_t blazer_mlx_from_bool(const uint8_t* data, size_t n) {
    /* Arrow stores bools as bytes (one byte per value in validity buffers,
       but for value arrays we use uint8 where 0==false, nonzero==true). */
    std::vector<bool> v(n);
    for (size_t i = 0; i < n; ++i) v[i] = data[i] != 0;
    /* MLX wants bool stored as uint8; use astype after creating uint8 array. */
    mx::array u8 = mx::array(data, {static_cast<int>(n)}, mx::uint8);
    return wrap(mx::astype(u8, mx::bool_, mx::Device::cpu));
}

mlx_array_t blazer_mlx_from_u32(const uint32_t* data, size_t n) {
    std::vector<uint32_t> v(data, data + n);
    return wrap(mx::array(v.data(), {static_cast<int>(n)}, mx::uint32));
}

void blazer_mlx_array_free(mlx_array_t arr) {
    delete static_cast<mx::array*>(arr);
}

size_t blazer_mlx_array_size(mlx_array_t arr) {
    return ref(arr).size();
}

/* ------------------------------------------------------------------ */
/* Copy back                                                            */
/* ------------------------------------------------------------------ */

/* Macro: eval, cast if needed, then memcpy. */
#define COPY_BACK(T, mlx_dtype, arr, out, n)          \
    do {                                               \
        mx::array& a = ref(arr);                       \
        a.eval();                                      \
        mx::array casted = mx::astype(a, mlx_dtype);  \
        casted.eval();                                 \
        std::memcpy(out, casted.data<T>(), (n) * sizeof(T)); \
    } while (0)

void blazer_mlx_copy_f32(mlx_array_t arr, float* out, size_t n) {
    COPY_BACK(float, mx::float32, arr, out, n);
}
void blazer_mlx_copy_f64(mlx_array_t arr, double* out, size_t n) {
    COPY_BACK(double, mx::float64, arr, out, n);
}
void blazer_mlx_copy_i32(mlx_array_t arr, int32_t* out, size_t n) {
    COPY_BACK(int32_t, mx::int32, arr, out, n);
}
void blazer_mlx_copy_i64(mlx_array_t arr, int64_t* out, size_t n) {
    COPY_BACK(int64_t, mx::int64, arr, out, n);
}
void blazer_mlx_copy_bool(mlx_array_t arr, uint8_t* out, size_t n) {
    mx::array& a = ref(arr);
    a.eval();
    mx::array u8 = mx::astype(a, mx::uint8);
    u8.eval();
    std::memcpy(out, u8.data<uint8_t>(), n);
}
void blazer_mlx_copy_u32(mlx_array_t arr, uint32_t* out, size_t n) {
    COPY_BACK(uint32_t, mx::uint32, arr, out, n);
}

/* ------------------------------------------------------------------ */
/* Arithmetic                                                           */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_add(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::add(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_sub(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::subtract(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_mul(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::multiply(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_div(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::divide(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_rem(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::remainder(ref(a), ref(b)));
}

/* ------------------------------------------------------------------ */
/* Comparison                                                           */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_eq(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::equal(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_neq(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::not_equal(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_lt(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::less(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_lte(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::less_equal(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_gt(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::greater(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_gte(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::greater_equal(ref(a), ref(b)));
}

/* ------------------------------------------------------------------ */
/* Logical                                                              */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_logical_and(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::logical_and(ref(a), ref(b)));
}
mlx_array_t blazer_mlx_logical_or(mlx_array_t a, mlx_array_t b) {
    return wrap(mx::logical_or(ref(a), ref(b)));
}

/* ------------------------------------------------------------------ */
/* Aggregation                                                          */
/* ------------------------------------------------------------------ */

static inline double scalar_f64(mx::array arr) {
    arr.eval();
    mx::array f64 = mx::astype(arr, mx::float64);
    f64.eval();
    return f64.item<double>();
}

double blazer_mlx_sum_f64(mlx_array_t a) {
    return scalar_f64(mx::sum(ref(a), false));
}
double blazer_mlx_mean_f64(mlx_array_t a) {
    return scalar_f64(mx::mean(ref(a), false));
}
double blazer_mlx_min_f64(mlx_array_t a) {
    return scalar_f64(mx::min(ref(a), false));
}
double blazer_mlx_max_f64(mlx_array_t a) {
    return scalar_f64(mx::max(ref(a), false));
}

/* ------------------------------------------------------------------ */
/* Sort / argsort                                                       */
/* ------------------------------------------------------------------ */

mlx_array_t blazer_mlx_sort(mlx_array_t a) {
    return wrap(mx::sort(ref(a)));
}

mlx_array_t blazer_mlx_sort_desc(mlx_array_t a) {
    /* MLX has no native descending sort.
       Strategy: negate, sort ascending, negate back.
       Works for all numeric dtypes; float NaN behaviour mirrors sort(). */
    mx::array neg = mx::subtract(mx::array(0.0f, mx::float64), ref(a));
    mx::array sorted_neg = mx::sort(neg);
    mx::array result = mx::subtract(mx::array(0.0f, mx::float64), sorted_neg);
    return wrap(result);
}

mlx_array_t blazer_mlx_argsort(mlx_array_t a) {
    return wrap(mx::argsort(ref(a)));
}

mlx_array_t blazer_mlx_argsort_desc(mlx_array_t a) {
    /* Argsort of negated array gives descending order indices. */
    mx::array neg = mx::subtract(mx::array(0.0f, mx::float64), ref(a));
    return wrap(mx::argsort(neg));
}

mlx_array_t blazer_mlx_astype_f64(mlx_array_t a) {
    return wrap(mx::astype(ref(a), mx::float64));
}

} /* extern "C" */
