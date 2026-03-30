// MLX C++ bridge implementation for Blazer.
// This wraps the MLX C++ API in a C-compatible API defined in blaze_mlx.h.
//
// Build: cmake -B build && cmake --build build
// Requires: MLX installed (pip install mlx)

#include "blaze_mlx.h"

#ifdef __APPLE__

#include <mlx/mlx.h>
#include <cstring>
#include <cstdlib>

namespace mx = mlx::core;

// ---- Internal: BlazeArray wraps an mlx::core::array ----

struct BlazeArray {
    mx::array arr;
    BlazeDType dtype;

    BlazeArray(mx::array a, BlazeDType dt) : arr(std::move(a)), dtype(dt) {}
};

static mx::Dtype to_mlx_dtype(BlazeDType dt) {
    switch (dt) {
        case BLAZE_FLOAT16: return mx::float16;
        case BLAZE_FLOAT32: return mx::float32;
        case BLAZE_INT8:    return mx::int8;
        case BLAZE_INT16:   return mx::int16;
        case BLAZE_INT32:   return mx::int32;
        case BLAZE_INT64:   return mx::int64;
        case BLAZE_UINT8:   return mx::uint8;
        case BLAZE_UINT16:  return mx::uint16;
        case BLAZE_UINT32:  return mx::uint32;
        case BLAZE_BOOL:    return mx::bool_;
        default:            return mx::float32;
    }
}

static size_t dtype_size(BlazeDType dt) {
    switch (dt) {
        case BLAZE_FLOAT16: return 2;
        case BLAZE_FLOAT32: return 4;
        case BLAZE_FLOAT64: return 8;
        case BLAZE_INT8: case BLAZE_UINT8: case BLAZE_BOOL: return 1;
        case BLAZE_INT16: case BLAZE_UINT16: return 2;
        case BLAZE_INT32: case BLAZE_UINT32: return 4;
        case BLAZE_INT64: case BLAZE_UINT64: return 8;
        default: return 4;
    }
}

// ---- Device info ----

extern "C" BlazeDeviceInfo blaze_device_info(void) {
    BlazeDeviceInfo info;
    info.name = "Apple Silicon (MLX)";
    info.has_mlx = 1;
    info.has_gpu = 1;
    info.memory_bytes = 0; // TODO: query actual memory
    return info;
}

// ---- Array creation ----

extern "C" BlazeArray* blaze_array_from_buffer(const void* data, size_t count, BlazeDType dtype) {
    if (dtype == BLAZE_FLOAT64) {
        // MLX doesn't support float64 natively; convert to float32
        const double* src = static_cast<const double*>(data);
        std::vector<float> f32(count);
        for (size_t i = 0; i < count; i++) f32[i] = static_cast<float>(src[i]);
        auto arr = mx::array(f32.data(), {static_cast<int>(count)}, mx::float32);
        return new BlazeArray(std::move(arr), dtype);
    }
    if (dtype == BLAZE_UINT64) {
        // MLX doesn't support uint64; convert to int64
        const uint64_t* src = static_cast<const uint64_t*>(data);
        std::vector<int64_t> i64(count);
        for (size_t i = 0; i < count; i++) i64[i] = static_cast<int64_t>(src[i]);
        auto arr = mx::array(i64.data(), {static_cast<int>(count)}, mx::int64);
        return new BlazeArray(std::move(arr), dtype);
    }

    auto mlx_dt = to_mlx_dtype(dtype);
    auto arr = mx::array(data, {static_cast<int>(count)}, mlx_dt);
    return new BlazeArray(std::move(arr), dtype);
}

extern "C" BlazeArray* blaze_array_empty(size_t count, BlazeDType dtype) {
    auto mlx_dt = to_mlx_dtype(dtype);
    auto arr = mx::zeros({static_cast<int>(count)}, mlx_dt);
    return new BlazeArray(std::move(arr), dtype);
}

extern "C" void blaze_array_free(BlazeArray* arr) {
    delete arr;
}

// ---- Array properties ----

extern "C" size_t blaze_array_size(const BlazeArray* arr) {
    return arr->arr.size();
}

extern "C" BlazeDType blaze_array_dtype(const BlazeArray* arr) {
    return arr->dtype;
}

// ---- Data transfer ----

extern "C" void blaze_eval(BlazeArray* arr) {
    mx::eval(arr->arr);
}

extern "C" void blaze_array_to_buffer(const BlazeArray* arr, void* out, size_t byte_len) {
    mx::eval(arr->arr);
    auto& a = arr->arr;

    if (arr->dtype == BLAZE_FLOAT64) {
        // Convert back from float32 to float64
        const float* src = a.data<float>();
        double* dst = static_cast<double*>(out);
        size_t count = a.size();
        for (size_t i = 0; i < count && i * 8 < byte_len; i++) {
            dst[i] = static_cast<double>(src[i]);
        }
        return;
    }

    size_t copy_size = a.nbytes();
    if (copy_size > byte_len) copy_size = byte_len;
    std::memcpy(out, a.data<void>(), copy_size);
}

// ---- Arithmetic ----

extern "C" BlazeArray* blaze_add(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::add(a->arr, b->arr);
    return new BlazeArray(std::move(result), a->dtype);
}

extern "C" BlazeArray* blaze_sub(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::subtract(a->arr, b->arr);
    return new BlazeArray(std::move(result), a->dtype);
}

extern "C" BlazeArray* blaze_mul(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::multiply(a->arr, b->arr);
    return new BlazeArray(std::move(result), a->dtype);
}

extern "C" BlazeArray* blaze_div(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::divide(a->arr, b->arr);
    return new BlazeArray(std::move(result), a->dtype);
}

// ---- Reductions ----

extern "C" BlazeArray* blaze_sum(const BlazeArray* a) {
    auto result = mx::sum(a->arr);
    return new BlazeArray(std::move(result), a->dtype);
}

extern "C" BlazeArray* blaze_mean(const BlazeArray* a) {
    auto result = mx::mean(a->arr);
    return new BlazeArray(std::move(result), BLAZE_FLOAT32);
}

extern "C" BlazeArray* blaze_min(const BlazeArray* a) {
    auto result = mx::min(a->arr);
    return new BlazeArray(std::move(result), a->dtype);
}

extern "C" BlazeArray* blaze_max(const BlazeArray* a) {
    auto result = mx::max(a->arr);
    return new BlazeArray(std::move(result), a->dtype);
}

// ---- Comparison ----

extern "C" BlazeArray* blaze_eq(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::equal(a->arr, b->arr);
    return new BlazeArray(std::move(result), BLAZE_BOOL);
}

extern "C" BlazeArray* blaze_neq(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::not_equal(a->arr, b->arr);
    return new BlazeArray(std::move(result), BLAZE_BOOL);
}

extern "C" BlazeArray* blaze_lt(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::less(a->arr, b->arr);
    return new BlazeArray(std::move(result), BLAZE_BOOL);
}

extern "C" BlazeArray* blaze_gt(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::greater(a->arr, b->arr);
    return new BlazeArray(std::move(result), BLAZE_BOOL);
}

// ---- Sort ----

extern "C" BlazeArray* blaze_sort(const BlazeArray* a, int descending) {
    auto result = mx::sort(a->arr);
    if (descending) {
        // Reverse the sorted array
        result = mx::slice(result, {static_cast<int>(a->arr.size()) - 1}, {-1}, {-1});
    }
    return new BlazeArray(std::move(result), a->dtype);
}

extern "C" BlazeArray* blaze_argsort(const BlazeArray* a, int descending) {
    auto result = mx::argsort(a->arr);
    if (descending) {
        result = mx::slice(result, {static_cast<int>(a->arr.size()) - 1}, {-1}, {-1});
    }
    return new BlazeArray(std::move(result), BLAZE_INT32);
}

// ---- Logical ----

extern "C" BlazeArray* blaze_logical_and(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::logical_and(a->arr, b->arr);
    return new BlazeArray(std::move(result), BLAZE_BOOL);
}

extern "C" BlazeArray* blaze_logical_or(const BlazeArray* a, const BlazeArray* b) {
    auto result = mx::logical_or(a->arr, b->arr);
    return new BlazeArray(std::move(result), BLAZE_BOOL);
}

extern "C" BlazeArray* blaze_logical_not(const BlazeArray* a) {
    auto result = mx::logical_not(a->arr);
    return new BlazeArray(std::move(result), BLAZE_BOOL);
}

// ---- Filter ----

extern "C" BlazeArray* blaze_filter(const BlazeArray* data, const BlazeArray* mask) {
    // Use nonzero to get indices where mask is true, then gather
    auto indices = mx::flatten(mx::argwhere(mask->arr));
    auto result = mx::take(data->arr, indices);
    return new BlazeArray(std::move(result), data->dtype);
}

// ---- Rolling window using convolution ----

extern "C" BlazeArray* blaze_rolling_with_conv(const BlazeArray* a, size_t window_size, int op) {
    // Simple rolling mean using 1D convolution
    int ws = static_cast<int>(window_size);
    auto kernel = mx::ones({ws}, mx::float32) / static_cast<float>(ws);

    // Pad input for "valid" convolution behavior
    auto input = mx::reshape(a->arr, {1, 1, static_cast<int>(a->arr.size())});
    auto kern = mx::reshape(kernel, {1, 1, ws});

    // Use matmul-based sliding window for simplicity
    // For now, just compute rolling mean on CPU-side
    mx::eval(a->arr);
    int n = static_cast<int>(a->arr.size());
    std::vector<float> result(n, 0.0f);
    const float* data = a->arr.data<float>();

    for (int i = 0; i < n; i++) {
        if (i + 1 < ws) {
            result[i] = std::numeric_limits<float>::quiet_NaN();
        } else {
            float sum = 0.0f;
            for (int j = i - ws + 1; j <= i; j++) {
                sum += data[j];
            }
            result[i] = sum / static_cast<float>(ws);
        }
    }

    auto out = mx::array(result.data(), {n}, mx::float32);
    return new BlazeArray(std::move(out), BLAZE_FLOAT32);
}

// ---- Group-by helpers ----

extern "C" BlazeArray* blaze_group_encode(const BlazeArray* keys) {
    // Simple group encoding: return unique indices
    // This is a simplified version; full implementation would need hash-based grouping
    auto [unique_vals, inverse] = mx::unique(keys->arr, true);
    return new BlazeArray(std::move(inverse), BLAZE_INT32);
}

extern "C" BlazeArray* blaze_group_sum(const BlazeArray* data, const BlazeArray* group_ids, size_t n_groups) {
    // Scatter-add: sum data by group_ids
    mx::eval(data->arr);
    mx::eval(group_ids->arr);

    int ng = static_cast<int>(n_groups);
    std::vector<float> sums(ng, 0.0f);
    const float* d = data->arr.data<float>();
    const int32_t* g = group_ids->arr.data<int32_t>();

    for (int i = 0; i < static_cast<int>(data->arr.size()); i++) {
        if (g[i] >= 0 && g[i] < ng) {
            sums[g[i]] += d[i];
        }
    }

    auto out = mx::array(sums.data(), {ng}, mx::float32);
    return new BlazeArray(std::move(out), BLAZE_FLOAT32);
}

extern "C" BlazeArray* blaze_group_mean(const BlazeArray* data, const BlazeArray* group_ids, size_t n_groups) {
    mx::eval(data->arr);
    mx::eval(group_ids->arr);

    int ng = static_cast<int>(n_groups);
    std::vector<float> sums(ng, 0.0f);
    std::vector<int> counts(ng, 0);
    const float* d = data->arr.data<float>();
    const int32_t* g = group_ids->arr.data<int32_t>();

    for (int i = 0; i < static_cast<int>(data->arr.size()); i++) {
        if (g[i] >= 0 && g[i] < ng) {
            sums[g[i]] += d[i];
            counts[g[i]]++;
        }
    }

    std::vector<float> means(ng);
    for (int i = 0; i < ng; i++) {
        means[i] = counts[i] > 0 ? sums[i] / counts[i] : 0.0f;
    }

    auto out = mx::array(means.data(), {ng}, mx::float32);
    return new BlazeArray(std::move(out), BLAZE_FLOAT32);
}

#else
// Non-Apple platforms: stub implementations

extern "C" BlazeDeviceInfo blaze_device_info(void) {
    BlazeDeviceInfo info;
    info.name = "CPU (no MLX)";
    info.has_mlx = 0;
    info.has_gpu = 0;
    info.memory_bytes = 0;
    return info;
}

extern "C" BlazeArray* blaze_array_from_buffer(const void* data, size_t count, BlazeDType dtype) {
    (void)data; (void)count; (void)dtype;
    return NULL;
}

extern "C" BlazeArray* blaze_array_empty(size_t count, BlazeDType dtype) {
    (void)count; (void)dtype;
    return NULL;
}

extern "C" void blaze_array_free(BlazeArray* arr) { (void)arr; }
extern "C" size_t blaze_array_size(const BlazeArray* arr) { (void)arr; return 0; }
extern "C" BlazeDType blaze_array_dtype(const BlazeArray* arr) { (void)arr; return BLAZE_FLOAT32; }
extern "C" void blaze_array_to_buffer(const BlazeArray* arr, void* out, size_t byte_len) {
    (void)arr; (void)out; (void)byte_len;
}
extern "C" void blaze_eval(BlazeArray* arr) { (void)arr; }
extern "C" BlazeArray* blaze_add(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_sub(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_mul(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_div(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_sum(const BlazeArray* a) { (void)a; return NULL; }
extern "C" BlazeArray* blaze_mean(const BlazeArray* a) { (void)a; return NULL; }
extern "C" BlazeArray* blaze_min(const BlazeArray* a) { (void)a; return NULL; }
extern "C" BlazeArray* blaze_max(const BlazeArray* a) { (void)a; return NULL; }
extern "C" BlazeArray* blaze_eq(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_neq(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_lt(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_gt(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_sort(const BlazeArray* a, int d) { (void)a; (void)d; return NULL; }
extern "C" BlazeArray* blaze_argsort(const BlazeArray* a, int d) { (void)a; (void)d; return NULL; }
extern "C" BlazeArray* blaze_logical_and(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_logical_or(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_logical_not(const BlazeArray* a) { (void)a; return NULL; }
extern "C" BlazeArray* blaze_filter(const BlazeArray* a, const BlazeArray* b) { (void)a; (void)b; return NULL; }
extern "C" BlazeArray* blaze_rolling_with_conv(const BlazeArray* a, size_t w, int o) { (void)a; (void)w; (void)o; return NULL; }
extern "C" BlazeArray* blaze_group_encode(const BlazeArray* a) { (void)a; return NULL; }
extern "C" BlazeArray* blaze_group_sum(const BlazeArray* a, const BlazeArray* b, size_t n) { (void)a; (void)b; (void)n; return NULL; }
extern "C" BlazeArray* blaze_group_mean(const BlazeArray* a, const BlazeArray* b, size_t n) { (void)a; (void)b; (void)n; return NULL; }

#endif
