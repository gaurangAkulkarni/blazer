#ifndef BLAZE_MLX_H
#define BLAZE_MLX_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

// ---- Data types ----
typedef enum {
    BLAZE_FLOAT16 = 0,
    BLAZE_FLOAT32 = 1,
    BLAZE_FLOAT64 = 2,
    BLAZE_INT8    = 3,
    BLAZE_INT16   = 4,
    BLAZE_INT32   = 5,
    BLAZE_INT64   = 6,
    BLAZE_UINT8   = 7,
    BLAZE_UINT16  = 8,
    BLAZE_UINT32  = 9,
    BLAZE_UINT64  = 10,
    BLAZE_BOOL    = 11,
} BlazeDType;

// ---- Opaque array handle ----
typedef struct BlazeArray BlazeArray;

// ---- Device info ----
typedef struct {
    const char* name;
    int has_mlx;
    int has_gpu;
    size_t memory_bytes;
} BlazeDeviceInfo;

// ---- Device ----
BlazeDeviceInfo blaze_device_info(void);

// ---- Array creation / destruction ----
BlazeArray* blaze_array_from_buffer(const void* data, size_t count, BlazeDType dtype);
BlazeArray* blaze_array_empty(size_t count, BlazeDType dtype);
void        blaze_array_free(BlazeArray* arr);

// ---- Array properties ----
size_t     blaze_array_size(const BlazeArray* arr);
BlazeDType blaze_array_dtype(const BlazeArray* arr);

// ---- Data transfer ----
void blaze_array_to_buffer(const BlazeArray* arr, void* out, size_t byte_len);
void blaze_eval(BlazeArray* arr);

// ---- Arithmetic (element-wise) ----
BlazeArray* blaze_add(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_sub(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_mul(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_div(const BlazeArray* a, const BlazeArray* b);

// ---- Reductions ----
BlazeArray* blaze_sum(const BlazeArray* a);
BlazeArray* blaze_mean(const BlazeArray* a);
BlazeArray* blaze_min(const BlazeArray* a);
BlazeArray* blaze_max(const BlazeArray* a);

// ---- Comparison ----
BlazeArray* blaze_eq(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_neq(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_lt(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_gt(const BlazeArray* a, const BlazeArray* b);

// ---- Sort ----
BlazeArray* blaze_sort(const BlazeArray* a, int descending);
BlazeArray* blaze_argsort(const BlazeArray* a, int descending);

// ---- Logical ----
BlazeArray* blaze_logical_and(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_logical_or(const BlazeArray* a, const BlazeArray* b);
BlazeArray* blaze_logical_not(const BlazeArray* a);

// ---- Filter ----
BlazeArray* blaze_filter(const BlazeArray* data, const BlazeArray* mask);

// ---- Rolling window ----
BlazeArray* blaze_rolling_with_conv(const BlazeArray* a, size_t window_size, int op);

// ---- Group-by helpers ----
BlazeArray* blaze_group_encode(const BlazeArray* keys);
BlazeArray* blaze_group_sum(const BlazeArray* data, const BlazeArray* group_ids, size_t n_groups);
BlazeArray* blaze_group_mean(const BlazeArray* data, const BlazeArray* group_ids, size_t n_groups);

#ifdef __cplusplus
}
#endif

#endif // BLAZE_MLX_H
