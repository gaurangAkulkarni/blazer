use std::sync::OnceLock;

use crate::error::Result;
use crate::series::Series;

/// Preference for which compute backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendPreference {
    Cpu,
    #[cfg(all(target_os = "macos", feature = "mlx"))]
    Mlx,
    #[cfg(feature = "cuda")]
    Cuda,
}

/// The compute backend trait. All GPU-acceleratable operations are defined here.
pub trait ComputeBackend: Send + Sync {
    fn name(&self) -> &str;

    // Arithmetic
    fn add(&self, left: &Series, right: &Series) -> Result<Series>;
    fn sub(&self, left: &Series, right: &Series) -> Result<Series>;
    fn mul(&self, left: &Series, right: &Series) -> Result<Series>;
    fn div(&self, left: &Series, right: &Series) -> Result<Series>;
    fn modulo(&self, left: &Series, right: &Series) -> Result<Series>;

    // Comparison
    fn eq_series(&self, left: &Series, right: &Series) -> Result<Series>;
    fn neq_series(&self, left: &Series, right: &Series) -> Result<Series>;
    fn lt_series(&self, left: &Series, right: &Series) -> Result<Series>;
    fn lte_series(&self, left: &Series, right: &Series) -> Result<Series>;
    fn gt_series(&self, left: &Series, right: &Series) -> Result<Series>;
    fn gte_series(&self, left: &Series, right: &Series) -> Result<Series>;

    // Logical
    fn and_series(&self, left: &Series, right: &Series) -> Result<Series>;
    fn or_series(&self, left: &Series, right: &Series) -> Result<Series>;

    // Aggregation
    fn sum(&self, series: &Series) -> Result<f64>;
    fn mean(&self, series: &Series) -> Result<f64>;
    fn min(&self, series: &Series) -> Result<f64>;
    fn max(&self, series: &Series) -> Result<f64>;

    // Sort
    fn sort(&self, series: &Series, descending: bool) -> Result<Series>;
    fn argsort(&self, series: &Series, descending: bool) -> Result<arrow2::array::PrimitiveArray<u32>>;
}

/// CPU backend implementation — pure Arrow2/Rust computation.
pub struct CpuBackend;

impl ComputeBackend for CpuBackend {
    fn name(&self) -> &str {
        "cpu"
    }

    fn add(&self, left: &Series, right: &Series) -> Result<Series> {
        left.add(right)
    }

    fn sub(&self, left: &Series, right: &Series) -> Result<Series> {
        left.sub(right)
    }

    fn mul(&self, left: &Series, right: &Series) -> Result<Series> {
        left.mul(right)
    }

    fn div(&self, left: &Series, right: &Series) -> Result<Series> {
        left.div(right)
    }

    fn modulo(&self, left: &Series, right: &Series) -> Result<Series> {
        left.modulo(right)
    }

    fn eq_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.eq_series(right)
    }

    fn neq_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.neq_series(right)
    }

    fn lt_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.lt_series(right)
    }

    fn lte_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.lte_series(right)
    }

    fn gt_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.gt_series(right)
    }

    fn gte_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.gte_series(right)
    }

    fn and_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.and_series(right)
    }

    fn or_series(&self, left: &Series, right: &Series) -> Result<Series> {
        left.or_series(right)
    }

    fn sum(&self, series: &Series) -> Result<f64> {
        series.sum_as_f64()
    }

    fn mean(&self, series: &Series) -> Result<f64> {
        series.mean_as_f64()
    }

    fn min(&self, series: &Series) -> Result<f64> {
        series.min_as_f64()
    }

    fn max(&self, series: &Series) -> Result<f64> {
        series.max_as_f64()
    }

    fn sort(&self, series: &Series, descending: bool) -> Result<Series> {
        series.sort(descending)
    }

    fn argsort(&self, series: &Series, descending: bool) -> Result<arrow2::array::PrimitiveArray<u32>> {
        series.argsort(descending)
    }
}

// TODO: MLX backend (macOS only, behind feature flag)
#[cfg(all(target_os = "macos", feature = "mlx"))]
pub struct MlxBackend;

// TODO: CUDA backend (behind feature flag)

// ---- Global backend singleton ----

static GLOBAL_BACKEND: OnceLock<Box<dyn ComputeBackend>> = OnceLock::new();

/// Initialize the global compute backend.
pub fn init_backend(pref: BackendPreference) {
    let backend: Box<dyn ComputeBackend> = match pref {
        BackendPreference::Cpu => Box::new(CpuBackend),
        #[cfg(all(target_os = "macos", feature = "mlx"))]
        BackendPreference::Mlx => {
            // TODO: Initialize MLX backend
            Box::new(CpuBackend) // fallback for now
        }
        #[cfg(feature = "cuda")]
        BackendPreference::Cuda => {
            // TODO: Initialize CUDA backend
            Box::new(CpuBackend) // fallback for now
        }
    };
    let _ = GLOBAL_BACKEND.set(backend);
}

/// Get the global compute backend. Initializes to CPU if not yet set.
pub fn global_backend() -> &'static dyn ComputeBackend {
    GLOBAL_BACKEND.get_or_init(|| Box::new(CpuBackend)).as_ref()
}
