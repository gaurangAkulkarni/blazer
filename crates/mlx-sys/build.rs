use std::env;
use std::path::PathBuf;

fn main() {
    // ----------------------------------------------------------------
    // Locate MLX installation.
    // Priority:
    //   1. $MLX_ROOT  (user-override)
    //   2. /opt/homebrew  (Apple Silicon brew default)
    //   3. /usr/local    (Intel brew / manual install)
    // ----------------------------------------------------------------
    let mlx_root: PathBuf = env::var("MLX_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            for candidate in &["/opt/homebrew", "/usr/local"] {
                let p = PathBuf::from(candidate);
                if p.join("include/mlx/mlx.h").exists() {
                    return p;
                }
            }
            panic!(
                "MLX not found. Install with `brew install mlx` or set $MLX_ROOT."
            );
        });

    let mlx_include = mlx_root.join("include");
    let mlx_lib = mlx_root.join("lib");

    // ----------------------------------------------------------------
    // Compile the C++ shim.
    // ----------------------------------------------------------------
    cc::Build::new()
        .file("src/shim/mlx_shim.cpp")
        .include(&mlx_include)
        // MLX requires C++17.
        .flag("-std=c++17")
        // Suppress common warnings from third-party headers.
        .flag("-Wno-deprecated-declarations")
        .flag("-Wno-unused-parameter")
        .cpp(true)
        .compile("blazer_mlx_shim");

    // ----------------------------------------------------------------
    // Link against libmlx and the required Apple frameworks.
    // ----------------------------------------------------------------
    println!("cargo:rustc-link-search=native={}", mlx_lib.display());
    println!("cargo:rustc-link-lib=dylib=mlx");

    // Metal + Foundation are required by MLX's Metal backend.
    println!("cargo:rustc-link-lib=framework=Metal");
    println!("cargo:rustc-link-lib=framework=Foundation");
    println!("cargo:rustc-link-lib=framework=Accelerate");

    // Re-run build script if the shim sources change.
    println!("cargo:rerun-if-changed=src/shim/mlx_shim.cpp");
    println!("cargo:rerun-if-changed=src/shim/mlx_shim.h");
    println!("cargo:rerun-if-env-changed=MLX_ROOT");
}
