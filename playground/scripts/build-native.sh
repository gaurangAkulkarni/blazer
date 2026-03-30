#!/bin/bash
set -e

# Build the blazer-node native addon
echo "Building blazer-node native addon..."
cd "$(dirname "$0")/../../crates/node-bindings"

# Build with cargo (produces .dylib/.so/.dll)
cargo build --release

echo "Native addon built successfully."
echo "Location: ../../target/release/libblazer_node.*"
