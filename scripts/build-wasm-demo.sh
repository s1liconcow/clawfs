#!/usr/bin/env bash
# Build the ClawFS WASM demo and output artifacts to the website directory.
#
# Requirements:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-pack
#
# Usage:
#   ./scripts/build-wasm-demo.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WASM_CRATE="$REPO_ROOT/clawfs-wasm-demo"
OUT_DIR="$REPO_ROOT/website/clawfs.dev/wasm"

echo "Building ClawFS WASM demo..."
echo "  crate: $WASM_CRATE"
echo "  output: $OUT_DIR"

cd "$WASM_CRATE"
wasm-pack build --target web --out-dir "$OUT_DIR" --no-pack

echo ""
echo "Done. Output files:"
ls -lh "$OUT_DIR"/*.wasm "$OUT_DIR"/*.js 2>/dev/null
