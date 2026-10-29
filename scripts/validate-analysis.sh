#!/usr/bin/env bash
# Run from any working directory. Additional arguments are passed to the Rust CLI.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
BINARY=${QUANT_VALIDATOR_BIN:-"$ROOT/target/release/validate-analysis"}
NEEDS_BUILD=false
[[ -x "$BINARY" ]] || NEEDS_BUILD=true
if [[ -z "${QUANT_VALIDATOR_BIN:-}" ]]; then
    for SOURCE in "$ROOT/native/src/bin/validate-analysis.rs" "$ROOT"/native/src/validation/*.rs "$ROOT/Cargo.lock"; do
        [[ "$SOURCE" -nt "$BINARY" ]] && NEEDS_BUILD=true
    done
fi
if [[ "$NEEDS_BUILD" == true ]]; then
    command -v cargo >/dev/null || { printf 'Rust 1.99.0 is needed to build validate-analysis.\n' >&2; exit 1; }
    cargo build --release --locked --bin validate-analysis
fi
exec "$BINARY" "$@"
