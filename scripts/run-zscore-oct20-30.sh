#!/usr/bin/env bash
# UTC [2026-10-20 00:00, 2026-10-30 00:00); full input, no max-files cap.
# Build once: cargo build --release --locked --bin zscore
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
exec ./target/release/zscore --config config/zscore-oct20-30.json "$@"
