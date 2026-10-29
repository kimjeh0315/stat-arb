#!/usr/bin/env bash
# Download one normalized Parquet object. Requires curl >= 7.75 and xmllint.
set +x
set -euo pipefail
umask 077
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ENV_FILE=${ENV_FILE:-"$ROOT/.env"}
SAMPLE_DATE=${1:-2026-10-25} # UTC partition date
EXCHANGE=${2:-bybit}
DATASET=${3:-book_state_20}
OUTPUT_DIR=${4:-"$ROOT/data/samples"}
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
for tool in curl xmllint; do command -v "$tool" >/dev/null || fail "Missing command: $tool"; done
[[ -f "$ENV_FILE" ]] || fail "Environment file not found"
# Parse only AWS/S3 variables; never execute .env or load the GitHub token.
while IFS= read -r line || [[ -n "$line" ]]; do
    line=${line%$'\r'}
    [[ "$line" =~ ^[[:space:]]*(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|AWS_REGION|QUANT_S3_BUCKET|QUANT_S3_PREFIX)[[:space:]]*=(.*)$ ]] || continue
    key=${BASH_REMATCH[1]}; value=${BASH_REMATCH[2]}
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "$value" == \"*\" || "$value" == \'*\' ]]; then value=${value:1:${#value}-2}; fi
    export "$key=$value"
done < "$ENV_FILE"
for key in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION QUANT_S3_BUCKET; do
    [[ -n "${!key:-}" ]] || fail "Missing variable: $key"
done
[[ "$SAMPLE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || fail "Date must be YYYY-MM-DD"
[[ "$EXCHANGE" =~ ^(bybit|aster|grvt|hyperliquid)$ ]] || fail "Unsupported exchange"
[[ "$DATASET" =~ ^[a-z0-9_]+$ ]] || fail "Invalid dataset"
[[ "$AWS_REGION" =~ ^[a-z0-9-]+$ ]] || fail "Invalid region"
[[ "$QUANT_S3_BUCKET" =~ ^[a-z0-9][a-z0-9.-]+$ ]] || fail "Invalid bucket name"
PREFIX=${QUANT_S3_PREFIX:-quant}; PREFIX=${PREFIX#/}; PREFIX=${PREFIX%/}
PREFIX="$PREFIX/normalized/dataset=$DATASET/exchange=$EXCHANGE/date=$SAMPLE_DATE/"
BASE="https://s3.$AWS_REGION.amazonaws.com/$QUANT_S3_BUCKET"
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT
# Supply credentials via stdin, rather than command arguments or output.
request() {
    local operation=$1 target=$2; shift 2
    local status
    status=$({
        printf 'user = "%s:%s"\n' "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
        if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
            printf 'header = "x-amz-security-token: %s"\n' "$AWS_SESSION_TOKEN"
        fi
    } | curl --config - --aws-sigv4 "aws:amz:$AWS_REGION:s3" \
        --silent --show-error --connect-timeout 15 --max-time 180 \
        --output "$target" --write-out '%{http_code}' "$@") || fail "S3 network request failed"
    if [[ "$status" != 200 ]]; then
        local code
        code=$(xmllint --xpath 'string(/*[local-name()="Error"]/*[local-name()="Code"])' "$target" 2>/dev/null || true)
        fail "S3 $operation: HTTP $status (${code:-unknown error}); check credentials, ListBucket/GetObject permissions, bucket and region"
    fi
}
printf 'Searching normalized data: date=%s exchange=%s dataset=%s\n' "$SAMPLE_DATE" "$EXCHANGE" "$DATASET"
request ListObjectsV2 "$TEMP_DIR/list.xml" --get "$BASE" \
    --data-urlencode 'list-type=2' --data-urlencode 'max-keys=1' --data-urlencode "prefix=$PREFIX"
OBJECT_KEY=$(xmllint --xpath 'string(/*[local-name()="ListBucketResult"]/*[local-name()="Contents"][1]/*[local-name()="Key"])' "$TEMP_DIR/list.xml")
[[ -n "$OBJECT_KEY" ]] || fail "No objects found for this UTC date/exchange/dataset"
[[ "$OBJECT_KEY" == "$PREFIX"* && "$OBJECT_KEY" == *.parquet ]] || fail "Unexpected object key"
[[ "$OBJECT_KEY" =~ ^[a-zA-Z0-9_./=-]+$ ]] || fail "Object key requires additional URL encoding"
# Older curl versions sign literal '=' incorrectly in S3 partition paths.
# The allowlist above leaves '=' as the only character requiring encoding.
OBJECT_URL_KEY=${OBJECT_KEY//=/\%3D}
request GetObject "$TEMP_DIR/sample.parquet" "$BASE/$OBJECT_URL_KEY"
[[ "$(head -c 4 "$TEMP_DIR/sample.parquet")" == PAR1 && "$(tail -c 4 "$TEMP_DIR/sample.parquet")" == PAR1 ]] || fail "Downloaded file is not a Parquet file"
mkdir -p "$OUTPUT_DIR"
OUTPUT="$OUTPUT_DIR/${EXCHANGE}_${DATASET}_${SAMPLE_DATE}_${OBJECT_KEY##*/}"
[[ ! -e "$OUTPUT" ]] || fail "Output already exists: $OUTPUT"
mv "$TEMP_DIR/sample.parquet" "$OUTPUT"
printf 'Saved: %s\nBytes: %s\n' "$OUTPUT" "$(wc -c < "$OUTPUT" | tr -d ' ')"
