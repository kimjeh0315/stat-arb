from decimal import Decimal

import pyarrow as pa

DECIMAL = pa.decimal128(38, 18)
LEVELS = pa.list_(pa.struct([("price", DECIMAL), ("qty", DECIMAL)]))
COMMON = [
    ("event_id", pa.string()), ("exchange", pa.string()), ("instrument_id", pa.string()),
    ("base_asset", pa.string()), ("category", pa.string()), ("quote_asset", pa.string()),
    ("session_id", pa.string()), ("raw_id", pa.string()),
    ("event_time_ns", pa.int64()), ("received_at_ns", pa.int64()),
    ("received_mono_ns", pa.int64()), ("schema_version", pa.int32()),
    ("quality_flags", pa.list_(pa.string())),
]
FIELDS = {
    "book_state_20": [("book_state_id", pa.string()), ("book_epoch_id", pa.string()),
                      ("source_sequence", pa.string()), ("bids", LEVELS), ("asks", LEVELS),
                      ("bid_level_count", pa.int32()), ("ask_level_count", pa.int32()),
                      ("book_valid", pa.bool_())],
    "public_trade": [("native_trade_id", pa.string()), ("price", DECIMAL), ("base_qty", DECIMAL),
                     ("aggressor_side", pa.string()), ("is_aggregated", pa.bool_()),
                     ("is_snapshot", pa.bool_())],
    "price_event": [("price_type", pa.string()), ("price", DECIMAL)],
    "funding_event": [("rate_kind", pa.string()), ("rate_decimal", DECIMAL),
                      ("native_rate", DECIMAL), ("native_rate_unit", pa.string()),
                      ("interval_hours", DECIMAL), ("settlement_at_ns", pa.int64())],
    "oi_event": [("native_value", DECIMAL), ("native_unit", pa.string()), ("oi_base_qty", DECIMAL)],
    "liquidation_event": [("order_side", pa.string()), ("liquidated_position_side", pa.string()),
                          ("order_qty", DECIMAL), ("filled_qty", DECIMAL),
                          ("reported_price", DECIMAL), ("price_kind", pa.string()),
                          ("coverage_type", pa.string())],
    "instrument_metadata": [("native_symbol", pa.string()), ("native_base", pa.string()),
                            ("settle_asset", pa.string()), ("multiplier", DECIMAL),
                            ("tick_size", DECIMAL), ("qty_step", DECIMAL), ("min_qty", DECIMAL),
                            ("funding_interval_hours", DECIMAL)],
    "collection_status": [("state", pa.string()), ("detail", pa.string())],
    "analysis_state_1s": [
        ("as_of_ns", pa.int64()), ("book_state_id", pa.string()),
        ("mid_price", DECIMAL), ("spread_bps", DECIMAL),
        ("mark_price", DECIMAL), ("index_price", DECIMAL), ("oracle_price", DECIMAL),
        ("last_price", DECIMAL), ("funding_rate", DECIMAL), ("funding_interval_hours", DECIMAL),
        ("next_funding_at_ns", pa.int64()), ("oi_base_qty", DECIMAL),
        ("trade_volume_1s", DECIMAL), ("trade_notional_1s", DECIMAL),
        ("taker_buy_volume_1s", DECIMAL), ("taker_sell_volume_1s", DECIMAL),
        ("reported_liquidation_volume_1s", DECIMAL),
        ("book_age_ms", pa.float64()), ("oi_age_ms", pa.float64()),
        ("mark_age_ms", pa.float64()), ("index_age_ms", pa.float64()),
        ("oracle_age_ms", pa.float64()), ("last_age_ms", pa.float64()),
        ("funding_age_ms", pa.float64()), ("liquidation_coverage", pa.string()),
        ("trade_feed_connected", pa.bool_()), ("liquidation_feed_connected", pa.bool_()),
    ],
}
SCHEMAS = {name: pa.schema(COMMON + fields) for name, fields in FIELDS.items()}


def coerce(value, dtype):
    if value is None:
        return None
    if pa.types.is_decimal(dtype):
        return Decimal(str(value))
    if pa.types.is_list(dtype):
        return [coerce(item, dtype.value_type) for item in value]
    if pa.types.is_struct(dtype):
        return {field.name: coerce(value.get(field.name), field.type) for field in dtype}
    return value


def table(dataset, rows):
    schema = SCHEMAS[dataset]
    return pa.Table.from_pylist(
        [{f.name: coerce(row.get(f.name), f.type) for f in schema} for row in rows], schema=schema)
