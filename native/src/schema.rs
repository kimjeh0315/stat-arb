use crate::model::{Cell, Event};
use anyhow::{Result, bail};
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;
#[derive(Clone, Copy)]
pub enum Kind {
    String,
    Decimal,
    Int,
    Int32,
    Float,
    Bool,
    Strings,
    Levels,
}
use Kind::*;
pub fn fields(dataset: &str) -> Result<Vec<(&'static str, Kind)>> {
    let mut f = vec![
        ("event_id", String),
        ("exchange", String),
        ("instrument_id", String),
        ("base_asset", String),
        ("category", String),
        ("quote_asset", String),
        ("session_id", String),
        ("raw_id", String),
        ("event_time_ns", Int),
        ("received_at_ns", Int),
        ("received_mono_ns", Int),
        ("schema_version", Int32),
        ("quality_flags", Strings),
    ];
    f.extend(match dataset {
        "bbo" => vec![
            ("bid_price", Decimal), ("bid_qty", Decimal),
            ("ask_price", Decimal), ("ask_qty", Decimal),
            ("source_sequence", String), ("quote_valid", Bool),
        ],
        "book_state_20" => vec![
            ("book_state_id", String),
            ("book_epoch_id", String),
            ("source_sequence", String),
            ("bids", Levels),
            ("asks", Levels),
            ("bid_level_count", Int32),
            ("ask_level_count", Int32),
            ("book_valid", Bool),
        ],
        "public_trade" => vec![
            ("native_trade_id", String),
            ("price", Decimal),
            ("base_qty", Decimal),
            ("aggressor_side", String),
            ("is_aggregated", Bool),
            ("is_snapshot", Bool),
        ],
        "price_event" => vec![("price_type", String), ("price", Decimal)],
        "funding_event" => vec![
            ("rate_kind", String),
            ("rate_decimal", Decimal),
            ("native_rate", Decimal),
            ("native_rate_unit", String),
            ("interval_hours", Decimal),
            ("settlement_at_ns", Int),
        ],
        "oi_event" => vec![
            ("native_value", Decimal),
            ("native_unit", String),
            ("oi_base_qty", Decimal),
        ],
        "liquidation_event" => vec![
            ("order_side", String),
            ("liquidated_position_side", String),
            ("order_qty", Decimal),
            ("filled_qty", Decimal),
            ("reported_price", Decimal),
            ("price_kind", String),
            ("coverage_type", String),
        ],
        "instrument_metadata" => vec![
            ("native_symbol", String),
            ("native_base", String),
            ("settle_asset", String),
            ("multiplier", Decimal),
            ("tick_size", Decimal),
            ("qty_step", Decimal),
            ("min_qty", Decimal),
            ("funding_interval_hours", Decimal),
        ],
        "collection_status" => vec![("state", String), ("detail", String)],
        "analysis_state_1s" => vec![
            ("as_of_ns", Int),
            ("book_state_id", String),
            ("mid_price", Decimal),
            ("spread_bps", Decimal),
            ("mark_price", Decimal),
            ("index_price", Decimal),
            ("oracle_price", Decimal),
            ("last_price", Decimal),
            ("funding_rate", Decimal),
            ("funding_interval_hours", Decimal),
            ("next_funding_at_ns", Int),
            ("oi_base_qty", Decimal),
            ("trade_volume_1s", Decimal),
            ("trade_notional_1s", Decimal),
            ("taker_buy_volume_1s", Decimal),
            ("taker_sell_volume_1s", Decimal),
            ("reported_liquidation_volume_1s", Decimal),
            ("book_age_ms", Float),
            ("oi_age_ms", Float),
            ("mark_age_ms", Float),
            ("index_age_ms", Float),
            ("oracle_age_ms", Float),
            ("last_age_ms", Float),
            ("funding_age_ms", Float),
            ("liquidation_coverage", String),
            ("trade_feed_connected", Bool),
            ("liquidation_feed_connected", Bool),
        ],
        _ => bail!("unknown dataset {dataset}"),
    });
    Ok(f)
}
fn decimal_builder() -> Decimal128Builder {
    Decimal128Builder::new().with_data_type(DataType::Decimal128(38, 18))
}
fn dtype(k: Kind) -> DataType {
    match k {
        String => DataType::Utf8,
        Decimal => DataType::Decimal128(38, 18),
        Int => DataType::Int64,
        Int32 => DataType::Int32,
        Float => DataType::Float64,
        Bool => DataType::Boolean,
        Strings => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Levels => DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("price", DataType::Decimal128(38, 18), true),
                    Field::new("qty", DataType::Decimal128(38, 18), true),
                ]
                .into(),
            ),
            true,
        ))),
    }
}
pub fn batch(dataset: &str, rows: &[Event]) -> Result<RecordBatch> {
    let fields = fields(dataset)?;
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, k)| Field::new(*name, dtype(*k), true))
            .collect::<Vec<_>>(),
    ));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (name, kind) in fields {
        let a: ArrayRef = match kind {
            String => Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| match r.fields.get(name) {
                        Some(Cell::String(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            Decimal => Arc::new(
                Decimal128Array::from(
                    rows.iter()
                        .map(|r| r.decimal(name).map(|d| d.0))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(38, 18)?,
            ),
            Int => Arc::new(Int64Array::from(
                rows.iter().map(|r| r.integer(name)).collect::<Vec<_>>(),
            )),
            Int32 => Arc::new(Int32Array::from(
                rows.iter()
                    .map(|r| r.integer(name).map(|n| n as i32))
                    .collect::<Vec<_>>(),
            )),
            Float => Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| match r.fields.get(name) {
                        Some(Cell::Float(n)) => Some(*n),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            Bool => Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|r| match r.fields.get(name) {
                        Some(Cell::Bool(n)) => Some(*n),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            Strings => {
                let mut b = ListBuilder::new(StringBuilder::new())
                    .with_field(Arc::new(Field::new("item", DataType::Utf8, true)));
                for r in rows {
                    if let Some(Cell::Strings(v)) = r.fields.get(name) {
                        for s in v {
                            b.values().append_value(s);
                        }
                        b.append(true);
                    } else {
                        b.append(false);
                    }
                }
                Arc::new(b.finish())
            }
            Levels => {
                let sf = vec![
                    Field::new("price", DataType::Decimal128(38, 18), true),
                    Field::new("qty", DataType::Decimal128(38, 18), true),
                ];
                let item_type = DataType::Struct(sf.clone().into());
                let sb = StructBuilder::new(
                    sf,
                    vec![Box::new(decimal_builder()), Box::new(decimal_builder())],
                );
                let mut b =
                    ListBuilder::new(sb).with_field(Arc::new(Field::new("item", item_type, true)));
                for r in rows {
                    if let Some(Cell::Levels(v)) = r.fields.get(name) {
                        for l in v {
                            b.values()
                                .field_builder::<Decimal128Builder>(0)
                                .unwrap()
                                .append_value(l.price.0);
                            b.values()
                                .field_builder::<Decimal128Builder>(1)
                                .unwrap()
                                .append_value(l.qty.0);
                            b.values().append(true);
                        }
                        b.append(true);
                    } else {
                        b.append(false);
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(a);
    }
    Ok(RecordBatch::try_new(schema, arrays)?)
}
