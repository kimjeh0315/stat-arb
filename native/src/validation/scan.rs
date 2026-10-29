use super::{Args, DAY_SECONDS, Expected, SECOND, increment, input::Source};
use anyhow::{Context, Result, ensure};
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use std::{
    collections::{BTreeMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

pub const PRICE: u8 = 1;
pub const TRADE: u8 = 2;
pub const OI: u8 = 4;
pub const FUNDING: u8 = 8;
pub const MISSING: u32 = 1;
pub const DUPLICATE: u32 = 2;
pub const CONFLICT: u32 = 4;
pub const INCOMPLETE: u32 = 8;
pub const BAD_BOOK: u32 = 16;
pub const BAD_NUMBER: u32 = 32;
pub const BAD_TRADE: u32 = 64;
pub const BAD_AGE: u32 = 128;
pub const DISCONNECTED: u32 = 256;
pub const UNEXPECTED: u32 = 512;
pub const INCIDENTS: &[(u32, &str)] = &[
    (MISSING, "missing_second"),
    (DUPLICATE, "duplicate_second"),
    (CONFLICT, "conflicting_duplicate"),
    (INCOMPLETE, "incomplete_feed_window"),
    (BAD_BOOK, "book_unusable"),
    (BAD_NUMBER, "invalid_numeric_value"),
    (BAD_TRADE, "trade_inconsistent"),
    (BAD_AGE, "invalid_age"),
    (DISCONNECTED, "feed_disconnected"),
    (UNEXPECTED, "outside_expected_window"),
];
const DECIMALS: &[&str] = &[
    "mid_price",
    "spread_bps",
    "mark_price",
    "index_price",
    "oracle_price",
    "last_price",
    "funding_rate",
    "funding_interval_hours",
    "oi_base_qty",
    "trade_volume_1s",
    "trade_notional_1s",
    "taker_buy_volume_1s",
    "taker_sell_volume_1s",
    "reported_liquidation_volume_1s",
];
const AGES: &[&str] = &[
    "book_age_ms",
    "oi_age_ms",
    "mark_age_ms",
    "index_age_ms",
    "oracle_age_ms",
    "last_age_ms",
    "funding_age_ms",
];
const AGE_BOUNDS: &[f64] = &[
    0.,
    50.,
    100.,
    250.,
    500.,
    1000.,
    3000.,
    5000.,
    10000.,
    15000.,
    30000.,
    60000.,
    180000.,
    300000.,
    900000.,
    3600000.,
    86400000.,
    f64::MAX,
];

#[derive(Clone, Copy)]
pub struct Slot {
    pub hash: u64,
    pub issues: u32,
    pub masks: u8,
    pub present: bool,
}
impl Default for Slot {
    fn default() -> Self {
        Self {
            hash: 0,
            issues: MISSING,
            masks: 0,
            present: false,
        }
    }
}

#[derive(Default, Serialize)]
pub struct FieldStats {
    pub null_rows: u64,
    pub present_rows: u64,
    pub invalid_rows: u64,
    pub stale_rows: u64,
    pub min_age_ms: Option<f64>,
    pub max_age_ms: Option<f64>,
    #[serde(skip)]
    pub histogram: [u64; 18],
    pub p50_upper_bound_ms: Option<f64>,
    pub p95_upper_bound_ms: Option<f64>,
    pub p99_upper_bound_ms: Option<f64>,
}
impl FieldStats {
    fn observe_age(&mut self, value: Option<f64>, threshold: f64) -> bool {
        let Some(value) = value else {
            self.null_rows += 1;
            return false;
        };
        self.present_rows += 1;
        if !value.is_finite() || value < 0. {
            self.invalid_rows += 1;
            return false;
        }
        self.min_age_ms = Some(self.min_age_ms.map_or(value, |v| v.min(value)));
        self.max_age_ms = Some(self.max_age_ms.map_or(value, |v| v.max(value)));
        let bin = AGE_BOUNDS.iter().position(|b| value <= *b).unwrap_or(17);
        self.histogram[bin] += 1;
        if value > threshold {
            self.stale_rows += 1;
        }
        value <= threshold
    }
    pub fn finish(&mut self) {
        let n: u64 = self.histogram.iter().sum();
        let percentile = |p: f64| {
            if n == 0 {
                return None;
            }
            let target = (n as f64 * p).ceil() as u64;
            let mut sum = 0;
            for (bound, count) in AGE_BOUNDS.iter().zip(self.histogram) {
                sum += count;
                if sum >= target {
                    return Some(if *bound == f64::MAX {
                        self.max_age_ms.unwrap()
                    } else {
                        *bound
                    });
                }
            }
            None
        };
        self.p50_upper_bound_ms = percentile(0.5);
        self.p95_upper_bound_ms = percentile(0.95);
        self.p99_upper_bound_ms = percentile(0.99);
    }
}

pub struct Instrument {
    pub exchange: String,
    pub instrument_id: String,
    pub begin: usize,
    pub end: usize,
    pub declared: bool,
    pub slots: Vec<Slot>,
    pub rows: u64,
    pub fields: BTreeMap<String, FieldStats>,
    pub flags: BTreeMap<String, u64>,
    pub coverage: BTreeMap<String, u64>,
    pub errors: BTreeMap<String, u64>,
    pub book_age_over_ms: BTreeMap<String, u64>,
}
impl Instrument {
    fn new(exchange: &str, id: &str, begin: usize, end: usize, declared: bool) -> Self {
        Self {
            exchange: exchange.into(),
            instrument_id: id.into(),
            begin,
            end,
            declared,
            slots: vec![Slot::default(); DAY_SECONDS],
            rows: 0,
            fields: BTreeMap::new(),
            flags: BTreeMap::new(),
            coverage: BTreeMap::new(),
            errors: BTreeMap::new(),
            book_age_over_ms: BTreeMap::new(),
        }
    }
    pub fn record(&mut self, second: usize, hash: u64, masks: u8, mut issues: u32) {
        if second < self.begin || second >= self.end {
            issues |= UNEXPECTED;
            increment(&mut self.errors, "outside_expected_window_rows");
        }
        let old = &mut self.slots[second];
        if old.present {
            old.issues |= issues | DUPLICATE;
            if old.hash != hash {
                old.issues |= CONFLICT;
                old.masks = 0;
            }
            old.masks &= masks;
            increment(&mut self.errors, "duplicate_rows");
        } else {
            *old = Slot {
                hash,
                issues,
                masks,
                present: true,
            };
        }
    }
}

pub struct Audit {
    pub start: i64,
    pub rows: u64,
    pub files_read: usize,
    pub failures: Vec<serde_json::Value>,
    pub row_errors: BTreeMap<String, u64>,
    pub missing_exchanges: Vec<String>,
    pub instruments: BTreeMap<(String, String), Instrument>,
    thresholds: [f64; 4],
    exchange_filter: Vec<String>,
    pub expected_supplied: bool,
    pub filtered_rows: u64,
}

impl Audit {
    pub fn new(start: i64, args: &Args, expected: Option<&[Expected]>) -> Self {
        let mut instruments = BTreeMap::new();
        if let Some(items) = expected {
            for e in items {
                let boundary = |t: i64| {
                    ((t as i128 - start as i128 + SECOND as i128 - 1).div_euclid(SECOND as i128))
                        .clamp(0, DAY_SECONDS as i128) as usize
                };
                let begin = boundary(e.start_ns.unwrap_or(start));
                let end = boundary(e.end_ns.unwrap_or(start + DAY_SECONDS as i64 * SECOND));
                instruments.insert(
                    (e.exchange.clone(), e.instrument_id.clone()),
                    Instrument::new(&e.exchange, &e.instrument_id, begin, end, true),
                );
            }
        }
        Self {
            start,
            rows: 0,
            files_read: 0,
            failures: Vec::new(),
            row_errors: BTreeMap::new(),
            missing_exchanges: Vec::new(),
            instruments,
            thresholds: [
                args.book_max_age_ms,
                args.price_max_age_ms,
                args.oi_max_age_ms,
                args.funding_max_age_ms,
            ],
            exchange_filter: args.exchanges.clone(),
            expected_supplied: expected.is_some(),
            filtered_rows: 0,
        }
    }
    pub fn file(&mut self, source: &Source) -> Result<()> {
        // A malformed file is recorded and the remaining files are still inspected.
        // Mark the report incomplete, never silently treat this as clean data.
        if let Err(error) = self.read_file(source) {
            self.failures
                .push(serde_json::json!({"source":source.key,"error":format!("{error:#}")}));
        } else {
            self.files_read += 1;
        }
        Ok(())
    }
    fn read_file(&mut self, source: &Source) -> Result<()> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&source.local_path)?)?;
        validate_schema(builder.schema())?;
        for batch in builder.with_batch_size(8192).build()? {
            let batch = batch?;
            self.batch(&batch, source.exchange.as_deref())?;
        }
        Ok(())
    }
    pub fn batch(&mut self, batch: &RecordBatch, source_exchange: Option<&str>) -> Result<()> {
        let mut decimal = BTreeMap::new();
        let mut ages = BTreeMap::new();
        for name in DECIMALS {
            decimal.insert(*name, array::<Decimal128Array>(batch, name)?);
        }
        for name in AGES {
            ages.insert(*name, array::<Float64Array>(batch, name)?);
        }
        let exchange = array::<StringArray>(batch, "exchange")?;
        let ids = array::<StringArray>(batch, "instrument_id")?;
        let timestamps = array::<Int64Array>(batch, "as_of_ns")?;
        let event_time = array::<Int64Array>(batch, "event_time_ns")?;
        let received_time = array::<Int64Array>(batch, "received_at_ns")?;
        let next_funding = array::<Int64Array>(batch, "next_funding_at_ns")?;
        let schema_version = array::<Int32Array>(batch, "schema_version")?;
        let connected = array::<BooleanArray>(batch, "trade_feed_connected")?;
        let liquidation_connected = array::<BooleanArray>(batch, "liquidation_feed_connected")?;
        let coverage = array::<StringArray>(batch, "liquidation_coverage")?;
        let flags = array::<ListArray>(batch, "quality_flags")?;
        let book_ids = array::<StringArray>(batch, "book_state_id")?;
        for row in 0..batch.num_rows() {
            self.rows += 1;
            if exchange.is_null(row) || ids.is_null(row) || timestamps.is_null(row) {
                increment(&mut self.row_errors, "missing_primary_key");
                continue;
            }
            let ex = exchange.value(row);
            if !self.exchange_filter.is_empty() && !self.exchange_filter.iter().any(|s| s == ex) {
                self.filtered_rows += 1;
                continue;
            }
            if source_exchange.is_some_and(|s| s != ex) {
                increment(&mut self.row_errors, "partition_exchange_mismatch");
            }
            let t = timestamps.value(row);
            let delta = t as i128 - self.start as i128;
            if !(0..DAY_SECONDS as i128 * SECOND as i128).contains(&delta) {
                increment(&mut self.row_errors, "outside_utc_day");
                continue;
            }
            if delta % SECOND as i128 != 0 {
                increment(&mut self.row_errors, "not_second_aligned");
                continue;
            }
            let second = (delta / SECOND as i128) as usize;
            let key = (ex.to_owned(), ids.value(row).to_owned());
            let i = self.instruments.entry(key).or_insert_with(|| {
                Instrument::new(
                    ex,
                    ids.value(row),
                    0,
                    if self.expected_supplied {
                        0
                    } else {
                        DAY_SECONDS
                    },
                    !self.expected_supplied,
                )
            });
            i.rows += 1;
            let mut issues = 0;
            if !i.declared {
                issues |= UNEXPECTED;
                increment(&mut i.errors, "unexpected_instrument_rows");
            }
            if schema_version.is_null(row) || schema_version.value(row) != 2 {
                increment(&mut i.errors, "unsupported_schema_version");
                issues |= BAD_NUMBER;
            }
            if event_time.is_null(row)
                || received_time.is_null(row)
                || event_time.value(row) != t
                || received_time.value(row) != t
            {
                increment(&mut i.errors, "analysis_timestamp_mismatch");
                issues |= BAD_NUMBER;
            }
            let mut flag_names = Vec::new();
            if flags.is_null(row) {
                increment(&mut i.errors, "null_quality_flags");
                issues |= INCOMPLETE;
            } else {
                let values = flags.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("quality flags must be strings")?;
                for index in 0..values.len() {
                    if values.is_null(index) {
                        issues |= INCOMPLETE;
                        continue;
                    }
                    let flag = values.value(index);
                    increment(&mut i.flags, flag);
                    flag_names.push(flag.to_owned());
                }
            }
            flag_names.sort();
            let feed = !connected.is_null(row) && connected.value(row);
            let incomplete = flag_names.iter().any(|s| s == "incomplete_feed_window")
                || issues & INCOMPLETE != 0;
            if incomplete {
                issues |= INCOMPLETE;
            }
            if !feed {
                issues |= DISCONNECTED;
            }
            let d = |name: &str| {
                let a = decimal[name];
                (!a.is_null(row)).then(|| a.value(row))
            };
            let age = |name: &str| {
                let a = ages[name];
                (!a.is_null(row)).then(|| a.value(row))
            };
            let mut fresh = BTreeMap::new();
            for name in AGES {
                let threshold = match *name {
                    "book_age_ms" => self.thresholds[0],
                    "oi_age_ms" => self.thresholds[2],
                    "funding_age_ms" => self.thresholds[3],
                    _ => self.thresholds[1],
                };
                let value = age(name);
                let ok = i
                    .fields
                    .entry((*name).into())
                    .or_default()
                    .observe_age(value, threshold);
                if value.is_some_and(|v| !v.is_finite() || v < 0.) {
                    issues |= BAD_AGE;
                }
                fresh.insert(*name, ok);
            }
            if let Some(age) = age("book_age_ms").filter(|v| v.is_finite() && *v >= 0.) {
                for threshold in [1000, 3000, 5000, 15000] {
                    if age > threshold as f64 {
                        increment(&mut i.book_age_over_ms, &threshold.to_string());
                    }
                }
            }
            for name in DECIMALS {
                let stats = i.fields.entry((*name).into()).or_default();
                if let Some(v) = d(name) {
                    stats.present_rows += 1;
                    let valid = match *name {
                        "funding_rate" => true,
                        "mid_price"
                        | "mark_price"
                        | "index_price"
                        | "oracle_price"
                        | "last_price"
                        | "funding_interval_hours" => v > 0,
                        _ => v >= 0,
                    };
                    if !valid {
                        stats.invalid_rows += 1;
                        issues |= BAD_NUMBER;
                    }
                } else {
                    stats.null_rows += 1;
                }
            }
            let mut masks = 0;
            if feed
                && fresh["book_age_ms"]
                && d("mid_price").is_some_and(|n| n > 0)
                && d("spread_bps").is_some_and(|n| n >= 0)
                && !book_ids.is_null(row)
                && !book_ids.value(row).is_empty()
                && !flag_names.iter().any(|s| s == "book_unavailable_or_stale")
            {
                masks |= PRICE;
            } else {
                issues |= BAD_BOOK;
            }
            let volumes = [
                d("trade_volume_1s"),
                d("trade_notional_1s"),
                d("taker_buy_volume_1s"),
                d("taker_sell_volume_1s"),
            ];
            let trade_consistent =
                if let [Some(total), Some(notional), Some(buy), Some(sell)] = volumes {
                    total >= 0
                        && notional >= 0
                        && buy >= 0
                        && sell >= 0
                        && buy.checked_add(sell) == Some(total)
                        && ((total == 0 && notional == 0) || (total > 0 && notional > 0))
                } else {
                    false
                };
            if feed && !incomplete && trade_consistent {
                masks |= TRADE;
            }
            if volumes.iter().all(Option::is_some) && !trade_consistent {
                issues |= BAD_TRADE;
            }
            if (!feed || incomplete) && volumes.iter().any(Option::is_some) {
                issues |= BAD_TRADE;
                increment(&mut i.errors, "volume_present_in_incomplete_window");
            }
            if d("oi_base_qty").is_some_and(|n| n >= 0) && fresh["oi_age_ms"] {
                masks |= OI;
            }
            if d("funding_rate").is_some()
                && d("funding_interval_hours").is_some_and(|n| n > 0)
                && fresh["funding_age_ms"]
            {
                masks |= FUNDING;
            }
            if !coverage.is_null(row) {
                let c = coverage.value(row);
                increment(&mut i.coverage, c);
                if c == "unavailable_public_api" && d("reported_liquidation_volume_1s").is_some() {
                    increment(&mut i.errors, "liquidation_value_on_unavailable_feed");
                    issues |= BAD_NUMBER;
                }
                if c == "unavailable_public_api"
                    && !liquidation_connected.is_null(row)
                    && liquidation_connected.value(row)
                {
                    increment(&mut i.errors, "liquidation_connected_on_unavailable_feed");
                    issues |= BAD_NUMBER;
                }
                if c != "unavailable_public_api"
                    && feed
                    && !incomplete
                    && !liquidation_connected.is_null(row)
                    && liquidation_connected.value(row)
                    && d("reported_liquidation_volume_1s").is_none()
                {
                    increment(&mut i.errors, "missing_liquidation_total_on_complete_feed");
                    issues |= BAD_NUMBER;
                }
                if !liquidation_connected.is_null(row)
                    && !liquidation_connected.value(row)
                    && d("reported_liquidation_volume_1s").is_some()
                {
                    increment(&mut i.errors, "liquidation_value_while_disconnected");
                    issues |= BAD_NUMBER;
                }
            }
            // Compare semantic analysis values; runtime timestamps/event IDs are not payload conflicts.
            let mut hasher = DefaultHasher::new();
            for name in DECIMALS {
                d(name).hash(&mut hasher);
            }
            for name in AGES {
                age(name).map(f64::to_bits).hash(&mut hasher);
            }
            flag_names.hash(&mut hasher);
            feed.hash(&mut hasher);
            (!coverage.is_null(row))
                .then(|| coverage.value(row))
                .hash(&mut hasher);
            (!book_ids.is_null(row))
                .then(|| book_ids.value(row))
                .hash(&mut hasher);
            (!next_funding.is_null(row))
                .then(|| next_funding.value(row))
                .hash(&mut hasher);
            (!liquidation_connected.is_null(row))
                .then(|| liquidation_connected.value(row))
                .hash(&mut hasher);
            if schema_version.is_null(row) || schema_version.value(row) != 2 {
                masks = 0;
            }
            i.record(second, hasher.finish(), masks, issues);
        }
        Ok(())
    }
}

fn array<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("unexpected column type: {name}"))
}
fn validate_schema(schema: &arrow::datatypes::SchemaRef) -> Result<()> {
    let required = [
        ("exchange", DataType::Utf8),
        ("instrument_id", DataType::Utf8),
        ("as_of_ns", DataType::Int64),
        ("event_time_ns", DataType::Int64),
        ("received_at_ns", DataType::Int64),
        ("next_funding_at_ns", DataType::Int64),
        ("schema_version", DataType::Int32),
        ("trade_feed_connected", DataType::Boolean),
        ("liquidation_feed_connected", DataType::Boolean),
        ("liquidation_coverage", DataType::Utf8),
        ("book_state_id", DataType::Utf8),
    ];
    for (name, expected) in required {
        ensure!(
            schema.field_with_name(name)?.data_type() == &expected,
            "unexpected schema for {name}"
        );
    }
    for name in DECIMALS {
        ensure!(
            schema.field_with_name(name)?.data_type() == &DataType::Decimal128(38, 18),
            "expected Decimal128(38,18): {name}"
        );
    }
    for name in AGES {
        ensure!(
            schema.field_with_name(name)?.data_type() == &DataType::Float64,
            "expected Float64: {name}"
        );
    }
    ensure!(
        matches!(schema.field_with_name("quality_flags")?.data_type(),DataType::List(f) if f.data_type() == &DataType::Utf8),
        "invalid quality_flags schema"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use clap::Parser;
    use std::sync::Arc;

    fn fixture(start: i64) -> RecordBatch {
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut add = |name: &str, array: ArrayRef| {
            fields.push(Field::new(name, array.data_type().clone(), true));
            arrays.push(array);
        };
        // Out of file order, one duplicate, one missing second, and one conflicting duplicate.
        let seconds = [3, 0, 1, 1, 3];
        for (name, value) in [
            ("exchange", "bybit"),
            ("instrument_id", "bybit:BTCUSDT"),
            ("book_state_id", "book-1"),
            ("liquidation_coverage", "exchange_all_liquidation"),
        ] {
            add(name, Arc::new(StringArray::from(vec![value; 5])));
        }
        for name in ["as_of_ns", "event_time_ns", "received_at_ns"] {
            add(
                name,
                Arc::new(Int64Array::from(
                    seconds
                        .iter()
                        .map(|s| start + s * SECOND)
                        .collect::<Vec<_>>(),
                )),
            );
        }
        add(
            "next_funding_at_ns",
            Arc::new(Int64Array::from(vec![Some(start + 8 * 3600 * SECOND); 5])),
        );
        add("schema_version", Arc::new(Int32Array::from(vec![2; 5])));
        add(
            "trade_feed_connected",
            Arc::new(BooleanArray::from(vec![true, true, true, true, false])),
        );
        add(
            "liquidation_feed_connected",
            Arc::new(BooleanArray::from(vec![true, true, true, true, false])),
        );
        let mut flags = ListBuilder::new(StringBuilder::new());
        for n in 0..5 {
            if n == 4 {
                flags.values().append_value("incomplete_feed_window");
            }
            flags.append(true);
        }
        add("quality_flags", Arc::new(flags.finish()));
        let scale = 1_000_000_000_000_000_000_i128;
        for name in DECIMALS {
            let value = match *name {
                "trade_volume_1s" => 3,
                "trade_notional_1s" => 30,
                "taker_buy_volume_1s" => 1,
                "taker_sell_volume_1s" => 2,
                "funding_rate" => -1,
                "funding_interval_hours" => 8,
                "spread_bps" => 1,
                "reported_liquidation_volume_1s" => 0,
                _ => 10,
            };
            let mut values = vec![Some(value * scale); 5];
            if *name == "mid_price" {
                values[4] = Some(11 * scale);
            }
            if [
                "trade_volume_1s",
                "trade_notional_1s",
                "taker_buy_volume_1s",
                "taker_sell_volume_1s",
                "reported_liquidation_volume_1s",
            ]
            .contains(name)
            {
                values[4] = None;
            }
            add(
                name,
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(38, 18)
                        .unwrap(),
                ),
            );
        }
        for name in AGES {
            let mut values = vec![Some(100.); 5];
            if *name == "oi_age_ms" {
                values[0] = Some(200_000.);
            }
            add(name, Arc::new(Float64Array::from(values)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn parquet_end_to_end_detects_gaps_conflicts_and_stale_values() {
        let dir =
            std::env::temp_dir().join(format!("quant-validation-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let args =
            Args::try_parse_from(["validate-analysis", "--date", "2026-10-25", "--write-masks"])
                .unwrap();
        let start = args
            .date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let batch = fixture(start);
        let path = dir.join("fixture.parquet");
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let source = Source {
            key: "fixture.parquet".into(),
            bytes: path.metadata().unwrap().len(),
            etag: None,
            exchange: Some("bybit".into()),
            local_path: path,
        };
        // Include a completely absent expected contract and a bounded active window.
        let expected = [
            Expected {
                exchange: "bybit".into(),
                instrument_id: "bybit:BTCUSDT".into(),
                start_ns: None,
                end_ns: Some(start + 4 * SECOND),
            },
            Expected {
                exchange: "bybit".into(),
                instrument_id: "bybit:ETHUSDT".into(),
                start_ns: None,
                end_ns: Some(start + 4 * SECOND),
            },
        ];
        let mut audit = Audit::new(start, &args, Some(&expected));
        audit.file(&source).unwrap();
        assert!(audit.failures.is_empty(), "{:?}", audit.failures);
        let i = &audit.instruments[&("bybit".into(), "bybit:BTCUSDT".into())];
        assert_eq!(i.slots[0].masks, PRICE | TRADE | OI | FUNDING);
        assert_eq!(i.slots[1].issues, DUPLICATE);
        assert_eq!(i.slots[2].issues, MISSING);
        assert_ne!(i.slots[3].issues & CONFLICT, 0);
        assert_eq!(i.slots[3].masks, 0);
        assert_eq!(i.fields["oi_age_ms"].stale_rows, 1);
        assert_eq!(i.fields["funding_rate"].invalid_rows, 0);
        super::super::report::write(&args, &dir, &[source], &mut audit, true).unwrap();
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("summary.json")).unwrap()).unwrap();
        assert_eq!(report["expected_instrument_seconds"], 8);
        assert_eq!(report["present_instrument_seconds"], 3);
        assert_eq!(report["missing_instrument_seconds"], 5);
        assert_eq!(report["conflicting_duplicate_instrument_seconds"], 1);
        let mut mask_rows = 0;
        for batch in ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(dir.join("usability.parquet")).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        {
            mask_rows += batch.unwrap().num_rows();
        }
        assert_eq!(mask_rows, 8);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn malformed_files_are_reported_as_incomplete() {
        let dir = std::env::temp_dir().join(format!("quant-invalid-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.parquet");
        std::fs::write(&path, b"not parquet").unwrap();
        let args = Args::try_parse_from(["validate-analysis", "--date", "2026-10-25"]).unwrap();
        let mut audit = Audit::new(0, &args, None);
        audit
            .file(&Source {
                key: "bad.parquet".into(),
                bytes: 11,
                etag: None,
                exchange: None,
                local_path: path,
            })
            .unwrap();
        assert_eq!(audit.failures.len(), 1);
        assert_eq!(audit.files_read, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn misaligned_and_outside_day_rows_do_not_fill_coverage() {
        let args = Args::try_parse_from(["validate-analysis", "--date", "2026-10-25"]).unwrap();
        let start = args
            .date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let batch = fixture(start);
        let index = batch.schema().index_of("as_of_ns").unwrap();
        let mut arrays = batch.columns().to_vec();
        arrays[index] = Arc::new(Int64Array::from(vec![
            start - SECOND,
            start + 1,
            start + DAY_SECONDS as i64 * SECOND,
            start - 1,
            start + 1,
        ]));
        let invalid = RecordBatch::try_new(batch.schema(), arrays).unwrap();
        let mut audit = Audit::new(start, &args, None);
        audit.batch(&invalid, None).unwrap();
        assert_eq!(audit.row_errors["outside_utc_day"], 3);
        assert_eq!(audit.row_errors["not_second_aligned"], 2);
        assert!(audit.instruments.is_empty());
    }
    #[test]
    fn duplicate_merges_conservatively_and_conflicts_invalidate() {
        let mut i = Instrument::new("x", "x:BTC", 0, DAY_SECONDS, true);
        i.record(5, 100, PRICE | TRADE, 0);
        i.record(5, 100, PRICE, INCOMPLETE);
        assert_eq!(i.slots[5].masks, PRICE);
        assert_eq!(i.slots[5].issues, DUPLICATE | INCOMPLETE);
        i.record(5, 101, PRICE, 0);
        assert_eq!(i.slots[5].masks, 0);
        assert_ne!(i.slots[5].issues & CONFLICT, 0);
        assert!(!i.slots[4].present);
    }
    #[test]
    fn histogram_rejects_invalid_ages_and_reports_upper_bounds() {
        let mut stats = FieldStats::default();
        assert!(!stats.observe_age(Some(-1.), 1000.));
        assert!(!stats.observe_age(Some(f64::NAN), 1000.));
        assert!(stats.observe_age(Some(75.), 1000.));
        assert!(!stats.observe_age(Some(2000.), 1000.));
        stats.finish();
        assert_eq!(stats.invalid_rows, 2);
        assert_eq!(stats.stale_rows, 1);
        assert_eq!(stats.p95_upper_bound_ms, Some(3000.));
    }
}
