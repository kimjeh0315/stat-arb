use super::{Args, input::Source};
use anyhow::{Context, Result, ensure};
use arrow::{array::*, record_batch::RecordBatch};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
}
impl Quote {
    pub fn mid(self) -> f64 {
        (self.bid + self.ask) / 2.0
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Frame {
    pub time_ns: i64,
    pub left: Quote,
    pub right: Quote,
}
impl Frame {
    pub fn basis(self) -> f64 {
        (self.left.mid().ln() - self.right.mid().ln()) * 10_000.0
    }
}
#[derive(Default, Serialize)]
pub struct Quality {
    pub scanned_rows: usize,
    pub skipped_row_groups: usize,
    pub selected_rows: usize,
    pub rejected_rows: usize,
    pub duplicate_timestamps: usize,
    pub left_events: usize,
    pub right_events: usize,
    pub left_subsecond_intervals: usize,
    pub right_subsecond_intervals: usize,
    pub first_paired_ns: Option<i64>,
    pub last_paired_ns: Option<i64>,
    pub sampled_slots: usize,
    pub unavailable_slots: usize,
}
fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref()
        .with_context(|| format!("wrong type for {name}"))
}
fn number(a: &Decimal128Array, row: usize) -> Option<f64> {
    (!a.is_null(row)).then(|| a.value(row) as f64 * 10_f64.powi(-i32::from(a.scale())))
}
fn best(levels: &ListArray, row: usize) -> Result<Option<f64>> {
    if levels.is_null(row) {
        return Ok(None);
    }
    let values = levels.value(row);
    let values = values
        .as_any()
        .downcast_ref::<StructArray>()
        .context("levels must be structs")?;
    if values.is_empty() || values.is_null(0) {
        return Ok(None);
    }
    let prices = values
        .column_by_name("price")
        .context("missing level price")?
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .context("invalid price type")?;
    let qty = values
        .column_by_name("qty")
        .context("missing level qty")?
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .context("invalid qty type")?;
    Ok(number(prices, 0).filter(|p| {
        p.is_finite() && *p > 0.0 && number(qty, 0).is_some_and(|q| q.is_finite() && q > 0.0)
    }))
}
type Events = BTreeMap<i64, Option<Quote>>;
fn resample(
    sides: &[Events; 2],
    start: i64,
    end: i64,
    step: i64,
    max_age: i64,
    quality: &mut Quality,
) -> Vec<Frame> {
    let mut iters = [sides[0].iter().peekable(), sides[1].iter().peekable()];
    let mut latest = [None, None];
    let mut frames = Vec::new();
    let mut t = start;
    while t < end {
        quality.sampled_slots += 1;
        for side in 0..2 {
            while iters[side].peek().is_some_and(|(time, _)| **time <= t) {
                let (&time, &quote) = iters[side].next().unwrap();
                latest[side] = Some((time, quote));
            }
        }
        let usable = |side: usize| {
            latest[side].and_then(|(time, q)| if t - time <= max_age { q } else { None })
        };
        if let (Some(left), Some(right)) = (usable(0), usable(1)) {
            frames.push(Frame {
                time_ns: t,
                left,
                right,
            });
        } else {
            quality.unavailable_slots += 1;
        }
        let Some(next) = t.checked_add(step) else {
            break;
        };
        t = next;
    }
    frames
}
pub fn load(args: &Args, sources: &[Source]) -> Result<(Vec<Frame>, Quality)> {
    let mut sides: [Events; 2] = Default::default();
    let mut quality = Quality::default();
    let mut identities: [Option<(String, String)>; 2] = [None, None];
    let start = args.start.timestamp_nanos_opt().unwrap();
    let end = args.end.timestamp_nanos_opt().unwrap();
    for source in sources {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&source.local_path)?)?;
        let names = [
            "received_at_ns",
            "exchange",
            "instrument_id",
            "base_asset",
            "quote_asset",
            "book_valid",
            "bids",
            "asks",
        ];
        let indices = names
            .iter()
            .map(|name| builder.schema().index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let timestamp_leaf = builder
            .parquet_schema()
            .columns()
            .iter()
            .position(|column| column.path().parts() == ["received_at_ns"])
            .context("missing timestamp leaf")?;
        let row_groups: Vec<_> = builder
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(index, group)| {
                let outside = match group.column(timestamp_leaf).statistics() {
                    Some(parquet::file::statistics::Statistics::Int64(stats)) => {
                        stats.max_opt().is_some_and(|max| *max < start)
                            || stats.min_opt().is_some_and(|min| *min >= end)
                    }
                    _ => false,
                };
                if outside {
                    quality.skipped_row_groups += 1;
                    None
                } else {
                    Some(index)
                }
            })
            .collect();
        let projection = ProjectionMask::roots(builder.parquet_schema(), indices);
        let reader = builder
            .with_projection(projection)
            .with_row_groups(row_groups)
            .with_batch_size(8192)
            .build()?;
        for batch in reader {
            let batch = batch?;
            let times = column::<Int64Array>(&batch, "received_at_ns")?;
            let exchanges = column::<StringArray>(&batch, "exchange")?;
            let ids = column::<StringArray>(&batch, "instrument_id")?;
            let bases = column::<StringArray>(&batch, "base_asset")?;
            let quotes = column::<StringArray>(&batch, "quote_asset")?;
            let valid = column::<BooleanArray>(&batch, "book_valid")?;
            let bids = column::<ListArray>(&batch, "bids")?;
            let asks = column::<ListArray>(&batch, "asks")?;
            for row in 0..batch.num_rows() {
                quality.scanned_rows += 1;
                if times.is_null(row) || exchanges.is_null(row) || ids.is_null(row) {
                    continue;
                }
                let ns = times.value(row);
                if ns < start || ns >= end {
                    continue;
                }
                let side = if exchanges.value(row) == args.left
                    && ids.value(row) == args.left_instrument
                {
                    0
                } else if exchanges.value(row) == args.right
                    && ids.value(row) == args.right_instrument
                {
                    1
                } else {
                    continue;
                };
                quality.selected_rows += 1;
                ensure!(
                    !bases.is_null(row) && !quotes.is_null(row),
                    "missing contract identity"
                );
                let identity = (bases.value(row).to_owned(), quotes.value(row).to_owned());
                ensure!(
                    identities[side].as_ref().is_none_or(|old| *old == identity),
                    "contract identity changed"
                );
                identities[side] = Some(identity);
                let quote = match (best(bids, row)?, best(asks, row)?) {
                    (Some(bid), Some(ask))
                        if !valid.is_null(row) && valid.value(row) && bid < ask =>
                    {
                        Some(Quote { bid, ask })
                    }
                    _ => None,
                };
                if quote.is_none() {
                    quality.rejected_rows += 1;
                }
                // Invalid books invalidate the previous book. Ambiguous same-time updates are excluded.
                match sides[side].entry(ns) {
                    std::collections::btree_map::Entry::Occupied(mut e) => {
                        e.insert(None);
                        quality.duplicate_timestamps += 1;
                    }
                    std::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(quote);
                    }
                }
            }
        }
    }
    if let (Some(l), Some(r)) = (&identities[0], &identities[1]) {
        ensure!(
            l == r,
            "pair must have identical base and quote assets: {l:?} vs {r:?}"
        );
    }
    quality.left_events = sides[0].len();
    quality.right_events = sides[1].len();
    let subsecond = |events: &Events| {
        let times: Vec<_> = events.keys().copied().collect();
        times
            .windows(2)
            .filter(|t| t[1] - t[0] < 1_000_000_000)
            .count()
    };
    quality.left_subsecond_intervals = subsecond(&sides[0]);
    quality.right_subsecond_intervals = subsecond(&sides[1]);
    let frames = resample(
        &sides,
        start,
        end,
        args.sample_ms as i64 * 1_000_000,
        (args.book_max_age_ms * 1_000_000.0) as i64,
        &mut quality,
    );
    quality.first_paired_ns = frames.first().map(|f| f.time_ns);
    quality.last_paired_ns = frames.last().map(|f| f.time_ns);
    Ok((frames, quality))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn asof_never_uses_future_and_invalid_book_clears_previous() {
        let q = Quote {
            bid: 99.0,
            ask: 101.0,
        };
        let sides = [
            BTreeMap::from([(50, Some(q)), (250, None), (450, Some(q))]),
            BTreeMap::from([(150, Some(q))]),
        ];
        let mut quality = Quality::default();
        let f = resample(&sides, 0, 700, 100, 400, &mut quality);
        assert_eq!(
            f.iter().map(|f| f.time_ns).collect::<Vec<_>>(),
            vec![200, 500]
        );
        assert_eq!(quality.unavailable_slots, 5);
    }
}
