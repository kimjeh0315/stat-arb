use super::{Args, DAY_SECONDS, SECOND, input::Source, scan::*};
use anyhow::Result;
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

fn csv(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub fn write(
    args: &Args,
    output: &Path,
    manifest: &[Source],
    audit: &mut Audit,
    authoritative: bool,
) -> Result<u64> {
    let mut csv_file = BufWriter::new(File::create(output.join("instruments.csv"))?);
    writeln!(
        csv_file,
        "exchange,instrument_id,expected_seconds,present_seconds,missing_seconds,coverage_ratio,duplicate_seconds,conflict_seconds,longest_gap_seconds,price_usable_seconds,trade_usable_seconds,oi_usable_seconds,funding_usable_seconds"
    )?;
    let mut incidents = BufWriter::new(File::create(output.join("incidents.jsonl"))?);
    let mut masks = if args.write_masks {
        Some(mask_writer(output)?)
    } else {
        None
    };
    let mut summaries = Vec::<Value>::new();
    let mut minutes = BTreeMap::<(String, usize), [u64; 4]>::new();
    let mut totals = [0_u64; 9]; // expected, present, missing, duplicate, conflict, usable[4]
    let mut issue_seconds = BTreeMap::<String, u64>::new();
    let mut hard_issues = audit.row_errors.values().sum::<u64>()
        + audit.failures.len() as u64
        + audit.missing_exchanges.len() as u64;
    for instrument in audit.instruments.values_mut() {
        let mut counts = [0_u64; 9];
        counts[0] = (instrument.end - instrument.begin) as u64;
        let mut longest_gap = 0;
        let mut gap = 0;
        let mut per_issue = BTreeMap::<String, u64>::new();
        for second in instrument.begin..instrument.end {
            let slot = instrument.slots[second];
            let minute = minutes
                .entry((instrument.exchange.clone(), second / 60))
                .or_default();
            minute[0] += 1;
            if slot.present {
                counts[1] += 1;
                gap = 0;
            } else {
                counts[2] += 1;
                gap += 1;
                longest_gap = longest_gap.max(gap);
                minute[1] += 1;
            }
            if slot.issues & DUPLICATE != 0 {
                counts[3] += 1;
            }
            if slot.issues & CONFLICT != 0 {
                counts[4] += 1;
            }
            for (index, mask) in [PRICE, TRADE, OI, FUNDING].into_iter().enumerate() {
                if slot.masks & mask != 0 {
                    counts[index + 5] += 1;
                }
            }
            if slot.masks & PRICE == 0 {
                minute[2] += 1;
            }
            if slot.present && slot.issues & DISCONNECTED != 0 {
                minute[3] += 1;
            }
            for (flag, name) in INCIDENTS {
                if slot.issues & flag != 0 {
                    *per_issue.entry((*name).into()).or_default() += 1;
                    *issue_seconds.entry((*name).into()).or_default() += 1;
                }
            }
        }
        // Emit independent contiguous spans per reason, rather than mixing unrelated faults.
        for (flag, name) in INCIDENTS {
            let mut begin = None;
            for second in instrument.begin..=instrument.end {
                let bad = second < instrument.end && instrument.slots[second].issues & flag != 0;
                if bad && begin.is_none() {
                    begin = Some(second);
                }
                if !bad && let Some(first) = begin.take() {
                    serde_json::to_writer(
                        &mut incidents,
                        &json!({"exchange":instrument.exchange,"instrument_id":instrument.instrument_id,"reason":name,"start_ns":audit.start+first as i64*SECOND,"end_ns_exclusive":audit.start+second as i64*SECOND,"seconds":second-first}),
                    )?;
                    writeln!(incidents)?;
                }
            }
        }
        for stats in instrument.fields.values_mut() {
            stats.finish();
        }
        hard_issues += counts[2]
            + counts[3]
            + instrument.errors.values().sum::<u64>()
            + per_issue.get("invalid_numeric_value").copied().unwrap_or(0)
            + per_issue.get("trade_inconsistent").copied().unwrap_or(0)
            + per_issue.get("invalid_age").copied().unwrap_or(0)
            + per_issue
                .get("outside_expected_window")
                .copied()
                .unwrap_or(0);
        for (total, n) in totals.iter_mut().zip(counts) {
            *total += n;
        }
        let coverage = if counts[0] == 0 {
            0.0
        } else {
            counts[1] as f64 / counts[0] as f64
        };
        writeln!(
            csv_file,
            "{},{},{},{},{},{:.9},{},{},{},{},{},{},{}",
            csv(&instrument.exchange),
            csv(&instrument.instrument_id),
            counts[0],
            counts[1],
            counts[2],
            coverage,
            counts[3],
            counts[4],
            longest_gap,
            counts[5],
            counts[6],
            counts[7],
            counts[8]
        )?;
        if let Some(writer) = &mut masks {
            write_masks(writer, instrument, audit.start)?;
        }
        let never_observed: Vec<_> = instrument
            .fields
            .iter()
            .filter(|(_, v)| v.present_rows == 0)
            .map(|(k, _)| k)
            .collect();
        let usable_ratios = ["price", "trade", "oi", "funding"]
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    name,
                    (counts[0] > 0).then(|| counts[index + 5] as f64 / counts[0] as f64),
                )
            })
            .collect::<BTreeMap<_, _>>();
        summaries.push(json!({
            "exchange": instrument.exchange,
            "instrument_id": instrument.instrument_id,
            "in_expected_universe": instrument.declared,
            "observed_rows_including_duplicates": instrument.rows,
            "expected_seconds": counts[0],
            "present_seconds": counts[1],
            "missing_seconds": counts[2],
            "coverage_ratio": coverage,
            "duplicate_seconds": counts[3],
            "conflict_seconds": counts[4],
            "longest_gap_seconds": longest_gap,
            "price_usable_seconds": counts[5],
            "trade_usable_seconds": counts[6],
            "oi_usable_seconds": counts[7],
            "funding_usable_seconds": counts[8],
            "usable_ratios": usable_ratios,
            "quality_flags": instrument.flags,
            "liquidation_coverage": instrument.coverage,
            "issue_seconds": per_issue,
            "row_errors": instrument.errors,
            "book_age_exceeds_ms": instrument.book_age_over_ms,
            "fields": instrument.fields,
            "never_observed_fields_unclassified": never_observed
        }));
    }
    if let Some(writer) = masks {
        writer.close()?;
    }
    csv_file.flush()?;
    incidents.flush()?;
    let mut timeline = BufWriter::new(File::create(output.join("timeline_1m.csv"))?);
    writeln!(
        timeline,
        "exchange,minute_utc,expected_instrument_seconds,missing_instrument_seconds,price_unusable_instrument_seconds,disconnected_instrument_seconds"
    )?;
    for ((exchange, minute), values) in minutes {
        let t =
            chrono::DateTime::from_timestamp(audit.start / SECOND + minute as i64 * 60, 0).unwrap();
        writeln!(
            timeline,
            "{},{},{},{},{},{}",
            csv(&exchange),
            t.to_rfc3339(),
            values[0],
            values[1],
            values[2],
            values[3]
        )?;
    }
    timeline.flush()?;
    let partial = args.max_files.is_some() || args.input.is_some();
    let mut warnings = vec![
        "Ages measure time since local receipt; they are not exchange-to-server latency.",
        "Connection flags share aggregate connection state; channel health is not independently proven.",
        "Never-observed fields are not automatically classified as unsupported; consult exchange capabilities.",
        "Age percentiles are histogram upper bounds, not exact quantiles.",
        "Raw orderbook/trade recomputation and price jump verification are not performed by this audit.",
    ];
    if !authoritative {
        warnings.push("Expected contracts are inferred from observed rows. Completely absent contracts cannot be detected.");
    }
    if partial {
        warnings.push("Local input or max-files may be partial: full-day coverage numbers are diagnostic, not a complete S3-day verdict.");
    }
    let operational_complete = audit.failures.is_empty() && audit.missing_exchanges.is_empty();
    let report = json!({
        "utc_date": args.date.to_string(),
        "day_start_ns": audit.start,
        "day_end_ns_exclusive": audit.start + DAY_SECONDS as i64 * SECOND,
        "source": if args.input.is_some() { "local" } else { "s3" },
        "partial_scope": partial,
        "expected_manifest_supplied": authoritative,
        "expected_universe": if authoritative { "explicit_manifest" } else { "observed_contracts_only" },
        "audit_complete": operational_complete,
        "full_day_s3_scan_complete": operational_complete && !partial,
        "file_count": manifest.len(),
        "files_read_successfully": audit.files_read,
        "file_bytes": manifest.iter().map(|s| s.bytes).sum::<u64>(),
        "rows_including_duplicates_and_rejected": audit.rows,
        "filtered_rows": audit.filtered_rows,
        "instrument_count": audit.instruments.len(),
        "expected_instrument_seconds": totals[0],
        "present_instrument_seconds": totals[1],
        "missing_instrument_seconds": totals[2],
        "duplicate_instrument_seconds": totals[3],
        "conflicting_duplicate_instrument_seconds": totals[4],
        "price_usable_instrument_seconds": totals[5],
        "trade_usable_instrument_seconds": totals[6],
        "oi_usable_instrument_seconds": totals[7],
        "funding_usable_instrument_seconds": totals[8],
        "hard_issue_count_nonexclusive": hard_issues,
        "issue_bits": INCIDENTS.iter().map(|(bit, name)| (name.to_string(), *bit)).collect::<BTreeMap<_, _>>(),
        "issue_seconds": issue_seconds,
        "row_errors": audit.row_errors,
        "file_errors": audit.failures,
        "missing_exchange_partitions": audit.missing_exchanges,
        "thresholds_ms": {
            "book": args.book_max_age_ms,
            "price": args.price_max_age_ms,
            "oi": args.oi_max_age_ms,
            "funding": args.funding_max_age_ms
        },
        "warnings": warnings,
        "instruments": summaries
    });
    std::fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(manifest)?,
    )?;
    // A summary is written last, so it also marks a finished inspection.
    std::fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    if !operational_complete {
        eprintln!("WARNING: some files/partitions could not be inspected. See summary.json.");
    }
    Ok(hard_issues)
}

fn mask_schema() -> Arc<Schema> {
    let mut fields = vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("as_of_ns", DataType::Int64, false),
    ];
    for name in [
        "row_present",
        "price_usable",
        "trade_usable",
        "oi_usable",
        "funding_usable",
    ] {
        fields.push(Field::new(name, DataType::Boolean, false));
    }
    fields.push(Field::new("issue_bits", DataType::UInt32, false));
    Arc::new(Schema::new(fields))
}
fn mask_writer(output: &Path) -> Result<ArrowWriter<File>> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
        .build();
    Ok(ArrowWriter::try_new(
        File::create(output.join("usability.parquet"))?,
        mask_schema(),
        Some(properties),
    )?)
}
fn write_masks(writer: &mut ArrowWriter<File>, i: &Instrument, start: i64) -> Result<()> {
    let schema = mask_schema();
    for begin in (i.begin..i.end).step_by(8192) {
        let end = (begin + 8192).min(i.end);
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(
                (begin..end).map(|_| i.exchange.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                (begin..end).map(|_| i.instrument_id.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                (begin..end).map(|s| start + s as i64 * SECOND),
            )),
            Arc::new(BooleanArray::from(
                (begin..end)
                    .map(|s| i.slots[s].present)
                    .collect::<Vec<bool>>(),
            )),
        ];
        for bit in [PRICE, TRADE, OI, FUNDING] {
            arrays.push(Arc::new(BooleanArray::from(
                (begin..end)
                    .map(|s| i.slots[s].masks & bit != 0)
                    .collect::<Vec<bool>>(),
            )));
        }
        arrays.push(Arc::new(UInt32Array::from_iter_values(
            (begin..end).map(|s| i.slots[s].issues),
        )));
        writer.write(&RecordBatch::try_new(schema.clone(), arrays)?)?;
    }
    Ok(())
}
