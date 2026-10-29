mod input;
mod report;
mod scan;

use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

pub const SECOND: i64 = 1_000_000_000;
pub const DAY_SECONDS: usize = 86_400;

#[derive(Parser)]
#[command(about = "Audit analysis_state_1s Parquet from S3 or local files (UTC day)")]
pub struct Args {
    #[arg(long)]
    pub date: NaiveDate,
    /// Local Parquet file or directory. Omit to list and download from S3.
    #[arg(long)]
    pub input: Option<PathBuf>,
    #[arg(long, default_value = ".env")]
    pub env_file: PathBuf,
    #[arg(long)]
    pub bucket: Option<String>,
    #[arg(long)]
    pub region: Option<String>,
    #[arg(long)]
    pub prefix: Option<String>,
    /// Repeat to select exchanges. Defaults to all four for S3.
    #[arg(long = "exchange")]
    pub exchanges: Vec<String>,
    #[arg(long, default_value = "data/cache/analysis")]
    pub cache_dir: PathBuf,
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// JSON array: exchange, instrument_id, optional start_ns/end_ns (UTC).
    #[arg(long)]
    pub expected_instruments: Option<PathBuf>,
    #[arg(long, default_value_t = 8)]
    pub download_concurrency: usize,
    /// Explicit partial run; missing-day metrics are not a full-day verdict.
    #[arg(long)]
    pub max_files: Option<usize>,
    #[arg(long, default_value_t = 1000.0)]
    pub book_max_age_ms: f64,
    #[arg(long, default_value_t = 10_000.0)]
    pub price_max_age_ms: f64,
    #[arg(long, default_value_t = 180_000.0)]
    pub oi_max_age_ms: f64,
    #[arg(long, default_value_t = 900_000.0)]
    pub funding_max_age_ms: f64,
    /// Also emit second-by-second usability masks, including missing seconds.
    #[arg(long)]
    pub write_masks: bool,
    /// Return exit code 2 if hard row errors, duplicates or missing seconds exist.
    #[arg(long)]
    pub fail_on_issues: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Expected {
    pub exchange: String,
    pub instrument_id: String,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
}

pub async fn run() -> Result<()> {
    let args = Args::parse();
    ensure!(
        (1..=32).contains(&args.download_concurrency),
        "download concurrency must be 1..32"
    );
    ensure!(args.max_files != Some(0), "max-files must be positive");
    for age in [
        args.book_max_age_ms,
        args.price_max_age_ms,
        args.oi_max_age_ms,
        args.funding_max_age_ms,
    ] {
        ensure!(
            age.is_finite() && age > 0.0,
            "age thresholds must be finite and positive"
        );
    }
    for exchange in &args.exchanges {
        ensure!(
            ["bybit", "aster", "grvt", "hyperliquid"].contains(&exchange.as_str()),
            "unsupported exchange"
        );
    }
    let start = args
        .date
        .and_hms_opt(0, 0, 0)
        .context("invalid UTC date")?
        .and_utc()
        .timestamp_nanos_opt()
        .context("date outside nanosecond range")?;
    let end = start
        .checked_add(DAY_SECONDS as i64 * SECOND)
        .context("date overflow")?;
    let expected: Option<Vec<Expected>> = args
        .expected_instruments
        .as_ref()
        .map(|path| -> Result<_> { Ok(serde_json::from_slice(&std::fs::read(path)?)?) })
        .transpose()?;
    if let Some(items) = &expected {
        ensure!(!items.is_empty(), "expected instrument list is empty");
        let mut keys = std::collections::BTreeSet::new();
        for item in items {
            ensure!(
                !item.exchange.is_empty() && !item.instrument_id.is_empty(),
                "empty expected instrument key"
            );
            ensure!(
                keys.insert((&item.exchange, &item.instrument_id)),
                "duplicate expected instrument key"
            );
            ensure!(
                args.exchanges.is_empty() || args.exchanges.contains(&item.exchange),
                "expected instrument outside exchange filter"
            );
            ensure!(
                item.start_ns.unwrap_or(start).max(start) < item.end_ns.unwrap_or(end).min(end),
                "expected window does not overlap UTC day"
            );
        }
    }
    let output = args
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("data/reports/analysis-{}", args.date)));
    // Never overwrite a finished report. Interrupted runs can be restarted separately.
    std::fs::create_dir_all(&output)?;
    ensure!(
        !output.join("summary.json").exists(),
        "output contains a completed report; choose a new --output"
    );
    let mut audit = scan::Audit::new(start, &args, expected.as_deref());
    let manifest = input::collect(&args, &mut audit).await?;
    let hard_issues = report::write(&args, &output, &manifest, &mut audit, expected.is_some())?;
    println!("Report: {}", output.join("summary.json").display());
    if args.max_files.is_some() || args.input.is_some() {
        println!(
            "Scope: potentially partial input; missing seconds are diagnostic, not a full-day verdict."
        );
    }
    println!(
        "Files: {}, rows: {}, instruments: {}, hard issues: {}",
        manifest.len(),
        audit.rows,
        audit.instruments.len(),
        hard_issues
    );
    if args.fail_on_issues && hard_issues > 0 {
        std::process::exit(2);
    }
    Ok(())
}

pub fn increment(counts: &mut BTreeMap<String, u64>, name: &str) {
    *counts.entry(name.to_owned()).or_default() += 1;
}
