mod config;
mod input;
mod market;
mod simulation;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Clone, Parser, Serialize)]
#[command(about = "Rust cross-venue z-score sweep over S3 book_state_20 (research proxy)")]
pub struct Args {
    /// JSON parameter file. Explicit CLI options override file values.
    #[arg(long)]
    #[serde(skip)]
    pub config: Option<PathBuf>,
    /// Validate and print effective parameters without reading market data.
    #[arg(long)]
    #[serde(skip)]
    pub print_config: bool,
    /// Event-date partitions around the receive-time range (late arrivals / clock skew).
    #[arg(long, default_value_t = 1)]
    pub partition_pad_days: u32,
    /// Sampling interval in milliseconds; source remains native subsecond books.
    #[arg(long, default_value_t = 100)]
    pub sample_ms: usize,
    /// Inclusive RFC3339 timestamp, e.g. 2026-10-25T00:00:00Z.
    #[arg(long)]
    pub start: DateTime<Utc>,
    /// Exclusive RFC3339 timestamp. Warmup is inside the requested period.
    #[arg(long)]
    pub end: DateTime<Utc>,
    #[arg(long, default_value = "bybit")]
    pub left: String,
    #[arg(long, default_value = "aster")]
    pub right: String,
    /// USDT assets to compare independently on Bybit/Aster, e.g. BTC,ETH,SOL.
    #[arg(long, value_delimiter = ',')]
    pub assets: Vec<String>,
    /// Exact instrument IDs for single-pair mode (omit when assets is set).
    #[arg(long, default_value = "")]
    pub left_instrument: String,
    #[arg(long, default_value = "")]
    pub right_instrument: String,
    #[arg(long, value_delimiter = ',', default_value = "5000,10000,30000")]
    pub windows_ms: Vec<usize>,
    #[arg(long, value_delimiter = ',', default_value = "1.5,2,2.5")]
    pub entry_z: Vec<f64>,
    #[arg(long, value_delimiter = ',', default_value = "0.5")]
    pub exit_z: Vec<f64>,
    #[arg(long, default_value_t = 4.0)]
    pub stop_z: f64,
    #[arg(long, default_value_t = 0.0)]
    pub min_deviation_bps: f64,
    #[arg(long, default_value_t = 300)]
    pub max_hold_seconds: i64,
    /// Execution at next valid paired observation at least this many milliseconds later.
    #[arg(long, default_value_t = 100)]
    pub latency_ms: i64,
    #[arg(long, default_value_t = 1000.0)]
    pub book_max_age_ms: f64,
    #[arg(long, default_value_t = 5.0)]
    pub left_fee_bps: f64,
    #[arg(long, default_value_t = 5.0)]
    pub right_fee_bps: f64,
    #[arg(long, default_value_t = 1000.0)]
    pub notional_per_leg: f64,
    #[arg(long, default_value_t = 100)]
    pub max_runs: usize,
    /// Local Parquet file/directory for offline repeats; omit to refresh S3 manifest.
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
    #[arg(skip)]
    #[serde(skip)]
    pub exchanges: Vec<String>,
    #[arg(long, default_value = "data/cache/books")]
    pub cache_dir: PathBuf,
    #[arg(long, default_value_t = 8)]
    pub download_concurrency: usize,
    /// Diagnostic only: cap files per exchange/day (local: total); marks result partial.
    #[arg(long)]
    pub max_files: Option<usize>,
    #[arg(long)]
    pub output: PathBuf,
}

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub async fn run() -> Result<()> {
    let mut args = config::load()?;
    ensure!(
        args.partition_pad_days <= 7,
        "partition padding must be <= 7 days"
    );
    ensure!(args.start < args.end, "start must precede end");
    args.start
        .timestamp_nanos_opt()
        .context("start outside ns range")?;
    args.end
        .timestamp_nanos_opt()
        .context("end outside ns range")?;
    ensure!(
        (1..1000).contains(&args.sample_ms),
        "sample-ms must be 1..999"
    );
    ensure!(args.left != args.right, "choose different exchanges");
    for ex in [&args.left, &args.right] {
        ensure!(
            ["bybit", "aster", "grvt", "hyperliquid"].contains(&ex.as_str()),
            "unsupported exchange"
        );
    }
    args.assets = args
        .assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect();
    let targets = targets(&args)?;
    ensure!(
        (1..=32).contains(&args.download_concurrency) && args.max_files != Some(0),
        "invalid download limits"
    );
    ensure!(
        (1..=86400000).contains(&args.latency_ms) && (1..=86400).contains(&args.max_hold_seconds),
        "latency and hold must be positive"
    );
    ensure!(
        args.book_max_age_ms.is_finite()
            && args.book_max_age_ms > 0.0
            && args.notional_per_leg.is_finite()
            && args.notional_per_leg > 0.0,
        "invalid age/notional"
    );
    for n in [
        args.left_fee_bps,
        args.right_fee_bps,
        args.min_deviation_bps,
    ] {
        ensure!(n.is_finite() && n >= 0.0, "invalid fee/deviation");
    }
    let count = args
        .windows_ms
        .len()
        .checked_mul(args.entry_z.len())
        .and_then(|n| n.checked_mul(args.exit_z.len()))
        .and_then(|n| n.checked_mul(targets.len()))
        .context("grid overflow")?;
    ensure!(
        count > 0 && count <= args.max_runs,
        "grid exceeds max-runs or is empty"
    );
    for &w in &args.windows_ms {
        ensure!(
            w >= args.sample_ms * 2 && w <= 86400000 && w % args.sample_ms == 0,
            "window-ms must be a sample-ms multiple, at least 2 samples and at most 1 day"
        );
    }
    for &entry in &args.entry_z {
        for &exit in &args.exit_z {
            ensure!(
                entry.is_finite()
                    && exit.is_finite()
                    && args.stop_z.is_finite()
                    && 0.0 <= exit
                    && exit < entry
                    && entry < args.stop_z,
                "require 0 <= exit < entry < stop"
            );
        }
    }
    if args.print_config {
        println!("{}", serde_json::to_string_pretty(&args)?);
        return Ok(());
    }
    ensure!(
        !args.output.exists(),
        "output exists; choose a new output directory"
    );
    args.exchanges = vec![args.left.clone(), args.right.clone()];
    let sources = input::collect(&args).await?;
    let mut missing_partitions = Vec::new();
    if args.input.is_none() {
        for date in input::dates(&args)? {
            for exchange in &args.exchanges {
                let suffix = format!("exchange={exchange}/date={date}/");
                if !sources.iter().any(|source| source.key.contains(&suffix)) {
                    missing_partitions.push(suffix);
                }
            }
        }
    }
    if args.assets.is_empty() {
        execute(&args, &sources, &missing_partitions)?;
    } else {
        std::fs::create_dir_all(&args.output)?;
        let mut results = Vec::new();
        for (asset, left, right) in targets {
            let mut pair = args.clone();
            pair.assets.clear();
            pair.left_instrument = left;
            pair.right_instrument = right;
            pair.output = args.output.join(&asset);
            eprintln!("Evaluating {asset}");
            let report = execute(&pair, &sources, &missing_partitions)?;
            results.push(serde_json::json!({
                "asset": asset, "report": format!("{asset}/summary.json"),
                "paired_samples": report["paired_samples"], "quality": report["quality"],
                "runs": report["runs"]
            }));
        }
        let summary = serde_json::json!({
            "configuration": args, "assets": results, "total_variants": count,
            "partial": args.max_files.is_some() || args.input.is_some() || !missing_partitions.is_empty(),
            "missing_partitions": missing_partitions,
            "evaluation": "Independent asset experiments; not a shared-capital portfolio."
        });
        std::fs::write(
            args.output.join("summary.json"),
            serde_json::to_vec_pretty(&summary)?,
        )?;
        println!("{}", args.output.join("summary.json").display());
    }
    Ok(())
}

fn targets(args: &Args) -> Result<Vec<(String, String, String)>> {
    if args.assets.is_empty() {
        ensure!(
            !args.left_instrument.is_empty() && !args.right_instrument.is_empty(),
            "set --assets or both --left-instrument and --right-instrument"
        );
        return Ok(vec![(
            "pair".into(),
            args.left_instrument.clone(),
            args.right_instrument.clone(),
        )]);
    }
    ensure!(
        args.left_instrument.is_empty() && args.right_instrument.is_empty(),
        "assets cannot be combined with explicit instrument IDs"
    );
    ensure!(
        [&args.left, &args.right]
            .iter()
            .all(|ex| ["bybit", "aster"].contains(&ex.as_str())),
        "assets mode supports Bybit/Aster USDT contracts; use exact instrument IDs for other exchanges"
    );
    let mut seen = std::collections::BTreeSet::new();
    args.assets
        .iter()
        .map(|asset| {
            ensure!(
                !asset.is_empty()
                    && asset.len() <= 20
                    && asset
                        .bytes()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
                "invalid asset: {asset}"
            );
            ensure!(seen.insert(asset), "duplicate asset: {asset}");
            Ok((
                asset.clone(),
                format!("{}:{asset}USDT", args.left),
                format!("{}:{asset}USDT", args.right),
            ))
        })
        .collect()
}

fn execute(
    args: &Args,
    sources: &[input::Source],
    missing_partitions: &[String],
) -> Result<serde_json::Value> {
    let (frames, quality) = market::load(args, sources)?;
    eprintln!(
        "Paired usable samples: {}; quality: {}",
        frames.len(),
        serde_json::to_string(&quality)?
    );
    std::fs::create_dir_all(&args.output)?;
    let mut runs = Vec::new();
    for &window in &args.windows_ms {
        let signals = simulation::signals(
            &frames,
            window / args.sample_ms,
            args.sample_ms as i64 * 1_000_000,
        );
        for &entry in &args.entry_z {
            for &exit in &args.exit_z {
                let result = simulation::evaluate(args, &frames, &signals, window, entry, exit);
                std::fs::write(
                    args.output.join(format!("{}.json", result.variant_id)),
                    serde_json::to_vec_pretty(&result)?,
                )?;
                runs.push(result.summary());
            }
        }
    }
    let manifest_bytes = serde_json::to_vec(&sources)?;
    std::fs::write(args.output.join("sources.json"), &manifest_bytes)?;
    let report = serde_json::json!({
        "hypothesis_id": "cross_venue_log_mid_zscore", "version": 1,
        "reference_pr": "https://github.com/KFAC-2026FALL/stat-arb/pull/3",
        "implementation_sha256": digest(concat!(include_str!("mod.rs"), include_str!("config.rs"), include_str!("input.rs"), include_str!("market.rs"), include_str!("simulation.rs")).as_bytes()),
        "manifest_sha256": digest(&manifest_bytes), "configuration": args,
        "partial": args.max_files.is_some() || args.input.is_some() || !missing_partitions.is_empty(), "missing_partitions": missing_partitions, "quality": quality,
        "paired_samples": frames.len(), "runs": runs,
        "model": "Subsecond as-of BBO from normalized book_state_20; delayed fills; equal entry notionals; linear quantity PnL; fees on four actual fill notionals. No funding, depth, partial fills or margin. Unclosed positions marked separately; not executable-profit evidence."
    });
    std::fs::write(
        args.output.join("summary.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", args.output.join("summary.json").display());
    Ok(report)
}

#[cfg(test)]
mod target_tests {
    use super::*;
    fn args(assets: &str) -> Args {
        Args::parse_from([
            "zscore",
            "--start",
            "2026-10-20T00:00:00Z",
            "--end",
            "2026-10-30T00:00:00Z",
            "--assets",
            assets,
            "--output",
            "unused",
        ])
    }
    #[test]
    fn maps_assets_without_merging_quote_currencies() {
        let targets = targets(&args("BTC,ETH")).unwrap();
        assert_eq!(
            targets[1],
            ("ETH".into(), "bybit:ETHUSDT".into(), "aster:ETHUSDT".into())
        );
    }
    #[test]
    fn rejects_duplicates_unsafe_paths_and_ambiguous_instruments() {
        for assets in ["BTC,BTC", "../ETH", "BTC/ETH"] {
            assert!(targets(&args(assets)).is_err());
        }
        let mut a = args("BTC");
        a.left_instrument = "bybit:BTCUSDT".into();
        assert!(targets(&a).is_err());
        a.left_instrument.clear();
        a.right = "hyperliquid".into();
        assert!(targets(&a).is_err());
    }
}
