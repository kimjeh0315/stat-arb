mod analysis;
mod bbo;
mod book;
mod exchanges;
mod journal;
mod model;
mod schema;
mod service;
mod storage;
use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use model::*;
use std::{path::PathBuf, time::Duration};
#[derive(Parser)]
#[command(version, about = "Native public perpetual market-data collector")]
struct Cli {
    #[arg(long, default_value = "config/collector.json")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Discover,
    #[command(alias = "run")]
    Collect {
        #[arg(long)]
        duration: Option<u64>,
    },
    Archive,
    Flush,
    ImportLegacy {
        input: PathBuf,
    },
    Replay {
        input: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Verify,
    Inspect {
        input: PathBuf,
        #[arg(long)]
        raw_dump: Option<PathBuf>,
    },
}
async fn shutdown(tx: tokio::sync::watch::Sender<bool>, duration: Option<u64>) {
    let signal = async {
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };
    tokio::select! {_=signal=>{},_=async{match duration{Some(s)=>tokio::time::sleep(Duration::from_secs(s)).await,None=>std::future::pending::<()>().await}}=>{}}
    let _ = tx.send(true);
}
fn replay(input: PathBuf, output: Option<PathBuf>, stale: u64) -> Result<()> {
    let envelopes = if input.to_string_lossy().ends_with(".wal.ready") {
        journal::read(&input, false)?
    } else {
        journal::legacy(&input)?
    };
    let mut events: Vec<_> = envelopes
        .into_iter()
        .flat_map(|e| e.events)
        .filter(|e| e.dataset != "analysis_state_1s")
        .collect();
    events.sort_by_key(|e| e.integer("received_at_ns").unwrap_or(0));
    ensure!(!events.is_empty(), "no normalized events to replay");
    let mut items = std::collections::BTreeMap::new();
    for e in &events {
        let i = Instrument {
            exchange: e.string("exchange").into(),
            native_symbol: e
                .string("instrument_id")
                .split_once(':')
                .map(|(_, s)| s)
                .unwrap_or("")
                .into(),
            native_base: e.string("base_asset").into(),
            base_asset: e.string("base_asset").into(),
            quote_asset: e.string("quote_asset").into(),
            settle_asset: e.string("quote_asset").into(),
            category: e.string("category").into(),
            multiplier: Decimal(SCALE),
            tick_size: None,
            qty_step: None,
            min_qty: None,
            funding_interval_hours: None,
        };
        items.insert(i.key(), i);
    }
    let instruments: Vec<_> = items.into_values().collect();
    let mut a = analysis::Analysis::new(
        &instruments,
        events[0].integer("received_at_ns").unwrap(),
        stale,
    );
    let mut derived = Vec::new();
    let last = events.last().unwrap().integer("received_at_ns").unwrap();
    for event in events {
        derived.extend(a.advance(event.integer("received_at_ns").unwrap())?);
        a.ingest(&event);
    }
    derived.extend(a.advance(last)?);
    if let Some(path) = output {
        let batches = derived.chunks(10000);
        let mut writer = None;
        for rows in batches {
            let batch = schema::batch("analysis_state_1s", rows)?;
            if writer.is_none() {
                writer = Some(parquet::arrow::ArrowWriter::try_new(
                    std::fs::File::create(&path)?,
                    batch.schema(),
                    None,
                )?);
            }
            writer.as_mut().unwrap().write(&batch)?;
        }
        if let Some(w) = writer {
            w.close()?;
        }
    }
    tracing::info!(
        rows = derived.len(),
        "replay finished (segment start may have incomplete initial state)"
    );
    Ok(())
}
fn inspect(input: &std::path::Path, raw_dump: Option<PathBuf>) -> Result<()> {
    if input.extension().is_some_and(|s| s == "parquet") {
        use arrow::array::Array;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(input)?,
        )?;
        let mut nulls = std::collections::BTreeMap::<String, usize>::new();
        let mut rows = 0;
        for batch in builder.build()? {
            let batch = batch?;
            rows += batch.num_rows();
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                *nulls.entry(field.name().clone()).or_default() += column.null_count();
            }
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"rows":rows,"null_counts":nulls}))?
        );
        return Ok(());
    }
    let paths = if input.is_dir() {
        let mut p: Vec<_> = std::fs::read_dir(input)?
            .filter_map(|p| p.ok().map(|p| p.path()))
            .filter(|p| p.extension().is_some_and(|e| e == "ready"))
            .collect();
        p.sort();
        p
    } else {
        vec![input.into()]
    };
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    let mut valid = std::collections::BTreeMap::<String, usize>::new();
    let mut bbo_valid = std::collections::BTreeMap::<String, usize>::new();
    let mut asofs = std::collections::BTreeMap::<String, Vec<i64>>::new();
    let mut output = raw_dump.map(std::fs::File::create).transpose()?;
    for path in paths {
        for envelope in journal::read(&path, false)? {
            if let Some(r) = envelope.raw {
                *counts.entry(format!("{}:raw", r.exchange)).or_default() += 1;
                if let Some(f) = output.as_mut() {
                    use std::io::Write;
                    serde_json::to_writer(&mut *f, &r)?;
                    f.write_all(b"\n")?;
                }
            }
            for e in envelope.events {
                *counts
                    .entry(format!("{}:{}", e.string("exchange"), e.dataset))
                    .or_default() += 1;
                if e.dataset == "book_state_20" && e.boolean("book_valid") {
                    *valid.entry(e.string("instrument_id").into()).or_default() += 1;
                }
                if e.dataset == "bbo" && e.boolean("quote_valid") {
                    *bbo_valid.entry(e.string("instrument_id").into()).or_default() += 1;
                }
                if e.dataset == "analysis_state_1s" {
                    asofs
                        .entry(e.string("instrument_id").into())
                        .or_default()
                        .push(e.integer("as_of_ns").unwrap());
                }
            }
        }
    }
    let mut gaps = 0;
    for times in asofs.values_mut() {
        times.sort();
        times.dedup();
        gaps += times
            .windows(2)
            .filter(|pair| pair[1] - pair[0] != SECOND)
            .count();
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"counts":counts,"valid_book_instruments":valid.len(),"valid_bbo_instruments":bbo_valid.len(),"bbo_rows_per_instrument":bbo_valid,"analysis_instruments":asofs.len(),"analysis_time_gaps":gaps})
        )?
    );
    Ok(())
}
fn verify(config: &std::path::Path) -> Result<()> {
    let d: Decimal = "1000000.000000000000000001".parse()?;
    ensure!(
        d.to_string() == "1000000.000000000000000001",
        "decimal round trip"
    );
    ensure!(
        "NaN".parse::<Decimal>().is_err() && "0.0000000000000000001".parse::<Decimal>().is_err(),
        "reject invalid precision"
    );
    ensure!(
        Decimal(i128::MAX).add(Decimal(1)).is_err(),
        "checked overflow"
    );
    let i = Instrument {
        exchange: "bybit".into(),
        native_symbol: "BTCUSDT".into(),
        native_base: "BTC".into(),
        base_asset: "BTC".into(),
        quote_asset: "USDT".into(),
        settle_asset: "USDT".into(),
        category: "앵커".into(),
        multiplier: Decimal(SCALE),
        tick_size: None,
        qty_step: None,
        min_qty: None,
        funding_interval_hours: Some(Decimal(8 * SCALE)),
    };
    let mut b = book::Book::default();
    b.snapshot(
        &serde_json::json!([["100", "2"], ["99", "3"]]),
        &serde_json::json!([["101", "4"]]),
        Some(1),
    )?;
    b.delta(
        &serde_json::json!([["100", "0"]]),
        &serde_json::json!([]),
        Some(2),
        Some(1),
    )?;
    ensure!(
        b.delta(
            &serde_json::json!([]),
            &serde_json::json!([]),
            Some(4),
            Some(3)
        )
        .is_err(),
        "sequence gap detection"
    );
    let e = b.export(&i, "session", "raw", Some(500_000_000), 500_000_000)?;
    let mut deep = book::Book::default();
    let levels: Vec<_> = (80..=100)
        .rev()
        .map(|n| serde_json::json!([n.to_string(), "1"]))
        .collect();
    deep.snapshot(
        &serde_json::json!(levels),
        &serde_json::json!([["101", "1"]]),
        Some(1),
    )?;
    deep.delta(
        &serde_json::json!([["100", "0"]]),
        &serde_json::json!([]),
        Some(2),
        Some(1),
    )?;
    let exported = deep.export(&i, "s", "r", Some(0), 0)?;
    ensure!(
        matches!(exported.fields.get("bids"),Some(Cell::Levels(b)) if b.len()==20&&b.last().unwrap().price==Decimal(80*SCALE)),
        "deeper subscribed levels retained after top-level removal"
    );
    let mut a = analysis::Analysis::new(std::slice::from_ref(&i), 0, 15);
    let mut status = Event::new("collection_status", &i, "s", "status", Some(0), 0);
    status.s("state", "connected");
    a.ingest(&status);
    a.ingest(&e);
    let derived = a.advance(3 * SECOND)?;
    ensure!(
        derived.len() == 3
            && derived
                .iter()
                .all(|e| e.decimal("mid_price") == Some("100".parse().unwrap())),
        "catch-up boundaries preserve prior state"
    );
    let mut future = e.clone();
    future.s("event_id", "future");
    future.n("received_at_ns", 4 * SECOND);
    ensure!(
        a.advance(4 * SECOND)?[0].decimal("mid_price") == Some("100".parse()?),
        "no lookahead before ingest"
    );
    a.ingest(&future);
    for dataset in [
        "book_state_20",
        "public_trade",
        "price_event",
        "funding_event",
        "oi_event",
        "liquidation_event",
        "instrument_metadata",
        "collection_status",
        "analysis_state_1s",
    ] {
        let row = if dataset == "book_state_20" {
            e.clone()
        } else {
            Event::new(dataset, &i, "s", "r", Some(0), 0)
        };
        let batch = schema::batch(dataset, &[row])?;
        ensure!(batch.num_rows() == 1, "Arrow schema failed");
    }
    let mut c = Config::load(config)?;
    let root = std::env::temp_dir().join(format!("quant-verify-{}", uuid::Uuid::new_v4()));
    c.spool_dir = root.clone();
    verify_exchanges(&i, &c)?;
    let env = Envelope {
        raw: None,
        events: vec![e],
    };
    {
        let mut j = journal::Journal::new(&c)?;
        ensure!(
            journal::Journal::new(&c).is_err(),
            "exclusive collector lock"
        );
        j.append(&env)?;
        j.seal()?;
    }
    let file = std::fs::read_dir(root.join("journal"))?
        .next()
        .unwrap()?
        .path();
    ensure!(journal::read(&file, false)?.len() == 1, "WAL round trip");
    let mut corrupt = std::fs::read(&file)?;
    *corrupt.last_mut().unwrap() ^= 1;
    let bad = root.join("corrupt.wal.ready");
    std::fs::write(&bad, corrupt)?;
    ensure!(
        journal::read(&bad, false).is_err(),
        "WAL checksum corruption detection"
    );
    let torn = file.with_extension("open");
    std::fs::rename(&file, &torn)?;
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&torn)?;
        f.write_all(&[1, 2, 3])?;
    }
    {
        let j = journal::Journal::new(&c)?;
        drop(j);
    }
    ensure!(
        journal::read(&file, false)?.len() == 1,
        "torn final frame recovery"
    );
    let legacy = root.join("legacy.ready");
    let row = serde_json::json!({"raw":{"exchange":"bybit","session_id":"old","raw_id":"raw-old","received_at_ns":SECOND,"received_mono_ns":10,"source":"websocket","channel":"tickers.BTCUSDT","payload":"{}"},"events":[{"dataset":"price_event","event_id":"old-price","exchange":"bybit","instrument_id":"bybit:BTCUSDT","base_asset":"BTC","category":"앵커","quote_asset":"USDT","session_id":"old","raw_id":"raw-old","event_time_ns":SECOND,"received_at_ns":SECOND,"received_mono_ns":10,"schema_version":1,"quality_flags":[],"price_type":"mark","price":"100.000000000000000001"}]});
    std::fs::write(&legacy, format!("{row}\n"))?;
    journal::import(&c, &legacy)?;
    ensure!(legacy.exists(), "legacy import preserves source");
    let parquet = root.join("check.parquet");
    let batch = schema::batch("analysis_state_1s", &derived)?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&parquet)?,
        batch.schema(),
        None,
    )?;
    writer.write(&batch)?;
    writer.close()?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(parquet)?,
    )?
    .build()?;
    let count = reader
        .map(|b| b.map(|b| b.num_rows()))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .iter()
        .sum::<usize>();
    ensure!(count == 3, "Parquet read/write verification");
    std::fs::remove_dir_all(root)?;
    tracing::info!(
        "decimal, book sequencing, catch-up analysis, no-lookahead and all Arrow schemas verified"
    );
    Ok(())
}
fn verify_exchanges(i: &Instrument, c: &Config) -> Result<()> {
    use serde_json::json;
    for ex in ["bybit", "aster", "grvt", "hyperliquid"] {
        let mut i = i.clone();
        i.exchange = ex.into();
        let mut p = exchanges::Parser::new(std::slice::from_ref(&i), c);
        p.start = 0;
        let message = match ex {
            "bybit" => {
                json!({"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1000,"data":{"u":1,"b":[["100","2"]],"a":[["101","3"]]}})
            }
            "aster" => {
                p.seed(
                    &i.native_symbol,
                    &json!({"lastUpdateId":100,"bids":[["100","2"]],"asks":[["101","3"]]}),
                )?;
                json!({"e":"depthUpdate","s":"BTCUSDT","E":1000,"U":100,"u":101,"pu":99,"b":[["100","4"]],"a":[]})
            }
            "grvt" => {
                let v = json!({"result":{"stream":"v1.book.d","subs":["BTCUSDT@50"],"num_snapshots":[1],"latest_sequence_number":[10]}});
                let raw = p.raw(ex, "websocket", None, v.to_string(), SECOND);
                p.parse(&raw, &v)?;
                json!({"stream":"v1.book.d","sequence_number":0,"feed":{"instrument":"BTCUSDT","event_time":"1000000000","bids":[{"price":"100","size":"2"}],"asks":[{"price":"101","size":"3"}]}})
            }
            _ => {
                json!({"channel":"l2Book","data":{"coin":"BTCUSDT","time":1000,"levels":[[{"px":"100","sz":"2"}],[{"px":"101","sz":"3"}]]}})
            }
        };
        let raw = p.raw(ex, "websocket", None, message.to_string(), SECOND);
        let events = p.parse(&raw, &message)?;
        ensure!(
            events.len() == 1 && events[0].boolean("book_valid"),
            "{ex} book snapshot/bridge validation"
        );
        if ex == "grvt" {
            let v = json!({"stream":"v1.book.d","sequence_number":11,"prev_sequence_number":10,"feed":{"instrument":"BTCUSDT","event_time":"1000000001","bids":[{"price":"100","size":"4"}],"asks":[]}});
            ensure!(p.parse(&raw, &v)?.len() == 1, "GRVT snapshot predecessor");
        }
        let msg = match ex {
            "bybit" => {
                json!({"topic":"tickers.BTCUSDT","ts":1000,"data":{"markPrice":"100","indexPrice":"99","lastPrice":"100","openInterest":"42","fundingRate":"0.0001","nextFundingTime":"28800000"}})
            }
            "aster" => {
                json!({"e":"markPriceUpdate","s":"BTCUSDT","E":1000,"p":"100","i":"99","r":"0.0001","T":28800000})
            }
            "grvt" => {
                json!({"stream":"v1.ticker.s","feed":{"instrument":"BTCUSDT","event_time":"1000000000","mark_price":"100","index_price":"99","last_price":"100","open_interest":"42","funding_rate":"0.01","next_funding_time":"28800000000000"}})
            }
            _ => {
                json!({"channel":"activeAssetCtx","data":{"coin":"BTCUSDT","ctx":{"markPx":"100","oraclePx":"99","openInterest":"42","funding":"0.0001"}}})
            }
        };
        let raw = p.raw(ex, "websocket", None, msg.to_string(), SECOND);
        let events = p.parse(&raw, &msg)?;
        ensure!(
            events.iter().any(|e| e.dataset == "funding_event"
                && e.decimal("rate_decimal") == Some("0.0001".parse().unwrap())),
            "{ex} funding normalization"
        );
        if ex != "aster" {
            ensure!(
                events
                    .iter()
                    .any(|e| e.decimal("oi_base_qty") == Some("42".parse().unwrap())),
                "{ex} OI parsing"
            );
        }
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("TLS provider already initialized"))?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Cli::parse();
    if matches!(args.command, Command::Verify) {
        return verify(&args.config);
    }
    let c = Config::load(&args.config)?;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let duration = if let Command::Collect { duration } = &args.command {
        *duration
    } else {
        None
    };
    tokio::spawn(shutdown(tx.clone(), duration));
    match args.command {
        Command::Discover => {
            service::discovery(&c).await?;
        }
        Command::Collect { .. } => service::collect(c, rx, tx).await?,
        Command::Archive => storage::run(c, false, rx).await?,
        Command::Flush => storage::run(c, true, rx).await?,
        Command::ImportLegacy { input } => journal::import(&c, &input)?,
        Command::Replay { input, output } => replay(input, output, c.book_stale_seconds)?,
        Command::Inspect { input, raw_dump } => inspect(&input, raw_dump)?,
        Command::Verify => unreachable!(),
    }
    Ok(())
}
