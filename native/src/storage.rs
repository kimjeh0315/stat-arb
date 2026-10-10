use crate::{journal, model::*, schema};
use anyhow::{Context, Result, ensure};
use flate2::{Compression as Gzip, write::GzEncoder};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
#[derive(Serialize, Deserialize)]
struct Manifest {
    inputs: Vec<String>,
    outputs: Vec<String>,
}
fn date(n: i64) -> Result<String> {
    Ok(
        chrono::DateTime::from_timestamp(n / SECOND, ((n % SECOND + SECOND) % SECOND) as u32)
            .context("timestamp outside UTC range")?
            .format("%Y-%m-%d")
            .to_string(),
    )
}
fn finish(tmp: &Path, target: &Path) -> Result<()> {
    File::open(tmp)?.sync_all()?;
    fs::rename(tmp, target)?;
    journal::sync_dir(target.parent().unwrap())
}
fn convert(c: &Config, paths: &[PathBuf]) -> Result<()> {
    let pending = c.spool_dir.join("pending");
    let commits = c.spool_dir.join("commits");
    fs::create_dir_all(&commits)?;
    let names: Vec<String> = paths
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    let batch_id = id(&names.join(":"));
    let manifest = commits.join(format!("{batch_id}.json"));
    if manifest.exists() {
        for p in paths {
            if p.exists() {
                fs::remove_file(p)?;
            }
        }
        return Ok(());
    }
    let mut raws: BTreeMap<(String, String), Vec<Raw>> = BTreeMap::new();
    let mut groups: BTreeMap<(String, String, String), Vec<Event>> = BTreeMap::new();
    let mut seen = HashSet::new();
    for path in paths {
        let envelopes = if path.to_string_lossy().ends_with(".wal.ready") {
            journal::read(path, false)?
        } else {
            journal::legacy(path)?
        };
        for envelope in envelopes {
            if let Some(r) = envelope.raw {
                raws.entry((r.exchange.clone(), date(r.received_at_ns)?))
                    .or_default()
                    .push(r);
            }
            for e in envelope.events {
                if seen.insert(e.string("event_id").to_owned()) {
                    let t = e
                        .integer("event_time_ns")
                        .or_else(|| e.integer("received_at_ns"))
                        .context("event missing timestamp")?;
                    groups
                        .entry((e.dataset.clone(), e.string("exchange").into(), date(t)?))
                        .or_default()
                        .push(e);
                }
            }
        }
    }
    let mut outputs = Vec::new();
    for ((exchange, date), rows) in raws {
        let rel = format!("raw/exchange={exchange}/date={date}/{batch_id}.jsonl.gz");
        let target = pending.join(&rel);
        fs::create_dir_all(target.parent().unwrap())?;
        let tmp = target.with_extension("tmp");
        let mut writer = GzEncoder::new(File::create(&tmp)?, Gzip::fast());
        for row in rows {
            serde_json::to_writer(&mut writer, &row)?;
            writer.write_all(b"\n")?;
        }
        writer.finish()?.sync_all()?;
        finish(&tmp, &target)?;
        outputs.push(rel);
    }
    for ((dataset, exchange, date), mut rows) in groups {
        rows.sort_by(|a, b| {
            a.string("instrument_id")
                .cmp(b.string("instrument_id"))
                .then(
                    a.integer("received_at_ns")
                        .cmp(&b.integer("received_at_ns")),
                )
        });
        let layer = if dataset == "analysis_state_1s" {
            "derived"
        } else {
            "normalized"
        };
        let rel =
            format!("{layer}/dataset={dataset}/exchange={exchange}/date={date}/{batch_id}.parquet");
        let target = pending.join(&rel);
        fs::create_dir_all(target.parent().unwrap())?;
        let tmp = target.with_extension("tmp");
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
            .set_max_row_group_row_count(Some(10000))
            .build();
        let first = schema::batch(&dataset, &rows[..rows.len().min(10000)])?;
        let mut writer = ArrowWriter::try_new(File::create(&tmp)?, first.schema(), Some(props))?;
        writer.write(&first)?;
        for chunk in rows[first.num_rows()..].chunks(10000) {
            writer.write(&schema::batch(&dataset, chunk)?)?;
        }
        writer.close()?;
        finish(&tmp, &target)?;
        outputs.push(rel);
    }
    journal::atomic(
        &manifest,
        &serde_json::to_vec(&Manifest {
            inputs: names,
            outputs,
        })?,
    )?;
    for p in paths {
        fs::remove_file(p)?;
    }
    journal::sync_dir(&c.spool_dir.join("journal"))?;
    tracing::info!(segments=paths.len(),batch=%batch_id,"archive committed");
    Ok(())
}
fn recover_commits(c: &Config) -> Result<()> {
    let dir = c.spool_dir.join("commits");
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|x| x.to_str()) != Some("json") {
            continue;
        }
        let m: Manifest = serde_json::from_slice(&fs::read(path)?)?;
        for input in m.inputs {
            let p = c.spool_dir.join("journal").join(input);
            if p.exists() {
                fs::remove_file(p)?;
            }
        }
    }
    Ok(())
}
fn select(c: &Config, force: bool) -> Result<Vec<PathBuf>> {
    let root = c.spool_dir.join("journal");
    if !root.exists() {
        return Ok(vec![]);
    }
    let mut paths: Vec<_> = fs::read_dir(root)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("ready"))
        .collect();
    paths.sort();
    let mut bytes = 0;
    let mut chosen = Vec::new();
    for p in paths {
        bytes += p.metadata()?.len();
        chosen.push(p);
        if bytes >= c.archive_batch_bytes {
            break;
        }
    }
    let aged = chosen
        .first()
        .map(|p| {
            p.metadata().and_then(|m| m.modified()).map(|t| {
                SystemTime::now()
                    .duration_since(t)
                    .unwrap_or_default()
                    .as_secs()
                    >= c.archive_batch_seconds
            })
        })
        .transpose()?
        .unwrap_or(false);
    if !force && bytes < c.archive_batch_bytes && !aged {
        chosen.clear();
    }
    Ok(chosen)
}
fn checksum(path: &Path) -> Result<String> {
    let mut hash = Sha256::new();
    let mut file = File::open(path)?;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
async fn upload(c: &Config, client: &aws_sdk_s3::Client) -> Result<()> {
    let root = c.spool_dir.join("pending");
    let mut files: Vec<_> = walkdir::WalkDir::new(&root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file()
                && matches!(
                    e.path().extension().and_then(|x| x.to_str()),
                    Some("gz" | "parquet")
                )
        })
        .map(|e| e.path().to_owned())
        .collect();
    files.sort();
    for path in files {
        let key = format!(
            "{}/{}",
            c.s3_prefix.trim_matches('/'),
            path.strip_prefix(&root)?.to_string_lossy()
        )
        .trim_start_matches('/')
        .to_string();
        let size = path.metadata()?.len();
        ensure!(
            size < 5 * 1024 * 1024 * 1024,
            "object too large for PutObject"
        );
        let p = path.clone();
        let digest = tokio::task::spawn_blocking(move || checksum(&p)).await??;
        client
            .put_object()
            .bucket(&c.s3_bucket)
            .key(&key)
            .metadata("sha256", &digest)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
            .body(aws_sdk_s3::primitives::ByteStream::from_path(&path).await?)
            .send()
            .await
            .with_context(|| format!("S3 PutObject failed: {key}"))?;
        let head = client
            .head_object()
            .bucket(&c.s3_bucket)
            .key(&key)
            .send()
            .await?;
        ensure!(
            head.content_length() == Some(size as i64)
                && head.metadata().and_then(|m| m.get("sha256")) == Some(&digest),
            "S3 verification failed: {key}"
        );
        fs::remove_file(&path)?;
        journal::sync_dir(path.parent().unwrap())?;
        tracing::info!(bytes=size,key=%key,"S3 upload verified");
    }
    Ok(())
}
pub async fn run(
    c: Config,
    once: bool,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let _lock = journal::Lock::acquire(&c.spool_dir, ".archiver.lock")?;
    // The archiver is the only pending-file writer/uploader. Collectors only append WAL.
    recover_commits(&c)?;
    let client = if c.s3_bucket.is_empty() {
        None
    } else {
        let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(c.aws_region.clone()))
            .load()
            .await;
        Some(aws_sdk_s3::Client::new(&sdk))
    };
    loop {
        let paths = select(&c, once || *stop.borrow())?;
        if !paths.is_empty() {
            let copy = c.clone();
            tokio::task::spawn_blocking(move || convert(&copy, &paths)).await??;
        }
        if let Some(client) = &client
            && let Err(e) = upload(&c, client).await
        {
            tracing::warn!(error=%format!("{e:#}"),"S3 upload deferred; local files retained");
            if once {
                return Err(e);
            }
        }
        if once {
            if select(&c, true)?.is_empty() {
                break;
            }
            continue;
        }
        if *stop.borrow() {
            break;
        }
        tokio::select! {_=stop.changed()=>{},_=tokio::time::sleep(Duration::from_secs(2))=>{}}
    }
    Ok(())
}
