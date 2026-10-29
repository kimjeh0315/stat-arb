use super::{Args, scan::Audit};
use anyhow::{Context, Result, bail, ensure};
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};
use tokio::task::JoinSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Source {
    pub key: String,
    pub bytes: u64,
    pub etag: Option<String>,
    pub exchange: Option<String>,
    pub local_path: PathBuf,
}

fn environment(path: &Path) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    if path.exists() {
        for line in std::fs::read_to_string(path)?.lines() {
            let Some((name, value)) = line.trim().split_once('=') else {
                continue;
            };
            let name = name.trim();
            if ![
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_REGION",
                "QUANT_S3_BUCKET",
                "QUANT_S3_PREFIX",
            ]
            .contains(&name)
            {
                continue;
            }
            let value = value.trim();
            let value = if value.len() >= 2
                && ((value.starts_with('"') && value.ends_with('"'))
                    || (value.starts_with('\'') && value.ends_with('\'')))
            {
                &value[1..value.len() - 1]
            } else {
                value
            };
            values.insert(name.to_owned(), value.to_owned());
        }
    }
    for name in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "QUANT_S3_BUCKET",
        "QUANT_S3_PREFIX",
    ] {
        if let Ok(value) = std::env::var(name) {
            values.insert(name.to_owned(), value);
        }
    }
    Ok(values)
}

pub async fn collect(args: &Args, audit: &mut Audit) -> Result<Vec<Source>> {
    if let Some(input) = &args.input {
        ensure!(input.exists(), "local input does not exist");
        let mut files = if input.is_file() {
            vec![input.clone()]
        } else {
            let mut files = Vec::new();
            for entry in walkdir::WalkDir::new(input) {
                let entry = entry?;
                if entry.file_type().is_file()
                    && entry.path().extension().is_some_and(|s| s == "parquet")
                {
                    files.push(entry.path().to_owned());
                }
            }
            files
        };
        files.sort();
        if let Some(limit) = args.max_files {
            files.truncate(limit);
        }
        ensure!(!files.is_empty(), "no local Parquet files");
        let mut manifest = Vec::new();
        for path in files {
            let source = Source {
                key: path.display().to_string(),
                bytes: path.metadata()?.len(),
                etag: None,
                exchange: None,
                local_path: path,
            };
            audit.file(&source)?;
            manifest.push(source);
        }
        return Ok(manifest);
    }
    let env = environment(&args.env_file)?;
    let region = args
        .region
        .clone()
        .or_else(|| env.get("AWS_REGION").cloned())
        .unwrap_or_else(|| "ap-southeast-1".into());
    let bucket = args
        .bucket
        .clone()
        .or_else(|| env.get("QUANT_S3_BUCKET").cloned())
        .context("set QUANT_S3_BUCKET or --bucket")?;
    ensure!(!bucket.is_empty(), "empty S3 bucket");
    let prefix = args
        .prefix
        .clone()
        .or_else(|| env.get("QUANT_S3_PREFIX").cloned())
        .unwrap_or_else(|| "quant".into());
    let mut loader =
        aws_config::defaults(aws_config::BehaviorVersion::latest()).region(Region::new(region));
    match (
        env.get("AWS_ACCESS_KEY_ID"),
        env.get("AWS_SECRET_ACCESS_KEY"),
    ) {
        (Some(key), Some(secret)) if !key.is_empty() && !secret.is_empty() => {
            loader = loader.credentials_provider(Credentials::new(
                key,
                secret,
                env.get("AWS_SESSION_TOKEN")
                    .filter(|s| !s.is_empty())
                    .cloned(),
                None,
                "analysis-validator",
            ));
        }
        (None, None) => {}
        _ => bail!("AWS access key ID and secret must both be supplied"),
    }
    let client = Client::new(&loader.load().await);
    let exchanges = if args.exchanges.is_empty() {
        vec![
            "bybit".into(),
            "aster".into(),
            "grvt".into(),
            "hyperliquid".into(),
        ]
    } else {
        args.exchanges.clone()
    };
    let mut manifest = Vec::new();
    for exchange in exchanges {
        let rel = format!(
            "derived/dataset=analysis_state_1s/exchange={exchange}/date={}/",
            args.date
        );
        let root = prefix.trim_matches('/');
        let path = if root.is_empty() {
            rel
        } else {
            format!("{root}/{rel}")
        };
        let mut token = None;
        let before = manifest.len();
        loop {
            let page = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&path)
                .set_continuation_token(token.clone())
                .send()
                .await
                .with_context(|| {
                    format!("S3 list failed for {exchange}; check ListBucket permissions")
                })?;
            for object in page.contents() {
                let key = object.key().context("S3 object missing key")?;
                if !key.ends_with(".parquet") {
                    continue;
                }
                ensure!(
                    key.starts_with(&path),
                    "S3 object outside requested partition"
                );
                ensure!(
                    Path::new(key)
                        .components()
                        .all(|part| matches!(part, Component::Normal(_))),
                    "unsafe S3 object key"
                );
                let size = object.size().context("S3 object missing size")?;
                ensure!(size > 0, "empty Parquet object");
                manifest.push(Source {
                    key: key.into(),
                    bytes: size as u64,
                    etag: object.e_tag().map(str::to_owned),
                    exchange: Some(exchange.clone()),
                    local_path: args.cache_dir.join(key),
                });
            }
            if !page.is_truncated().unwrap_or(false) {
                break;
            }
            let next = page
                .next_continuation_token()
                .context("missing pagination token")?
                .to_owned();
            ensure!(token.as_deref() != Some(&next), "repeated pagination token");
            token = Some(next);
        }
        if manifest.len() == before {
            audit.missing_exchanges.push(exchange.clone());
        }
        eprintln!("Listed {exchange}: {} files", manifest.len() - before);
    }
    manifest.sort_by(|a, b| a.key.cmp(&b.key));
    if let Some(limit) = args.max_files {
        manifest.truncate(limit);
    }
    ensure!(
        !manifest.is_empty(),
        "no analysis files for requested UTC date"
    );
    let mut pending = manifest.iter().cloned();
    let mut tasks = JoinSet::new();
    let mut done = 0;
    loop {
        while tasks.len() < args.download_concurrency {
            let Some(source) = pending.next() else { break };
            let client = client.clone();
            let bucket = bucket.clone();

            tasks.spawn(async move {
                download(&client, &bucket, &source).await?;
                Ok::<_, anyhow::Error>(source)
            });
        }
        let Some(result) = tasks.join_next().await else {
            break;
        };
        let source = result??;
        audit.file(&source)?;
        done += 1;
        if done % 250 == 0 || done == manifest.len() {
            eprintln!("Read {done}/{} files; {} rows", manifest.len(), audit.rows);
        }
    }
    Ok(manifest)
}

async fn download(client: &Client, bucket: &str, source: &Source) -> Result<()> {
    // Cache reuse requires the exact remote ETag and byte count, not just a filename.
    let stamp = source.local_path.with_extension("parquet.source.json");
    if source.etag.is_some()
        && source
            .local_path
            .metadata()
            .is_ok_and(|m| m.len() == source.bytes)
        && let Ok(bytes) = std::fs::read(&stamp)
        && let Ok(old) = serde_json::from_slice::<Source>(&bytes)
        && old.key == source.key
        && old.bytes == source.bytes
        && old.etag == source.etag
    {
        return Ok(());
    }
    let parent = source.local_path.parent().context("invalid cache path")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = source
        .local_path
        .with_extension(format!("{}.part", uuid::Uuid::new_v4()));
    let result: Result<()> = async {
        let mut request = client.get_object().bucket(bucket).key(&source.key);
        if let Some(etag) = &source.etag {
            request = request.if_match(etag);
        }
        let response = request.send().await.context("S3 download failed")?;
        let mut reader = response.body.into_async_read();
        let mut file = tokio::fs::File::create(&temporary).await?;
        let size = tokio::io::copy(&mut reader, &mut file).await?;
        ensure!(size == source.bytes, "download size mismatch");
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temporary, &source.local_path).await?;
        tokio::fs::write(&stamp, serde_json::to_vec(source)?).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}
