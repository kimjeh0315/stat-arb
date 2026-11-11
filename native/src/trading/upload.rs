//! Independent, retryable S3 snapshots. Never deletes source logs.
use anyhow::{Result,ensure};
use std::{path::Path,io::{Read,Write}};
use sha2::{Digest,Sha256};

fn complete_lines(bytes:&[u8])->&[u8] {
    &bytes[..bytes.iter().rposition(|b|*b==b'\n').map_or(0,|p|p+1)]
}
pub async fn run(dir:&Path,collector:&Path)->Result<()> {
    let c=crate::model::Config::load(collector)?;
    ensure!(!c.s3_bucket.is_empty(),"S3 bucket is required");
    let prefix=std::env::var("QUANT_SHADOW_S3_PREFIX").unwrap_or_else(|_|"quant/shadow-v1".into());
    ensure!(!prefix.trim_matches('/').is_empty(),"shadow prefix is required");
    std::fs::create_dir_all(dir)?;
    let sdk=aws_config::defaults(aws_config::BehaviorVersion::latest()).region(aws_config::Region::new(c.aws_region)).load().await;
    let client=aws_sdk_s3::Client::new(&sdk);
    let mut failed=0;
    for entry in std::fs::read_dir(dir)? {
        let path=entry?.path();
        if path.extension().and_then(|x|x.to_str())!=Some("jsonl"){continue;}
        let name=path.file_name().unwrap().to_string_lossy();
        // Replay outputs are deliberately excluded from production session datasets.
        if name.starts_with("replay-"){continue;}
        let date=name.get(..10).unwrap_or("");
        if chrono::NaiveDate::parse_from_str(date,"%Y-%m-%d").is_err(){continue;}
        let mut bytes=Vec::new();
        std::fs::File::open(&path)?.take(64*1024*1024+1).read_to_end(&mut bytes)?;
        ensure!(bytes.len()<=64*1024*1024,"log exceeds 64 MiB snapshot bound: {}",name);
        let bytes=complete_lines(&bytes);if bytes.is_empty(){continue;}
        let hash=format!("{:x}",Sha256::digest(bytes));
        let marker=path.with_extension("uploaded-sha256");
        if std::fs::read_to_string(&marker).ok().as_deref()==Some(hash.as_str()){continue;}
        let mut gzip=flate2::write::GzEncoder::new(Vec::new(),flate2::Compression::fast());
        gzip.write_all(bytes)?;let compressed=gzip.finish()?;
        let key=format!("{}/mode=shadow/date={}/{}.gz",prefix.trim_matches('/'),date,name);
        match client.put_object().bucket(&c.s3_bucket).key(&key)
            .content_type("application/gzip").metadata("uncompressed-sha256",&hash)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
            .body(aws_sdk_s3::primitives::ByteStream::from(compressed)).send().await {
            Ok(_)=>{
                let tmp=marker.with_extension("tmp");
                std::fs::write(&tmp,&hash)?;std::fs::rename(tmp,marker)?;
                tracing::info!(bucket=%c.s3_bucket,key=%key,bytes=bytes.len(),"shadow log uploaded");
            }
            Err(e)=>{failed+=1;tracing::error!(key=%key,error=%e,"shadow upload failed; local log retained for retry");}
        }
    }
    ensure!(failed==0,"{failed} shadow uploads failed; next timer retries");Ok(())
}
#[cfg(test)] mod tests {
    #[test] fn ignores_partial_last_record(){assert_eq!(super::complete_lines(b"{}\n{\"x\":"),b"{}\n");assert_eq!(super::complete_lines(b"partial"),b"");assert_eq!(super::complete_lines(b"{}\n"),b"{}\n");}
}
