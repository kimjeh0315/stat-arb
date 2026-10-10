use crate::model::*;
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
const MAGIC: &[u8; 8] = b"QWAL\0\0\0\x02";
const MAX_FRAME: usize = 128 * 1024 * 1024;
pub struct Lock(File);
impl Lock {
    pub fn acquire(root: &Path, name: &str) -> Result<Self> {
        fs::create_dir_all(root)?;
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(name))?;
        f.try_lock_exclusive()
            .with_context(|| format!("another process holds {name}"))?;
        Ok(Self(f))
    }
}
impl Drop for Lock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}
pub fn sync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}
pub fn atomic(p: &Path, bytes: &[u8]) -> Result<()> {
    fs::create_dir_all(p.parent().unwrap())?;
    let tmp = p.with_extension("tmp");
    let mut f = File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(tmp, p)?;
    sync_dir(p.parent().unwrap())
}
pub fn usage(root: &Path) -> u64 {
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}
pub fn read(path: &Path, repair: bool) -> Result<Vec<Envelope>> {
    let mut f = OpenOptions::new().read(true).write(repair).open(path)?;
    let size = f.metadata()?.len();
    let mut magic = [0; 8];
    f.read_exact(&mut magic)?;
    ensure!(
        &magic == MAGIC,
        "unsupported WAL version: {}",
        path.display()
    );
    let mut offset = 8;
    let mut out = Vec::new();
    loop {
        let mut header = [0; 8];
        if size - offset < 8 {
            if size > offset {
                ensure!(repair, "truncated WAL header");
                f.set_len(offset)?;
            }
            break;
        }
        f.read_exact(&mut header)?;
        let len = u32::from_le_bytes(header[..4].try_into()?) as usize;
        let checksum = u32::from_le_bytes(header[4..].try_into()?);
        ensure!(len <= MAX_FRAME, "invalid WAL frame length");
        if size - offset - 8 < len as u64 {
            ensure!(repair, "truncated WAL record");
            f.set_len(offset)?;
            break;
        }
        let mut bytes = vec![0; len];
        f.read_exact(&mut bytes)?;
        ensure!(
            crc32fast::hash(&bytes) == checksum,
            "WAL checksum mismatch at {offset}; preserve file for investigation"
        );
        out.push(bincode::deserialize(&bytes)?);
        offset += 8 + len as u64;
    }
    if repair {
        f.sync_all()?;
    }
    Ok(out)
}
pub struct Journal {
    c: Config,
    _lock: Lock,
    file: Option<BufWriter<File>>,
    path: PathBuf,
    size: u64,
    start: Instant,
    sync: Instant,
    usage_checked: Instant,
    cached_usage: u64,
}
impl Journal {
    pub fn new(c: &Config) -> Result<Self> {
        let lock = Lock::acquire(&c.spool_dir, ".collector.lock")?;
        let root = c.spool_dir.join("journal");
        fs::create_dir_all(&root)?;
        for entry in fs::read_dir(&root)? {
            let path = entry?.path();
            if path.to_string_lossy().ends_with(".wal.open") {
                if path.metadata()?.len() < 8 {
                    fs::remove_file(path)?;
                    continue;
                }
                read(&path, true)?;
                fs::rename(&path, path.with_extension("ready"))?;
                sync_dir(&root)?;
            }
        }
        Ok(Self {
            c: c.clone(),
            _lock: lock,
            file: None,
            path: PathBuf::new(),
            size: 0,
            start: Instant::now(),
            sync: Instant::now(),
            usage_checked: Instant::now() - Duration::from_secs(60),
            cached_usage: 0,
        })
    }
    pub fn append(&mut self, e: &Envelope) -> Result<()> {
        let bytes = bincode::serialize(e)?;
        ensure!(bytes.len() <= MAX_FRAME, "WAL frame too large");
        let len = bytes.len() as u64 + 8;
        if self.usage_checked.elapsed() > Duration::from_secs(10)
            || self.cached_usage + len > self.c.max_spool_bytes
        {
            self.cached_usage = usage(&self.c.spool_dir);
            self.usage_checked = Instant::now();
        }
        ensure!(
            self.cached_usage + len <= self.c.max_spool_bytes,
            "spool limit reached; preserving pending data and stopping collector"
        );
        if self.file.is_none() {
            self.path = self.c.spool_dir.join("journal").join(format!(
                "{:019}-{}.wal.open",
                now(),
                uuid::Uuid::new_v4().simple()
            ));
            let f = File::create(&self.path)?;
            let mut b = BufWriter::new(f);
            b.write_all(MAGIC)?;
            self.file = Some(b);
            self.size = 8;
            self.start = Instant::now();
        }
        let f = self.file.as_mut().unwrap();
        f.write_all(&(bytes.len() as u32).to_le_bytes())?;
        f.write_all(&crc32fast::hash(&bytes).to_le_bytes())?;
        f.write_all(&bytes)?;
        self.size += len;
        self.cached_usage += len;
        self.maintenance()?;
        Ok(())
    }
    pub fn maintenance(&mut self) -> Result<()> {
        if self.sync.elapsed().as_secs_f64() >= self.c.journal_fsync_seconds {
            if let Some(f) = self.file.as_mut() {
                f.flush()?;
                f.get_ref().sync_data()?;
            }
            self.sync = Instant::now();
        }
        if self.size >= self.c.journal_max_bytes
            || self.start.elapsed().as_secs() >= self.c.journal_rotate_seconds
        {
            self.seal()?;
        }
        Ok(())
    }
    pub fn seal(&mut self) -> Result<()> {
        if let Some(mut f) = self.file.take() {
            f.flush()?;
            f.get_ref().sync_all()?;
            drop(f);
            fs::rename(&self.path, self.path.with_extension("ready"))?;
            sync_dir(self.path.parent().unwrap())?;
            self.size = 0;
        }
        Ok(())
    }
}
impl Drop for Journal {
    fn drop(&mut self) {
        if let Err(e) = self.seal() {
            tracing::error!(error=%e,"WAL seal failed");
        }
    }
}
pub fn legacy(path: &Path) -> Result<Vec<Envelope>> {
    use std::io::BufRead;
    let mut out = Vec::new();
    for line in std::io::BufReader::new(File::open(path)?).lines() {
        let v: serde_json::Value = serde_json::from_str(&line?)?;
        let raw = if v["raw"].is_null() {
            None
        } else {
            Some(serde_json::from_value(v["raw"].clone())?)
        };
        let events = v["events"]
            .as_array()
            .context("legacy events array missing")?
            .iter()
            .map(legacy_event)
            .collect::<Result<_>>()?;
        out.push(Envelope { raw, events });
    }
    Ok(out)
}
pub fn import(c: &Config, input: &Path) -> Result<()> {
    let mut j = Journal::new(c)?;
    for e in legacy(input)? {
        j.append(&e)?;
    }
    j.seal()
}
