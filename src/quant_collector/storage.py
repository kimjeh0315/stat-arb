import asyncio
import fcntl
import gzip
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from .schema import table

LOG = logging.getLogger(__name__)


def utc_date(ns):
    return datetime.fromtimestamp(ns / 10 ** 9, tz=timezone.utc).strftime("%Y-%m-%d")


def atomic_bytes(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    os.replace(tmp, path)


class SpoolFull(RuntimeError):
    pass


class Journal:
    """Bounded disk WAL. Recovered segments always contain complete JSON lines."""

    def __init__(self, config):
        self.config = config
        self.root = config.spool_dir
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / ".collector.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise RuntimeError("Another collector is using this spool directory")
        self.wal = self.root / "journal"
        self.wal.mkdir(exist_ok=True)
        self.pending = self.root / "pending"
        self.pending.mkdir(exist_ok=True)
        self.file = None
        self.current_path = None
        self.size = 0
        self.last_sync = 0.0
        self.started = 0.0
        self.cached_usage = 0
        self.usage_checked_at = 0.0
        self.recover()

    def recover(self):
        for path in self.wal.glob("*.open"):
            with path.open("r+b") as handle:
                content = handle.read()
                length = content.rfind(b"\n") + 1
                handle.truncate(length)
                handle.flush()
                os.fsync(handle.fileno())
            if length:
                os.replace(path, path.with_suffix(".ready"))
            else:
                path.unlink()

    def disk_usage(self):
        total = 0
        for path in self.root.rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                pass  # uploader removed a completed object
        return total

    def append_many(self, envelopes):
        payload = b"".join((json.dumps(e, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
                           for e in envelopes)
        now = time.monotonic()
        if now - self.usage_checked_at >= 1 or self.cached_usage + len(payload) > self.config.max_spool_bytes:
            self.cached_usage = self.disk_usage()
            self.usage_checked_at = now
        if self.cached_usage + len(payload) > self.config.max_spool_bytes:
            raise SpoolFull("Spool capacity reached; stopping without discarding pending data")
        if self.file is None:
            self.current_path = self.wal / (uuid4().hex + ".open")
            self.file = self.current_path.open("wb")
            self.size = 0
            self.started = time.monotonic()
        self.file.write(payload)
        self.file.flush()
        self.size += len(payload)
        self.cached_usage += len(payload)
        if now - self.last_sync >= self.config.journal_fsync_seconds:
            os.fsync(self.file.fileno())
            self.last_sync = now
        if self.size >= self.config.journal_max_bytes or now - self.started >= self.config.journal_rotate_seconds:
            self.seal()

    def seal(self):
        if self.file is None:
            return
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()
        os.replace(self.current_path, self.current_path.with_suffix(".ready"))
        self.file = None

    def close(self):
        self.seal()
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()


def convert_segment(path, pending):
    raw_groups, event_groups = defaultdict(list), defaultdict(list)
    with path.open() as source:
        for line in source:
            envelope = json.loads(line)
            raw = envelope.get("raw")
            if raw:
                raw_groups[(raw["exchange"], utc_date(raw["received_at_ns"]))].append(raw)
            for row in envelope.get("events", []):
                stamp = row.get("event_time_ns") or row["received_at_ns"]
                event_groups[(row["dataset"], row["exchange"], utc_date(stamp))].append(row)
    for (exchange, date), rows in raw_groups.items():
        target = pending / "raw" / ("exchange=" + exchange) / ("date=" + date) / (path.stem + ".jsonl.gz")
        content = b"".join((json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
                           for r in rows)
        atomic_bytes(target, gzip.compress(content, mtime=0))
    for (dataset, exchange, date), rows in event_groups.items():
        layer = "derived" if dataset == "analysis_state_1s" else "normalized"
        target = pending / layer / ("dataset=" + dataset) / ("exchange=" + exchange) / ("date=" + date) / (path.stem + ".parquet")
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".parquet.tmp")
        # Idempotent native event IDs deduplicate overlaps within this segment.
        rows = list({row["event_id"]: row for row in rows}.values())
        rows.sort(key=lambda row: (row["instrument_id"], row["received_at_ns"]))
        pq.write_table(table(dataset, rows), tmp, compression="zstd", row_group_size=10000)
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    # Delete WAL only after every export has been atomically completed.
    path.unlink()


class S3Uploader:
    def __init__(self, config, client=None):
        self.config = config
        self.client = client

    def upload(self, path):
        if self.client is None:
            import boto3
            from botocore.config import Config
            self.client = boto3.client("s3", region_name=self.config.aws_region,
                                       config=Config(retries={"max_attempts": 5, "mode": "standard"},
                                                     connect_timeout=10, read_timeout=60))
        key = "/".join(filter(None, [self.config.s3_prefix.strip("/"),
                                    path.relative_to(self.config.spool_dir / "pending").as_posix()]))
        with path.open("rb") as source:
            checksum = hashlib.sha256(source.read()).hexdigest()
        size = path.stat().st_size
        from boto3.s3.transfer import TransferConfig
        self.client.upload_file(str(path), self.config.s3_bucket, key,
                                ExtraArgs={"Metadata": {"sha256": checksum},
                                           "ChecksumAlgorithm": "SHA256"},
                                Config=TransferConfig(max_concurrency=2,
                                                      multipart_threshold=16 * 1024 * 1024))
        head = self.client.head_object(Bucket=self.config.s3_bucket, Key=key)
        if head["ContentLength"] != size or head.get("Metadata", {}).get("sha256") != checksum:
            raise IOError("S3 upload verification failed")
        path.unlink()


async def export_loop(journal, stop):
    while not stop.is_set():
        for path in sorted(journal.wal.glob("*.ready")):
            await asyncio.to_thread(convert_segment, path, journal.pending)
        try:
            await asyncio.wait_for(stop.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass


async def upload_loop(config, stop):
    uploader = S3Uploader(config)
    while not stop.is_set():
        if config.s3_bucket:
            for path in sorted((config.spool_dir / "pending").rglob("*")):
                if path.is_file() and path.suffix in (".gz", ".parquet"):
                    try:
                        await asyncio.to_thread(uploader.upload, path)
                    except Exception as exc:
                        LOG.warning("S3 upload deferred: %s", type(exc).__name__)
                        break
        try:
            await asyncio.wait_for(stop.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
