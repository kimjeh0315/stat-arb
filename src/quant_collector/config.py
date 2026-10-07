import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class Config:
    exchanges: List[str]
    quote_assets: List[str]
    universe_path: Path
    spool_dir: Path
    s3_bucket: str = ""
    s3_prefix: str = "quant"
    aws_region: str = "ap-southeast-1"
    book_depth: int = 20
    analysis_interval_seconds: int = 1
    book_stale_seconds: int = 15
    journal_rotate_seconds: int = 30
    journal_max_bytes: int = 16 * 1024 * 1024
    journal_fsync_seconds: float = 1
    max_spool_bytes: int = 10 * 1024 ** 3
    queue_capacity: int = 20000
    symbols_per_connection: int = 15
    aster_oi_poll_seconds: int = 60
    funding_poll_seconds: int = 300
    funding_lookback_hours: int = 24
    http_requests_per_second: float = 3
    grvt_live_funding_unit: str = "percentage_points"
    aliases: Dict[str, Any] = field(default_factory=dict)
    external_liquidation_sources: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def load(cls, path: str) -> "Config":
        source = Path(path).resolve()
        data = json.loads(source.read_text())
        for name in ("universe_path", "spool_dir"):
            data[name] = (source.parent / data[name]).resolve()
        for env, name in (("QUANT_S3_BUCKET", "s3_bucket"),
                          ("QUANT_S3_PREFIX", "s3_prefix"),
                          ("AWS_REGION", "aws_region")):
            if env in os.environ:
                data[name] = os.environ[env]
        if "QUANT_SPOOL_DIR" in os.environ:
            data["spool_dir"] = Path(os.environ["QUANT_SPOOL_DIR"]).resolve()
        cfg = cls(**data)
        allowed = {"bybit", "aster", "grvt", "hyperliquid"}
        if not cfg.exchanges or not set(cfg.exchanges) <= allowed:
            raise ValueError("Unknown or empty exchange list")
        if cfg.book_depth != 20 or cfg.analysis_interval_seconds != 1:
            raise ValueError("This schema requires depth=20 and analysis interval=1 second")
        for name in ("queue_capacity", "symbols_per_connection", "journal_rotate_seconds",
                     "journal_max_bytes", "max_spool_bytes", "book_stale_seconds",
                     "aster_oi_poll_seconds", "funding_poll_seconds", "funding_lookback_hours",
                     "http_requests_per_second"):
            if getattr(cfg, name) <= 0:
                raise ValueError("%s must be positive" % name)
        if cfg.symbols_per_connection > 20:
            raise ValueError("Use at most 20 symbols per connection to limit stream counts")
        if cfg.journal_fsync_seconds < 0:
            raise ValueError("journal_fsync_seconds must be nonnegative")
        if not cfg.quote_assets or not set(cfg.quote_assets) <= {"USDT", "USDC"}:
            raise ValueError("Only linear USDT/USDC contracts are supported")
        if cfg.grvt_live_funding_unit not in ("percentage_points", "centibeeps"):
            raise ValueError("Unsupported GRVT live funding unit")
        external_exchanges = set()
        for source in cfg.external_liquidation_sources:
            if source.get("exchange") not in cfg.exchanges:
                raise ValueError("External source exchange must be enabled")
            if source["exchange"] in external_exchanges:
                raise ValueError("Only one external liquidation source per exchange is supported")
            if source["exchange"] not in ("grvt", "hyperliquid"):
                raise ValueError("External sources are only used where native public liquidation is unavailable")
            external_exchanges.add(source["exchange"])
            if not source.get("url_env") or not source.get("coverage_type", "").startswith("external"):
                raise ValueError("External sources require url_env and an external_* coverage_type")
        return cfg
