import asyncio
import json
import logging
import signal
import time

import aiohttp

from .adapters import ADAPTERS
from .analysis import Analyzer
from .domain import Universe
from .external import ExternalLiquidations
from .pipeline import Pipeline
from .storage import Journal, S3Uploader, atomic_bytes, convert_segment, export_loop, upload_loop
from .transport import PublicHTTP

LOG = logging.getLogger(__name__)


async def discover(config, session):
    universe = Universe(json.loads(config.universe_path.read_text()), config.aliases)
    adapters = [ADAPTERS[name](config, universe, PublicHTTP(session, config.http_requests_per_second))
                for name in config.exchanges]
    results = await asyncio.gather(*(a.discover() for a in adapters), return_exceptions=True)
    report, failed = {}, False
    external = {s["exchange"]: s["coverage_type"] for s in config.external_liquidation_sources}
    for adapter, result in zip(adapters, results):
        if isinstance(result, Exception):
            report[adapter.name] = {"status": "error", "error_type": type(result).__name__}
            failed = True
            continue
        adapter.configure(result)
        found = {i.base_asset for i in result}
        report[adapter.name] = dict(
            status="ok", instruments=[i.row() for i in result],
            missing_assets=sorted(set(universe.assets) - found),
            liquidation_coverage=external.get(adapter.name, adapter.liquidation_coverage),
            oi_source="rest_poll" if adapter.name == "aster" else "websocket",
        )
    config.spool_dir.mkdir(parents=True, exist_ok=True)
    atomic_bytes(config.spool_dir / "reports" / "discovery.json",
                 json.dumps(report, ensure_ascii=False, indent=2).encode())
    if failed:
        raise RuntimeError("Discovery failed; see spool/reports/discovery.json")
    return adapters, report


async def run(config, duration=None):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    journal = Journal(config)
    producers, background = [], []
    consumer = None
    fatal = None
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),
                                         connector=aiohttp.TCPConnector(limit=80)) as session:
            adapters, report = await discover(config, session)
            instruments = [i for a in adapters for i in a.instruments.values()]
            if not instruments:
                raise RuntimeError("No listed instruments match the configured universe")
            coverage = {a.name: report[a.name]["liquidation_coverage"] for a in adapters}
            analyzer = Analyzer(instruments, coverage, config.book_stale_seconds)
            pipeline = Pipeline(journal, analyzer, instruments, config.queue_capacity)
            consumer = asyncio.create_task(pipeline.consume(), name="journal-writer")
            background = [asyncio.create_task(export_loop(journal, stop), name="parquet-exporter"),
                          asyncio.create_task(upload_loop(config, stop), name="s3-uploader")]
            await pipeline.metadata(adapters, report)
            producers.append(asyncio.create_task(pipeline.ticks(stop), name="analysis-ticks"))
            for adapter in adapters:
                if not adapter.instruments:
                    continue
                LOG.info("%s: collecting %d instruments; liquidation=%s", adapter.name,
                         len(adapter.instruments), coverage[adapter.name])
                producers += [asyncio.create_task(adapter.run(pipeline, stop), name=adapter.name),
                              asyncio.create_task(adapter.funding_loop(pipeline, stop),
                                                  name=adapter.name + "-funding")]
                if adapter.name == "aster":
                    producers.append(asyncio.create_task(adapter.oi_loop(pipeline, stop), name="aster-oi"))
            for spec in config.external_liquidation_sources:
                bridge = ExternalLiquidations(spec, instruments)
                producers.append(asyncio.create_task(bridge.run(session, pipeline, stop),
                                                     name=bridge.name + "-external-liquidations"))
            stopper = asyncio.create_task(stop.wait(), name="stop-signal")
            timer = asyncio.create_task(asyncio.sleep(duration)) if duration else None
            watched = [consumer] + producers + background + [stopper] + ([timer] if timer else [])
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stopper and task is not timer:
                    fatal = task.exception() or RuntimeError("Worker exited unexpectedly: " + task.get_name())
            stop.set()
            pipeline.accepting = False
            for task in producers + [stopper] + ([timer] if timer else []):
                task.cancel()
            await asyncio.gather(*producers, stopper, *([timer] if timer else []), return_exceptions=True)
            if consumer.done():
                fatal = consumer.exception() or fatal
            else:
                drain = asyncio.create_task(pipeline.close())
                finished, _ = await asyncio.wait([drain, consumer], return_when=asyncio.FIRST_COMPLETED)
                if consumer in finished and consumer.exception():
                    fatal = consumer.exception()
                    drain.cancel()
                    await asyncio.gather(drain, return_exceptions=True)
                else:
                    await drain
                    await consumer
            background_results = await asyncio.gather(*background, return_exceptions=True)
            for result in background_results:
                if isinstance(result, Exception):
                    fatal = result
    finally:
        stop.set()
        # Discover/setup failures must not leave producer tasks alive.
        for task in producers:
            if not task.done():
                task.cancel()
        if producers:
            await asyncio.gather(*producers, return_exceptions=True)
        if consumer and not consumer.done():
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        if background:
            await asyncio.gather(*background, return_exceptions=True)
        try:
            journal.seal()
            if fatal is None:
                for path in sorted(journal.wal.glob("*.ready")):
                    await asyncio.to_thread(convert_segment, path, journal.pending)
        finally:
            journal.close()
    if fatal:
        raise fatal


async def flush(config):
    journal = Journal(config)
    try:
        for path in sorted(journal.wal.glob("*.ready")):
            await asyncio.to_thread(convert_segment, path, journal.pending)
        if config.s3_bucket:
            uploader = S3Uploader(config)
            for path in sorted(journal.pending.rglob("*")):
                if path.is_file() and path.suffix in (".gz", ".parquet"):
                    await asyncio.to_thread(uploader.upload, path)
    finally:
        journal.close()
