import argparse
import asyncio
import logging
import sys

import aiohttp

from .config import Config
from .service import discover, flush, run


def main():
    parser = argparse.ArgumentParser(description="Public market data collector (no trading)")
    parser.add_argument("--config", default="config/collector.json")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("discover", help="Resolve listed instruments and write a coverage report")
    collect = sub.add_parser("run", help="Collect, journal, export and optionally upload to S3")
    collect.add_argument("--duration", type=float, help="Optional duration in seconds for a smoke test")
    sub.add_parser("flush", help="Recover/export pending data and retry S3 uploads")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        config = Config.load(args.config)
        if args.command == "run":
            if args.duration is not None and args.duration <= 0:
                parser.error("duration must be positive")
            asyncio.run(run(config, args.duration))
        elif args.command == "flush":
            asyncio.run(flush(config))
        else:
            async def perform():
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                    adapters, report = await discover(config, session)
                    for adapter in adapters:
                        print("%s: %d instruments, %d missing assets, liquidation=%s" % (
                            adapter.name, len(adapter.instruments), len(report[adapter.name]["missing_assets"]),
                            report[adapter.name]["liquidation_coverage"]))
                    print("Report: " + str(config.spool_dir / "reports" / "discovery.json"))
            asyncio.run(perform())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logging.error("Collector stopped: %s: %s", type(exc).__name__, str(exc))
        return 1
    return 0
