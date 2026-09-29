import argparse
import asyncio
import json
from pathlib import Path

from .adapters import ADAPTERS
from .collector import run_probe


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect normalized cross-venue BBO samples")
    parser.add_argument("--venues", nargs="+", choices=sorted(ADAPTERS), default=sorted(ADAPTERS))
    parser.add_argument("--duration", type=float, default=30.0, help="seconds to collect")
    parser.add_argument("--output", type=Path, default=Path("data/bbo.jsonl"))
    parser.add_argument("--symbol", default="BTC", help="base symbol (current prototype uses USDT perps)")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.duration <= 0:
        raise SystemExit("--duration must be positive")
    base = args.symbol.upper()
    symbols = {"bybit": f"{base}USDT", "hyperliquid": base,
               "aster": f"{base}USDT", "grvt": f"{base}_USDT_Perp"}
    adapters = [ADAPTERS[name](instrument=symbols[name]) for name in args.venues]
    summary = asyncio.run(run_probe(adapters, args.duration, args.output))
    print(json.dumps({"output": str(args.output), "venues": summary}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
