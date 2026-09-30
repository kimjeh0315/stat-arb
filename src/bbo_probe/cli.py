import argparse
import asyncio
import json
import sys
from pathlib import Path

from .collector import run_probe
from .exchanges import ADAPTERS, BatchAdapter
from .markets import TOP_MARKETS, VENUES, UnsupportedMarket, select_market_symbols


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect normalized cross-venue BBO samples")
    parser.add_argument("--list-markets", action="store_true",
                        help="print the canonical market catalog and exit")
    parser.add_argument("--venues", nargs="+", choices=VENUES, default=list(VENUES))
    parser.add_argument("--duration", type=float, default=30.0, help="seconds to collect")
    parser.add_argument("--output", type=Path, default=Path("data/bbo.jsonl"))
    market_group = parser.add_mutually_exclusive_group()
    market_group.add_argument("--symbols", nargs="+", default=["BTC"], metavar="ASSET",
                              help="canonical assets, for example BTC ETH SOL")
    market_group.add_argument("--top", type=int, choices=range(1, 51), metavar="N",
                              help="collect the first N assets from the documented turnover universe")
    return parser


def build_adapters(venues, assets, *, skip_unsupported=False):
    selected, missing = select_market_symbols(
        venues, assets, skip_unsupported=skip_unsupported
    )
    adapters = []
    for venue, symbols in selected.items():
        children = tuple(ADAPTERS[venue](instrument=symbol) for symbol in symbols)
        if children:
            adapters.append(children[0] if len(children) == 1 else BatchAdapter(children=children))
    return adapters, missing


def main() -> None:
    args = build_parser().parse_args()
    if args.list_markets:
        print(json.dumps([
            {"rank": market.rank, "asset": market.asset, "symbols": market.symbols}
            for market in TOP_MARKETS
        ], indent=2))
        return
    if args.duration <= 0:
        raise SystemExit("--duration must be positive")
    assets = [market.asset for market in TOP_MARKETS[:args.top]] if args.top else args.symbols
    try:
        adapters, missing = build_adapters(
            args.venues, assets, skip_unsupported=args.top is not None
        )
    except UnsupportedMarket as exc:
        raise SystemExit(str(exc)) from exc
    if missing:
        print("skipping unsupported markets: " + ", ".join(missing), file=sys.stderr)
    summary = asyncio.run(run_probe(adapters, args.duration, args.output))
    print(json.dumps({"output": str(args.output), "assets": assets,
                      "venues": summary, "skipped": missing}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
