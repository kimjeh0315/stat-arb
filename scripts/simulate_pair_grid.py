"""Backtest one pair-trading rule across an asset-by-venue-pair grid."""

import argparse
import json
from pathlib import Path

from bbo_probe.markets import MARKETS, TOP_MARKETS, VENUES
from bbo_probe.strategy.grid import build_grid_report
from bbo_probe.strategy.loader import load_pair_hypothesis
from bbo_probe.strategy.pair_rules import PairTradingParameters


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest cross-venue pair trading across many assets and venue pairs"
    )
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument(
        "--hypothesis", type=Path,
        default=Path("hypotheses/cross_venue_pair_grid.py"),
    )
    parser.add_argument("--venues", nargs="+", choices=VENUES, required=True)
    assets = parser.add_mutually_exclusive_group(required=True)
    assets.add_argument("--symbols", nargs="+", choices=tuple(MARKETS))
    assets.add_argument("--top", type=int, choices=range(1, len(TOP_MARKETS) + 1))
    parser.add_argument("--output", type=Path, default=Path("data/pair-grid.json"))
    parser.add_argument("--window", type=int)
    parser.add_argument("--freshness-ms", type=float, default=1_000)
    parser.add_argument("--fee-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--warmup-samples", type=int)
    parser.add_argument("--entry-z", type=float)
    parser.add_argument("--min-deviation-bps", type=float)
    parser.add_argument("--exit-z", type=float)
    parser.add_argument("--stop-z", type=float)
    parser.add_argument("--max-hold-seconds", type=float)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    assets = (
        args.symbols
        if args.symbols is not None
        else [market.asset for market in TOP_MARKETS[:args.top]]
    )
    try:
        hypothesis = load_pair_hypothesis(args.hypothesis)
        base = hypothesis.specification.parameters
        window = args.window if args.window is not None else hypothesis.specification.window
        parameters = PairTradingParameters(
            warmup_samples=args.warmup_samples
            if args.warmup_samples is not None else base.warmup_samples,
            entry_z=args.entry_z if args.entry_z is not None else base.entry_z,
            min_deviation_bps=args.min_deviation_bps
            if args.min_deviation_bps is not None else base.min_deviation_bps,
            exit_z=args.exit_z if args.exit_z is not None else base.exit_z,
            stop_z=args.stop_z if args.stop_z is not None else base.stop_z,
            max_hold_seconds=args.max_hold_seconds
            if args.max_hold_seconds is not None else base.max_hold_seconds,
        )
        hypothesis = hypothesis.with_parameters(parameters)
        report = build_grid_report(
            args.data, args.venues, assets, hypothesis,
            window=window, freshness_ms=args.freshness_ms,
            fee_bps=args.fee_bps, min_trades=args.min_trades,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(args.output),
        "combinations": len(report["pairs"]),
        "ranked": len(report["ranking"]),
        "summary": report["summary"],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
