"""Backtest one pair-trading rule across an asset-by-venue-pair grid."""

import argparse
import json
from dataclasses import asdict
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, Sequence

from bbo_probe.markets import MARKETS, VENUES
from bbo_probe.strategy.engine import run_backtest
from bbo_probe.strategy.pair_rules import PairTradingParameters, decide_pair


def build_report(
    data_path: Path,
    venues: Sequence[str],
    assets: Sequence[str],
    parameters: PairTradingParameters,
    *,
    window: int,
    freshness_ms: float,
    fee_bps: float,
    min_trades: int,
) -> Dict[str, Any]:
    if len(set(venues)) != len(venues) or len(set(assets)) != len(assets):
        raise ValueError("venues and symbols must not contain duplicates")
    if window < parameters.warmup_samples:
        raise ValueError("window must be at least as large as warmup_samples")
    if min_trades < 1:
        raise ValueError("min_trades must be positive")

    def decide(frame):
        return decide_pair(frame, parameters)

    result = run_backtest(
        data_path, venues, assets, decide,
        window=window, freshness_ms=freshness_ms, fee_bps=fee_bps,
    )
    observed = result["pairs"]
    completed: Dict[str, Dict[str, Any]] = {}
    empty = {
        "samples": 0, "trades": 0, "win_rate": 0.0, "gross_bps": 0.0,
        "net_bps": 0.0, "average_net_bps": 0.0, "max_drawdown_bps": 0.0,
    }
    for asset in assets:
        for left, right in combinations(venues, 2):
            name = f"{asset.upper()}:{left}-{right}"
            metrics = dict(observed.get(name, empty))
            if metrics["samples"] == 0:
                status = "no_aligned_data"
            elif metrics["samples"] < parameters.warmup_samples:
                status = "insufficient_warmup"
            elif metrics["trades"] == 0:
                status = "no_signal"
            else:
                status = "traded"
            metrics["status"] = status
            completed[name] = metrics

    eligible = [
        (name, metrics) for name, metrics in completed.items()
        if metrics["trades"] >= min_trades
    ]
    eligible.sort(
        key=lambda item: (
            item[1]["net_bps"], item[1]["average_net_bps"],
            -item[1]["max_drawdown_bps"], item[1]["trades"],
        ),
        reverse=True,
    )
    result["strategy"] = asdict(parameters)
    result["pairs"] = completed
    result["ranking"] = [
        {"rank": rank, "pair": name, **metrics}
        for rank, (name, metrics) in enumerate(eligible, 1)
    ]
    result["ranking_min_trades"] = min_trades
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest cross-venue pair trading across many assets and venue pairs"
    )
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--venues", nargs="+", choices=VENUES, required=True)
    parser.add_argument("--symbols", nargs="+", choices=tuple(MARKETS), required=True)
    parser.add_argument("--output", type=Path, default=Path("data/pair-grid.json"))
    parser.add_argument("--window", type=int, default=120)
    parser.add_argument("--freshness-ms", type=float, default=1_000)
    parser.add_argument("--fee-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--warmup-samples", type=int, default=120)
    parser.add_argument("--entry-z", type=float, default=2.5)
    parser.add_argument("--min-deviation-bps", type=float, default=15.0)
    parser.add_argument("--exit-z", type=float, default=0.35)
    parser.add_argument("--stop-z", type=float, default=4.5)
    parser.add_argument("--max-hold-seconds", type=float, default=180.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    parameters = PairTradingParameters(
        warmup_samples=args.warmup_samples,
        entry_z=args.entry_z,
        min_deviation_bps=args.min_deviation_bps,
        exit_z=args.exit_z,
        stop_z=args.stop_z,
        max_hold_seconds=args.max_hold_seconds,
    )
    try:
        report = build_report(
            args.data, args.venues, args.symbols, parameters,
            window=args.window, freshness_ms=args.freshness_ms,
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
