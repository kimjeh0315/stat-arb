"""Evaluate a grid of pair-trading parameters without hiding failed pairs."""

import argparse
import json
from collections import Counter
from itertools import product
from pathlib import Path
from typing import Any, Dict, List

from bbo_probe.markets import MARKETS, TOP_MARKETS, VENUES
from bbo_probe.strategy.grid import build_grid_report
from bbo_probe.strategy.loader import load_pair_hypothesis
from bbo_probe.strategy.pair_rules import PairTradingParameters


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sweep pair-trading parameters across assets and venue pairs"
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
    parser.add_argument("--output", type=Path, default=Path("data/pair-sweep.json"))
    parser.add_argument("--windows", nargs="+", type=int)
    parser.add_argument("--warmup-samples", nargs="+", type=int)
    parser.add_argument("--entry-z", nargs="+", type=float)
    parser.add_argument("--min-deviation-bps", nargs="+", type=float)
    parser.add_argument("--exit-z", nargs="+", type=float)
    parser.add_argument("--stop-z", nargs="+", type=float)
    parser.add_argument("--max-hold-seconds", nargs="+", type=float)
    parser.add_argument("--freshness-ms", type=float, default=1_000)
    parser.add_argument("--fee-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--top-pairs", type=int, default=10)
    parser.add_argument("--max-runs", type=int, default=100)
    return parser


def parameter_grid(args, hypothesis) -> List[tuple]:
    base = hypothesis.parameters
    sweep = hypothesis.sweep
    grid = list(product(
        args.windows or sweep.windows or [hypothesis.window],
        args.warmup_samples or sweep.warmup_samples or [base.warmup_samples],
        args.entry_z or sweep.entry_z or [base.entry_z],
        args.min_deviation_bps or sweep.min_deviation_bps or [base.min_deviation_bps],
        args.exit_z or sweep.exit_z or [base.exit_z],
        args.stop_z or sweep.stop_z or [base.stop_z],
        args.max_hold_seconds or sweep.max_hold_seconds or [base.max_hold_seconds],
    ))
    if len(grid) > args.max_runs:
        raise ValueError(
            f"parameter grid has {len(grid)} runs; reduce it or raise --max-runs"
        )
    return grid


def compact_result(run_id: int, report: Dict[str, Any], top_pairs: int) -> Dict[str, Any]:
    statuses = Counter(pair["status"] for pair in report["pairs"].values())
    return {
        "run": run_id,
        "variant": report["hypothesis"]["variant"],
        "summary": {
            **report["summary"],
            "profitable_pairs": sum(
                pair["trades"] >= report["ranking_min_trades"] and pair["net_bps"] > 0
                for pair in report["pairs"].values()
            ),
        },
        "pair_statuses": dict(sorted(statuses.items())),
        "top_pairs": report["ranking"][:top_pairs],
    }


def main() -> None:
    args = build_parser().parse_args()
    assets = (
        args.symbols
        if args.symbols is not None
        else [market.asset for market in TOP_MARKETS[:args.top]]
    )
    try:
        hypothesis = load_pair_hypothesis(args.hypothesis)
        grid = parameter_grid(args, hypothesis.specification)
        runs = []
        for run_id, values in enumerate(grid, 1):
            window, warmup, entry, deviation, exit_z, stop, max_hold = values
            parameters = PairTradingParameters(
                warmup_samples=warmup,
                entry_z=entry,
                min_deviation_bps=deviation,
                exit_z=exit_z,
                stop_z=stop,
                max_hold_seconds=max_hold,
            )
            report = build_grid_report(
                args.data, args.venues, assets, hypothesis.with_parameters(parameters),
                window=window, freshness_ms=args.freshness_ms,
                fee_bps=args.fee_bps, min_trades=args.min_trades,
            )
            runs.append(compact_result(run_id, report, args.top_pairs))
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    runs.sort(
        key=lambda run: (
            run["summary"]["net_bps"], run["summary"]["average_net_bps"],
            run["summary"]["wins"],
        ),
        reverse=True,
    )
    result = {
        "hypothesis": hypothesis.definition_manifest(),
        "configuration": {
            "data": str(args.data), "venues": args.venues, "assets": assets,
            "freshness_ms": args.freshness_ms, "fee_bps_per_fill": args.fee_bps,
            "min_trades": args.min_trades,
        },
        "runs": runs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(args.output), "runs": len(runs),
        "assets": len(assets), "venue_pairs": len(args.venues) * (len(args.venues) - 1) // 2,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
