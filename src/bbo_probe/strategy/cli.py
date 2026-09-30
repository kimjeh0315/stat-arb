import argparse
import json
from pathlib import Path

from ..markets import MARKETS, VENUES
from .engine import run_backtest
from .loader import load_hypothesis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a BBO hypothesis through the research framework")
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--venues", nargs="+", choices=VENUES, required=True)
    parser.add_argument("--symbols", nargs="+", choices=tuple(MARKETS), required=True)
    parser.add_argument("--hypothesis", type=Path,
                        default=Path("hypotheses/simple_mean_reversion.py"))
    parser.add_argument("--window", type=int, default=120)
    parser.add_argument("--freshness-ms", type=float, default=1_000)
    parser.add_argument("--fee-bps", type=float, default=5.0,
                        help="taker fee assumption for each fill")
    parser.add_argument("--output", type=Path)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_backtest(
        args.data, args.venues, args.symbols, load_hypothesis(args.hypothesis),
        window=args.window, freshness_ms=args.freshness_ms, fee_bps=args.fee_bps,
    )
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
