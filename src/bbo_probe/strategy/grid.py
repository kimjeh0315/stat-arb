"""One-pass evaluation and reporting for an asset-by-venue-pair grid."""

from itertools import combinations
from pathlib import Path
from typing import Any, Dict, Sequence

from .engine import run_backtest
from .pair_hypothesis import LoadedPairHypothesis


def build_grid_report(
    data_path: Path,
    venues: Sequence[str],
    assets: Sequence[str],
    hypothesis: LoadedPairHypothesis,
    *,
    window: int,
    freshness_ms: float,
    fee_bps: float,
    min_trades: int,
) -> Dict[str, Any]:
    parameters = hypothesis.specification.parameters
    if len(set(venues)) != len(venues) or len(set(assets)) != len(assets):
        raise ValueError("venues and symbols must not contain duplicates")
    if window < parameters.warmup_samples:
        raise ValueError("window must be at least as large as warmup_samples")
    if min_trades < 1:
        raise ValueError("min_trades must be positive")

    result = run_backtest(
        data_path, venues, assets, hypothesis.specification.decide,
        window=window, freshness_ms=freshness_ms, fee_bps=fee_bps,
    )
    observed = result["pairs"]
    completed: Dict[str, Dict[str, Any]] = {}
    empty = {
        "samples": 0, "trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
        "gross_bps": 0.0, "net_bps": 0.0, "average_net_bps": 0.0,
        "max_drawdown_bps": 0.0,
    }
    for asset in assets:
        for left, right in combinations(venues, 2):
            name = f"{asset.upper()}:{left}-{right}"
            metrics = dict(observed.get(name, empty))
            if not hypothesis.specification.allows(asset.upper(), left, right):
                status = "outside_hypothesis_scope"
            elif metrics["samples"] == 0:
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
    result["hypothesis"] = hypothesis.manifest(window=window)
    result["pairs"] = completed
    result["ranking"] = [
        {"rank": rank, "pair": name, **metrics}
        for rank, (name, metrics) in enumerate(eligible, 1)
    ]
    result["ranking_min_trades"] = min_trades
    return result
