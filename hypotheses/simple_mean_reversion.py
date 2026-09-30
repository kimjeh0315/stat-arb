"""Broad cross-venue mean-reversion example hypothesis."""

from bbo_probe.strategy.pair_hypothesis import PairHypothesis
from bbo_probe.strategy.pair_rules import PairTradingParameters


HYPOTHESIS = PairHypothesis(
    hypothesis_id="simple_mean_reversion",
    version=1,
    title="Simple cross-venue basis mean reversion",
    thesis="Large cross-venue log-mid basis z-scores revert toward their rolling mean.",
    window=120,
    parameters=PairTradingParameters(
        warmup_samples=60,
        entry_z=2.5,
        min_deviation_bps=0.0,
        exit_z=0.5,
        stop_z=100.0,
        max_hold_seconds=300.0,
    ),
)

decide = HYPOTHESIS.decide
