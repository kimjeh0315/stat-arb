"""Reusable hypothesis definition for multi-asset, multi-venue grid research."""

from bbo_probe.strategy.pair_hypothesis import PairHypothesis, PairParameterSweep
from bbo_probe.strategy.pair_rules import PairTradingParameters


HYPOTHESIS = PairHypothesis(
    hypothesis_id="cross_venue_pair_grid",
    version=1,
    title="Cross-venue basis mean-reversion grid",
    thesis=(
        "For each requested crypto asset and venue pair, unusually large log-mid basis "
        "deviations that exceed costs revert toward a rolling mean."
    ),
    window=120,
    parameters=PairTradingParameters(
        warmup_samples=120,
        entry_z=2.5,
        min_deviation_bps=15.0,
        exit_z=0.35,
        stop_z=4.5,
        max_hold_seconds=180.0,
    ),
    sweep=PairParameterSweep(
        entry_z=(1.75, 2.0, 2.5),
        min_deviation_bps=(5.0, 10.0, 15.0),
        exit_z=(0.35, 0.75),
    ),
)

decide = HYPOTHESIS.decide
