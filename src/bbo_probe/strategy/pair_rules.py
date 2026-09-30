"""Reusable cross-venue pair-trading decision rules."""

from dataclasses import dataclass

from .types import Action, HypothesisInput


@dataclass(frozen=True)
class PairTradingParameters:
    warmup_samples: int = 120
    entry_z: float = 2.5
    min_deviation_bps: float = 15.0
    exit_z: float = 0.35
    stop_z: float = 4.5
    max_hold_seconds: float = 180.0

    def __post_init__(self) -> None:
        if self.warmup_samples < 2:
            raise ValueError("warmup_samples must be at least 2")
        if not 0 <= self.exit_z < self.entry_z < self.stop_z:
            raise ValueError("thresholds must satisfy 0 <= exit_z < entry_z < stop_z")
        if self.min_deviation_bps < 0 or self.max_hold_seconds <= 0:
            raise ValueError("deviation must be non-negative and holding limit positive")


DEFAULT_PARAMETERS = PairTradingParameters()


def decide_pair(
    x: HypothesisInput, parameters: PairTradingParameters = DEFAULT_PARAMETERS
) -> Action:
    """Trade a rolling-mean reversion signal for any asset and venue pair."""
    if x.position == 0:
        if x.sample_count < parameters.warmup_samples:
            return Action.HOLD
        if (
            abs(x.deviation_bps) < parameters.min_deviation_bps
            or abs(x.zscore) >= parameters.stop_z
        ):
            return Action.HOLD
        if x.zscore >= parameters.entry_z:
            return Action.SHORT_LEFT
        if x.zscore <= -parameters.entry_z:
            return Action.LONG_LEFT
        return Action.HOLD

    timed_out = x.holding_seconds >= parameters.max_hold_seconds
    stopped_out = abs(x.zscore) >= parameters.stop_z
    long_left_reverted = x.position == 1 and x.zscore >= -parameters.exit_z
    short_left_reverted = x.position == -1 and x.zscore <= parameters.exit_z
    if timed_out or stopped_out or long_left_reverted or short_left_reverted:
        return Action.CLOSE
    return Action.HOLD
