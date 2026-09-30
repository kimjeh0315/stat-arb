"""BTC cross-venue pair trade: fade temporary Bybit/GRVT basis shocks.

Research hypothesis
-------------------
The log mid-price basis between Bybit and GRVT is locally stationary. A move
that is both statistically unusual and large enough to clear expected trading
costs should revert toward its rolling mean.

The 15 bp minimum deviation assumes roughly 10 bp of round-trip cost in the
default backtest (5 bp per fill on two equal-notional legs) plus a 5 bp buffer.
"""

from bbo_probe.strategy import Action, HypothesisInput


ASSET = "BTC"
VENUES = frozenset(("bybit", "grvt"))
WARMUP_SAMPLES = 120
ENTRY_Z = 2.5
MIN_DEVIATION_BPS = 15.0
EXIT_Z = 0.35
STOP_Z = 4.5
MAX_HOLD_SECONDS = 180.0


def decide(x: HypothesisInput) -> Action:
    if x.asset != ASSET or frozenset((x.left_venue, x.right_venue)) != VENUES:
        return Action.HOLD

    if x.position == 0:
        if x.sample_count < WARMUP_SAMPLES:
            return Action.HOLD
        if abs(x.deviation_bps) < MIN_DEVIATION_BPS or abs(x.zscore) >= STOP_Z:
            return Action.HOLD
        if x.zscore >= ENTRY_Z:
            return Action.SHORT_LEFT
        if x.zscore <= -ENTRY_Z:
            return Action.LONG_LEFT
        return Action.HOLD

    timed_out = x.holding_seconds >= MAX_HOLD_SECONDS
    stopped_out = abs(x.zscore) >= STOP_Z
    long_left_reverted = x.position == 1 and x.zscore >= -EXIT_Z
    short_left_reverted = x.position == -1 and x.zscore <= EXIT_Z
    if timed_out or stopped_out or long_left_reverted or short_left_reverted:
        return Action.CLOSE
    return Action.HOLD
