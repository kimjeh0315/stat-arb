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
from bbo_probe.strategy.pair_rules import PairTradingParameters, decide_pair


ASSET = "BTC"
VENUES = frozenset(("bybit", "grvt"))
WARMUP_SAMPLES = 120
ENTRY_Z = 2.5
MIN_DEVIATION_BPS = 15.0
EXIT_Z = 0.35
STOP_Z = 4.5
MAX_HOLD_SECONDS = 180.0
PARAMETERS = PairTradingParameters(
    warmup_samples=WARMUP_SAMPLES,
    entry_z=ENTRY_Z,
    min_deviation_bps=MIN_DEVIATION_BPS,
    exit_z=EXIT_Z,
    stop_z=STOP_Z,
    max_hold_seconds=MAX_HOLD_SECONDS,
)


def decide(x: HypothesisInput) -> Action:
    if x.asset != ASSET or frozenset((x.left_venue, x.right_venue)) != VENUES:
        return Action.HOLD
    return decide_pair(x, PARAMETERS)
