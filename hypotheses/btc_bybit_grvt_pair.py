"""BTC cross-venue pair hypothesis for Bybit and GRVT."""

from bbo_probe.strategy.pair_hypothesis import PairHypothesis
from bbo_probe.strategy.pair_rules import PairTradingParameters


HYPOTHESIS = PairHypothesis(
    hypothesis_id="btc_bybit_grvt_pair",
    version=1,
    title="BTC Bybit-GRVT basis mean reversion",
    thesis=(
        "The BTC log-mid basis between Bybit and GRVT is locally stationary; "
        "liquidity shocks exceeding costs revert toward their rolling mean."
    ),
    window=120,
    assets=("BTC",),
    venue_pairs=(("bybit", "grvt"),),
    parameters=PairTradingParameters(
        warmup_samples=120,
        entry_z=2.5,
        min_deviation_bps=15.0,
        exit_z=0.35,
        stop_z=4.5,
        max_hold_seconds=180.0,
    ),
)

decide = HYPOTHESIS.decide
