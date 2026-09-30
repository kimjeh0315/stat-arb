from pathlib import Path

import pytest

from bbo_probe.strategy import Action, HypothesisInput
from bbo_probe.strategy.loader import load_hypothesis
from bbo_probe.strategy.pair_rules import PairTradingParameters


DECIDE = load_hypothesis(Path("hypotheses/btc_bybit_grvt_pair.py"))


def frame(**overrides) -> HypothesisInput:
    values = {
        "asset": "BTC",
        "left_venue": "bybit",
        "right_venue": "grvt",
        "timestamp_ns": 1,
        "basis_bps": 25.0,
        "zscore": 2.6,
        "sample_count": 120,
        "position": 0,
        "holding_seconds": 0.0,
        "basis_mean_bps": 5.0,
        "basis_std_bps": 7.7,
        "deviation_bps": 20.0,
    }
    values.update(overrides)
    return HypothesisInput(**values)


def test_pair_hypothesis_enters_both_directions() -> None:
    assert DECIDE(frame()) == Action.SHORT_LEFT
    assert DECIDE(frame(zscore=-2.6, deviation_bps=-20.0)) == Action.LONG_LEFT


def test_pair_hypothesis_requires_warmup_cost_buffer_and_target_pair() -> None:
    assert DECIDE(frame(sample_count=119)) == Action.HOLD
    assert DECIDE(frame(deviation_bps=14.9)) == Action.HOLD
    assert DECIDE(frame(zscore=4.5)) == Action.HOLD
    assert DECIDE(frame(asset="ETH")) == Action.HOLD
    assert DECIDE(frame(right_venue="aster")) == Action.HOLD


def test_pair_hypothesis_closes_directionally_on_reversion() -> None:
    assert DECIDE(frame(position=-1, zscore=0.35)) == Action.CLOSE
    assert DECIDE(frame(position=1, zscore=-0.35)) == Action.CLOSE
    assert DECIDE(frame(position=-1, zscore=1.0)) == Action.HOLD
    assert DECIDE(frame(position=1, zscore=-1.0)) == Action.HOLD


def test_pair_hypothesis_has_stop_and_time_exit() -> None:
    assert DECIDE(frame(position=-1, zscore=4.5)) == Action.CLOSE
    assert DECIDE(frame(position=1, zscore=-4.5)) == Action.CLOSE
    assert DECIDE(frame(position=-1, zscore=1.0, holding_seconds=180.0)) == Action.CLOSE


def test_pair_parameters_reject_inconsistent_thresholds() -> None:
    with pytest.raises(ValueError):
        PairTradingParameters(entry_z=5.0, stop_z=4.5)
    with pytest.raises(ValueError):
        PairTradingParameters(warmup_samples=1)
