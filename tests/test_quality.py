from decimal import Decimal

from bbo_probe.collector import QualityTracker
from bbo_probe.models import BBO


def quote(seq, prev=None, bid="100", ask="101") -> BBO:
    return BBO("grvt", "BTC_USDT_Perp", Decimal(bid), Decimal("1"), Decimal(ask),
               Decimal("1"), 1, 2, 3, seq, prev)


def test_quality_flags_gap_duplicate_and_cross() -> None:
    tracker = QualityTracker()
    tracker.observe(quote(10, 9))
    tracker.observe(quote(10, 10))
    tracker.observe(quote(12, 11, bid="102", ask="101"))
    stats = tracker.counts["grvt"]
    assert stats["duplicate_sequence"] == 1
    assert stats["sequence_gap"] == 1
    assert stats["invalid_or_crossed"] == 1


def test_grvt_snapshot_transition_is_not_a_gap() -> None:
    tracker = QualityTracker()
    tracker.observe(quote(999, 998))
    tracker.observe(quote(0, 0))
    tracker.observe(quote(22896, 22895))
    assert tracker.counts["grvt"]["snapshots"] == 1
    assert tracker.counts["grvt"]["sequence_gap"] == 0
