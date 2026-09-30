import json

from bbo_probe.cli import build_adapters
from bbo_probe.exchanges import BatchAdapter
from bbo_probe.strategy import Action
from bbo_probe.strategy.engine import run_backtest


def test_build_adapters_batches_symbols_per_venue() -> None:
    adapters, missing = build_adapters(["bybit", "aster"], ["BTC", "ETH"])
    assert missing == []
    assert len(adapters) == 2
    assert all(isinstance(adapter, BatchAdapter) for adapter in adapters)
    aster = next(adapter for adapter in adapters if adapter.name == "aster")
    [message] = list(aster.subscription_messages())
    assert message["params"] == ["btcusdt@bookTicker", "ethusdt@bookTicker"]


def test_build_adapters_batches_grvt_delta_subscriptions() -> None:
    adapters, missing = build_adapters(["grvt"], ["BTC", "ETH"])
    assert missing == []
    [adapter] = adapters
    [message] = list(adapter.subscription_messages())
    assert message["params"] == {
        "stream": "v1.book.d",
        "selectors": ["BTC_USDT_Perp@50", "ETH_USDT_Perp@50"],
    }


def test_event_backtest_uses_bid_ask_and_costs(tmp_path) -> None:
    rows = [
        ("bybit", "BTCUSDT", "100", "101", 1_000_000_000),
        ("aster", "BTCUSDT", "100", "101", 2_000_000_000),
        ("bybit", "BTCUSDT", "110", "111", 3_000_000_000),
        ("bybit", "BTCUSDT", "100", "101", 4_000_000_000),
    ]
    data = tmp_path / "bbo.jsonl"
    data.write_text("".join(json.dumps({
        "exchange": venue, "instrument": instrument,
        "bid_price": bid, "bid_size": "1", "ask_price": ask, "ask_size": "1",
        "recv_ts_ns": timestamp, "valid": True,
    }) + "\n" for venue, instrument, bid, ask, timestamp in rows), encoding="utf-8")

    observed_frames = []

    def hypothesis(frame):
        observed_frames.append(frame)
        if frame.position == 0 and frame.zscore >= 0.9:
            return Action.SHORT_LEFT
        if frame.position != 0:
            return Action.CLOSE
        return Action.HOLD

    result = run_backtest(
        data, ["bybit", "aster"], ["BTC"], hypothesis,
        window=3, freshness_ms=2_000, fee_bps=5,
    )
    assert result["summary"]["trades"] == 1
    assert result["summary"]["gross_bps"] > 0
    assert result["trades"][0]["cost_bps"] == 10
    assert result["summary"]["net_bps"] < result["summary"]["gross_bps"]
    pair = result["pairs"]["BTC:bybit-aster"]
    assert pair["samples"] == 3
    assert pair["trades"] == 1
    assert pair["max_drawdown_bps"] >= 0
    assert observed_frames[-1].deviation_bps == observed_frames[-1].basis_bps - observed_frames[-1].basis_mean_bps
    assert observed_frames[-1].basis_std_bps > 0
