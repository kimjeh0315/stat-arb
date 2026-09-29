from bbo_probe.exchanges import AsterAdapter, BybitAdapter, GrvtAdapter, HyperliquidAdapter


NOW = 1_700_000_000_100_000_000
MONO = 123


def test_bybit() -> None:
    quote = BybitAdapter().parse({"topic": "orderbook.1.BTCUSDT", "ts": 1700000000001,
        "cts": 1700000000000, "data": {"s": "BTCUSDT", "b": [["100", "2"]],
        "a": [["101", "3"]], "u": 9}}, NOW, MONO)
    assert quote is not None and quote.valid and quote.sequence == 9


def test_bybit_heartbeat() -> None:
    adapter = BybitAdapter()
    assert adapter.heartbeat_interval == 20
    assert adapter.heartbeat_message() == {"op": "ping"}
    assert list(adapter.subscription_messages()) == [
        {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}
    ]


def test_hyperliquid() -> None:
    quote = HyperliquidAdapter().parse({"channel": "bbo", "data": {"coin": "BTC",
        "time": 1700000000000, "bbo": [{"px": "100", "sz": "2", "n": 1},
        {"px": "101", "sz": "3", "n": 1}]}}, NOW, MONO)
    assert quote is not None and quote.bid_size == 2


def test_hyperliquid_ignores_one_sided_bbo() -> None:
    quote = HyperliquidAdapter().parse({"channel": "bbo", "data": {"coin": "BTC",
        "time": 1700000000000, "bbo": [None, {"px": "101", "sz": "3", "n": 1}]}}, NOW, MONO)
    assert quote is None


def test_hyperliquid_subscription_and_heartbeat() -> None:
    adapter = HyperliquidAdapter()
    assert list(adapter.subscription_messages()) == [{
        "method": "subscribe", "subscription": {"type": "bbo", "coin": "BTC"}
    }]
    assert adapter.heartbeat_message() == {"method": "ping"}


def test_aster_combined_payload() -> None:
    quote = AsterAdapter().parse({"stream": "btcusdt@bookTicker", "data": {"s": "BTCUSDT",
        "u": 3, "E": 1700000000000, "b": "100", "B": "2", "a": "101", "A": "3"}}, NOW, MONO)
    assert quote is not None and quote.sequence == 3


def test_aster_subscription_uses_lowercase_symbol() -> None:
    assert list(AsterAdapter(instrument="ETHUSDT").subscription_messages()) == [{
        "method": "SUBSCRIBE", "params": ["ethusdt@bookTicker"], "id": 1
    }]


def test_grvt_sorts_snapshot_and_preserves_sequence() -> None:
    quote = GrvtAdapter().parse({"stream": "v1.book.s", "selector": "BTC_USDT_Perp",
        "sequence_number": "11", "prev_sequence_number": "10", "feed": {
        "event_time": "1700000000000000000", "instrument": "BTC_USDT_Perp",
        "bids": [{"price": "99", "size": "1"}, {"price": "100", "size": "2"}],
        "asks": [{"price": "102", "size": "1"}, {"price": "101", "size": "3"}]}}, NOW, MONO)
    assert quote is not None and quote.bid_price == 100 and quote.ask_price == 101
    assert quote.prev_sequence == 10


def test_grvt_subscription_selector() -> None:
    assert list(GrvtAdapter(rate_ms=500, depth=10).subscription_messages()) == [{
        "jsonrpc": "2.0", "method": "subscribe", "params": {
            "stream": "v1.book.s", "selectors": ["BTC_USDT_Perp@500-10"]
        }, "id": 1
    }]
