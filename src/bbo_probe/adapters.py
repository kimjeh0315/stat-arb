from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

from .models import BBO


def _d(value: Any) -> Decimal:
    return Decimal(str(value))


def _level(value: Any) -> Optional[tuple]:
    if value is None:
        return None
    if isinstance(value, dict):
        return _d(value["px"]), _d(value["sz"])
    return _d(value[0]), _d(value[1])


@dataclass(frozen=True)
class VenueAdapter:
    name: str
    url: str
    instrument: str

    def subscription(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def parse(self, message: Dict[str, Any], recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        raise NotImplementedError


@dataclass(frozen=True)
class BybitAdapter(VenueAdapter):
    name: str = "bybit"
    url: str = "wss://stream.bybit.com/v5/public/linear"
    instrument: str = "BTCUSDT"

    def subscription(self) -> Dict[str, Any]:
        return {"op": "subscribe", "args": [f"orderbook.1.{self.instrument}"]}

    def parse(self, message: Dict[str, Any], recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        if not str(message.get("topic", "")).startswith("orderbook.1."):
            return None
        data = message["data"]
        bid = _level(data["b"][0])
        ask = _level(data["a"][0])
        return BBO(self.name, data.get("s", self.instrument), *bid, *ask,
                   int(message.get("cts", message["ts"])) * 1_000_000,
                   recv_ts_ns, recv_mono_ns, int(data["u"]))


@dataclass(frozen=True)
class HyperliquidAdapter(VenueAdapter):
    name: str = "hyperliquid"
    url: str = "wss://api.hyperliquid.xyz/ws"
    instrument: str = "BTC"

    def subscription(self) -> Dict[str, Any]:
        return {"method": "subscribe", "subscription": {"type": "bbo", "coin": self.instrument}}

    def parse(self, message: Dict[str, Any], recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        if message.get("channel") != "bbo":
            return None
        data = message["data"]
        bid, ask = (_level(level) for level in data["bbo"])
        if bid is None or ask is None:
            return None
        return BBO(self.name, data["coin"], *bid, *ask, int(data["time"]) * 1_000_000,
                   recv_ts_ns, recv_mono_ns)


@dataclass(frozen=True)
class AsterAdapter(VenueAdapter):
    name: str = "aster"
    url: str = "wss://fstream.asterdex.com/ws"
    instrument: str = "BTCUSDT"

    def subscription(self) -> Dict[str, Any]:
        return {"method": "SUBSCRIBE", "params": [f"{self.instrument.lower()}@bookTicker"], "id": 1}

    def parse(self, message: Dict[str, Any], recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        data = message.get("data", message)
        if not all(key in data for key in ("b", "B", "a", "A")):
            return None
        exchange_ms = data.get("T", data.get("E"))
        return BBO(self.name, data.get("s", self.instrument), _d(data["b"]), _d(data["B"]),
                   _d(data["a"]), _d(data["A"]),
                   None if exchange_ms is None else int(exchange_ms) * 1_000_000,
                   recv_ts_ns, recv_mono_ns,
                   None if data.get("u") is None else int(data["u"]))


@dataclass(frozen=True)
class GrvtAdapter(VenueAdapter):
    name: str = "grvt"
    url: str = "wss://market-data.grvt.io/ws/full"
    instrument: str = "BTC_USDT_Perp"
    rate_ms: int = 500
    depth: int = 10

    def subscription(self) -> Dict[str, Any]:
        selector = f"{self.instrument}@{self.rate_ms}-{self.depth}"
        return {"jsonrpc": "2.0", "method": "subscribe",
                "params": {"stream": "v1.book.s", "selectors": [selector]}, "id": 1}

    def parse(self, message: Dict[str, Any], recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        if message.get("stream") != "v1.book.s" or "feed" not in message:
            return None
        data = message["feed"]
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            return None
        bid = max(bids, key=lambda x: _d(x["price"]))
        ask = min(asks, key=lambda x: _d(x["price"]))
        return BBO(self.name, data["instrument"], _d(bid["price"]), _d(bid["size"]),
                   _d(ask["price"]), _d(ask["size"]), int(data["event_time"]),
                   recv_ts_ns, recv_mono_ns, int(message["sequence_number"]),
                   None if message.get("prev_sequence_number") is None
                   else int(message["prev_sequence_number"]))


ADAPTERS = {
    "bybit": BybitAdapter,
    "hyperliquid": HyperliquidAdapter,
    "aster": AsterAdapter,
    "grvt": GrvtAdapter,
}
