"""GRVT full JSON-RPC order-book snapshot adapter."""

from dataclasses import dataclass
from typing import Optional

from ..models import BBO
from .base import ExchangeAdapter, InvalidMessage, JsonObject, decimal_value


@dataclass(frozen=True)
class GrvtAdapter(ExchangeAdapter):
    instrument: str = "BTC_USDT_Perp"
    rate_ms: int = 500
    depth: int = 10

    @property
    def name(self) -> str:
        return "grvt"

    @property
    def url(self) -> str:
        return "wss://market-data.grvt.io/ws/full"

    def subscription_messages(self):
        selector = f"{self.instrument}@{self.rate_ms}-{self.depth}"
        return ({
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": {"stream": "v1.book.s", "selectors": [selector]},
            "id": 1,
        },)

    def parse(self, message: JsonObject, recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        if message.get("stream") != "v1.book.s" or "feed" not in message:
            return None
        data = message["feed"]
        if data.get("instrument") not in (None, self.instrument):
            return None
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            raise InvalidMessage("GRVT orderbook snapshot is missing a bid or ask")
        bid = max(bids, key=lambda level: decimal_value(level["price"]))
        ask = min(asks, key=lambda level: decimal_value(level["price"]))
        return BBO(
            exchange=self.name,
            instrument=str(data.get("instrument", self.instrument)),
            bid_price=decimal_value(bid["price"]),
            bid_size=decimal_value(bid["size"]),
            ask_price=decimal_value(ask["price"]),
            ask_size=decimal_value(ask["size"]),
            exchange_ts_ns=int(data["event_time"]),
            recv_ts_ns=recv_ts_ns,
            recv_mono_ns=recv_mono_ns,
            sequence=None if message.get("sequence_number") is None else int(message["sequence_number"]),
            prev_sequence=(
                None
                if message.get("prev_sequence_number") is None
                else int(message["prev_sequence_number"])
            ),
        )
