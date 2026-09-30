"""Bybit V5 linear-perpetual level-one order book adapter."""

from dataclasses import dataclass
from typing import Optional

from ..models import BBO
from .base import ExchangeAdapter, InvalidMessage, JsonObject, price_level


@dataclass(frozen=True)
class BybitAdapter(ExchangeAdapter):
    instrument: str = "BTCUSDT"

    @property
    def name(self) -> str:
        return "bybit"

    @property
    def url(self) -> str:
        return "wss://stream.bybit.com/v5/public/linear"

    def subscription_messages(self):
        return ({"op": "subscribe", "args": [f"orderbook.1.{self.instrument}"]},)

    @property
    def heartbeat_interval(self) -> float:
        return 20.0

    def heartbeat_message(self) -> JsonObject:
        return {"op": "ping"}

    def parse(self, message: JsonObject, recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        if message.get("topic") != f"orderbook.1.{self.instrument}":
            return None
        data = message.get("data")
        if not isinstance(data, dict) or not data.get("b") or not data.get("a"):
            raise InvalidMessage("Bybit orderbook snapshot is missing a bid or ask")
        bid = price_level(data["b"][0])
        ask = price_level(data["a"][0])
        if bid is None or ask is None:
            raise InvalidMessage("Bybit orderbook snapshot contains an empty level")
        exchange_ms = message.get("cts", message.get("ts"))
        return BBO(
            exchange=self.name,
            instrument=str(data.get("s", self.instrument)),
            bid_price=bid[0],
            bid_size=bid[1],
            ask_price=ask[0],
            ask_size=ask[1],
            exchange_ts_ns=None if exchange_ms is None else int(exchange_ms) * 1_000_000,
            recv_ts_ns=recv_ts_ns,
            recv_mono_ns=recv_mono_ns,
            sequence=None if data.get("u") is None else int(data["u"]),
        )
