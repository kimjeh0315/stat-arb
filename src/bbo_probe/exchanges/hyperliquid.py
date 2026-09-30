"""Hyperliquid block-driven BBO stream adapter."""

from dataclasses import dataclass
from typing import Optional

from ..models import BBO
from .base import ExchangeAdapter, JsonObject, price_level


@dataclass(frozen=True)
class HyperliquidAdapter(ExchangeAdapter):
    instrument: str = "BTC"

    @property
    def name(self) -> str:
        return "hyperliquid"

    @property
    def url(self) -> str:
        return "wss://api.hyperliquid.xyz/ws"

    def subscription_messages(self):
        return ({
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": self.instrument},
        },)

    @property
    def heartbeat_interval(self) -> float:
        # Hyperliquid closes quiet connections after 60 seconds.
        return 30.0

    def heartbeat_message(self) -> JsonObject:
        return {"method": "ping"}

    def parse(self, message: JsonObject, recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        if message.get("channel") != "bbo" or message.get("data", {}).get("coin") != self.instrument:
            return None
        data = message["data"]
        bid, ask = (price_level(level) for level in data["bbo"])
        # The official schema allows either side of the BBO to be null.
        if bid is None or ask is None:
            return None
        return BBO(
            exchange=self.name,
            instrument=str(data.get("coin", self.instrument)),
            bid_price=bid[0],
            bid_size=bid[1],
            ask_price=ask[0],
            ask_size=ask[1],
            exchange_ts_ns=int(data["time"]) * 1_000_000,
            recv_ts_ns=recv_ts_ns,
            recv_mono_ns=recv_mono_ns,
        )
