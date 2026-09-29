"""Aster USDT-futures real-time book-ticker adapter."""

from dataclasses import dataclass
from typing import Optional

from ..models import BBO
from .base import ExchangeAdapter, JsonObject, decimal_value


@dataclass(frozen=True)
class AsterAdapter(ExchangeAdapter):
    instrument: str = "BTCUSDT"

    @property
    def name(self) -> str:
        return "aster"

    @property
    def url(self) -> str:
        return "wss://fstream.asterdex.com/ws"

    def subscription_messages(self):
        return ({
            "method": "SUBSCRIBE",
            "params": [f"{self.instrument.lower()}@bookTicker"],
            "id": 1,
        },)

    def parse(self, message: JsonObject, recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        # Combined streams wrap the same raw payload in ``data``.
        data = message.get("data", message)
        if not isinstance(data, dict) or data.get("e") not in (None, "bookTicker"):
            return None
        if not all(key in data for key in ("b", "B", "a", "A")):
            return None
        exchange_ms = data.get("T", data.get("E"))
        return BBO(
            exchange=self.name,
            instrument=str(data.get("s", self.instrument)),
            bid_price=decimal_value(data["b"]),
            bid_size=decimal_value(data["B"]),
            ask_price=decimal_value(data["a"]),
            ask_size=decimal_value(data["A"]),
            exchange_ts_ns=None if exchange_ms is None else int(exchange_ms) * 1_000_000,
            recv_ts_ns=recv_ts_ns,
            recv_mono_ns=recv_mono_ns,
            sequence=None if data.get("u") is None else int(data["u"]),
        )
