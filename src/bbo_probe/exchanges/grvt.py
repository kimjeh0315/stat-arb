"""GRVT JSON-RPC order-book delta adapter with a local full book."""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Optional

from ..models import BBO
from .base import ExchangeAdapter, InvalidMessage, JsonObject, ReconnectRequired, decimal_value


@dataclass
class _BookState:
    bids: Dict[Decimal, Decimal] = field(default_factory=dict)
    asks: Dict[Decimal, Decimal] = field(default_factory=dict)
    last_sequence: Optional[int] = None


@dataclass(frozen=True)
class GrvtAdapter(ExchangeAdapter):
    instrument: str = "BTC_USDT_Perp"
    rate_ms: int = 50
    _book: _BookState = field(default_factory=_BookState, init=False, repr=False,
                              compare=False)

    @property
    def name(self) -> str:
        return "grvt"

    @property
    def url(self) -> str:
        return "wss://market-data.grvt.io/ws/full"

    def subscription_messages(self):
        # A reconnect starts with a new sequence-0 full-book snapshot.
        self._book.bids.clear()
        self._book.asks.clear()
        self._book.last_sequence = None
        selector = f"{self.instrument}@{self.rate_ms}"
        return ({
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": {"stream": "v1.book.d", "selectors": [selector]},
            "id": 1,
        },)

    def parse(self, message: JsonObject, recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        if message.get("stream") != "v1.book.d" or "feed" not in message:
            return None
        data = message["feed"]
        if data.get("instrument") not in (None, self.instrument):
            return None
        sequence = int(message["sequence_number"])
        previous = self._book.last_sequence
        if previous is not None and sequence == previous:
            return None
        if sequence == 0:
            self._book.bids.clear()
            self._book.asks.clear()
        elif previous is None:
            raise ReconnectRequired("GRVT delta received before the initial snapshot")
        elif previous != 0 and (
            sequence != previous + 1
            or int(message.get("prev_sequence_number", -1)) != previous
        ):
            self._book.bids.clear()
            self._book.asks.clear()
            self._book.last_sequence = None
            raise ReconnectRequired(
                f"GRVT sequence gap: previous={previous}, current={sequence}"
            )

        self._apply_levels(self._book.bids, data.get("bids", []))
        self._apply_levels(self._book.asks, data.get("asks", []))
        if not self._book.bids or not self._book.asks:
            raise InvalidMessage("GRVT local orderbook is missing a bid or ask")
        self._book.last_sequence = sequence
        bid_price = max(self._book.bids)
        ask_price = min(self._book.asks)
        return BBO(
            exchange=self.name,
            instrument=str(data.get("instrument", self.instrument)),
            bid_price=bid_price,
            bid_size=self._book.bids[bid_price],
            ask_price=ask_price,
            ask_size=self._book.asks[ask_price],
            exchange_ts_ns=int(data["event_time"]),
            recv_ts_ns=recv_ts_ns,
            recv_mono_ns=recv_mono_ns,
            sequence=sequence,
            prev_sequence=(
                None
                if message.get("prev_sequence_number") is None
                else int(message["prev_sequence_number"])
            ),
        )

    @staticmethod
    def _apply_levels(book: Dict[Decimal, Decimal], levels) -> None:
        for level in levels:
            price = decimal_value(level["price"])
            size = decimal_value(level["size"])
            if size < 0:
                raise InvalidMessage("GRVT orderbook level has a negative size")
            if size == 0:
                book.pop(price, None)
            else:
                book[price] = size
