from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class BBO:
    exchange: str
    instrument: str
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal
    exchange_ts_ns: Optional[int]
    recv_ts_ns: int
    recv_mono_ns: int
    sequence: Optional[int] = None
    prev_sequence: Optional[int] = None

    @property
    def valid(self) -> bool:
        return (
            self.bid_price > 0
            and self.ask_price > 0
            and self.bid_size >= 0
            and self.ask_size >= 0
            and self.bid_price <= self.ask_price
        )

    @property
    def spread_bps(self) -> Decimal:
        mid = (self.bid_price + self.ask_price) / 2
        return (self.ask_price - self.bid_price) / mid * Decimal(10_000)

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        for key in ("bid_price", "bid_size", "ask_price", "ask_size"):
            value[key] = str(value[key])
        value["valid"] = self.valid
        value["spread_bps"] = str(self.spread_bps)
        value["event_age_ms"] = (
            None
            if self.exchange_ts_ns is None
            else (self.recv_ts_ns - self.exchange_ts_ns) / 1_000_000
        )
        return value
