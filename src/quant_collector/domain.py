import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple


def number(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("Non-finite decimal")
    return format(parsed, "f")


def ns_from_ms(value: Any) -> Optional[int]:
    return int(value) * 1_000_000 if value is not None else None


def stable_id(*parts: Any) -> str:
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False, sort_keys=True,
                                    default=str).encode()).hexdigest()[:32]


@dataclass(frozen=True)
class Instrument:
    exchange: str
    native_symbol: str
    native_base: str
    base_asset: str
    quote_asset: str
    settle_asset: str
    category: str
    multiplier: str = "1"
    tick_size: Optional[str] = None
    qty_step: Optional[str] = None
    min_qty: Optional[str] = None
    funding_interval_hours: Optional[str] = None

    @property
    def instrument_id(self) -> str:
        return "%s:%s" % (self.exchange, self.native_symbol)

    def price(self, value: Any) -> Optional[str]:
        value = number(value)
        return number(Decimal(value) / Decimal(self.multiplier)) if value is not None else None

    def qty(self, value: Any) -> Optional[str]:
        value = number(value)
        return number(Decimal(value) * Decimal(self.multiplier)) if value is not None else None

    def row(self) -> Dict[str, Any]:
        return dict(asdict(self), instrument_id=self.instrument_id)


class Universe:
    def __init__(self, groups: Dict[str, Any], aliases: Optional[Dict[str, Any]] = None):
        self.assets = {}
        for category, assets in groups.items():
            for asset in assets:
                if asset in self.assets:
                    raise ValueError("Duplicate asset: " + asset)
                self.assets[asset] = category
        self.aliases = aliases or {}

    def resolve(self, exchange: str, native_base: str) -> Optional[Tuple[str, str]]:
        override = self.aliases.get(exchange, {}).get(native_base)
        if override is not None:
            base, multiplier = override["asset"], number(override["multiplier"])
            if base not in self.assets or Decimal(multiplier) <= 0:
                raise ValueError("Invalid explicit alias: " + native_base)
            return base, multiplier
        upper = native_base.upper()
        if upper in self.assets:
            return upper, "1"
        # Only numeric bundle conventions; semantic ticker renames require explicit aliases.
        for prefix, multiplier in (("1000000", "1000000"), ("10000", "10000"),
                                   ("1000", "1000")):
            if upper.startswith(prefix) and upper[len(prefix):] in self.assets:
                return upper[len(prefix):], multiplier
        if exchange == "hyperliquid" and native_base.startswith("k"):
            base = native_base[1:].upper()
            if base in self.assets:
                return base, "1000"
        return None

    def instrument(self, exchange: str, symbol: str, native_base: str,
                   quote: str, settle: str, **kwargs: Any) -> Optional[Instrument]:
        resolved = self.resolve(exchange, native_base)
        if resolved is None:
            return None
        base, multiplier = resolved
        return Instrument(exchange, symbol, native_base, base, quote, settle,
                          self.assets[base], multiplier, **kwargs)


@dataclass
class Context:
    exchange: str
    session_id: str
    raw_id: str
    received_at_ns: int
    received_mono_ns: int

    def event(self, dataset: str, instrument: Instrument,
              event_time_ns: Optional[int], **fields: Any) -> Dict[str, Any]:
        row = dict(dataset=dataset, exchange=self.exchange,
                   instrument_id=instrument.instrument_id, base_asset=instrument.base_asset,
                   category=instrument.category, quote_asset=instrument.quote_asset,
                   session_id=self.session_id, raw_id=self.raw_id,
                   event_time_ns=event_time_ns, received_at_ns=self.received_at_ns,
                   received_mono_ns=self.received_mono_ns, schema_version=1,
                   quality_flags=[])
        row.update(fields)
        # Avoid hashing full depth arrays on every high-frequency book update.
        row["event_id"] = stable_id(self.raw_id, dataset, instrument.instrument_id,
                                    event_time_ns, fields.get("price_type"),
                                    fields.get("native_trade_id"), fields.get("rate_kind"))
        return row
