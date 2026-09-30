"""Common contract shared by public market-data WebSocket adapters."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Optional, Tuple

from ..models import BBO

JsonObject = Dict[str, Any]
PriceLevel = Tuple[Decimal, Decimal]


class InvalidMessage(ValueError):
    """Raised when a venue payload has invalid numeric market data."""


class ReconnectRequired(InvalidMessage):
    """Raised when local stream state can only be repaired by reconnecting."""


def decimal_value(value: Any) -> Decimal:
    """Convert JSON numbers without passing through binary floating point."""
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise InvalidMessage(f"invalid decimal value: {value!r}") from exc


def price_level(value: Any) -> Optional[PriceLevel]:
    """Normalize either ``[price, size]`` or ``{px, sz}`` levels."""
    if value is None:
        return None
    try:
        if isinstance(value, dict):
            return decimal_value(value["px"]), decimal_value(value["sz"])
        return decimal_value(value[0]), decimal_value(value[1])
    except (KeyError, IndexError, TypeError) as exc:
        raise InvalidMessage(f"invalid price level: {value!r}") from exc


@dataclass(frozen=True)
class ExchangeAdapter(ABC):
    """Venue protocol details consumed by the generic collector."""

    instrument: str

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable venue identifier used in normalized output."""

    @property
    @abstractmethod
    def url(self) -> str:
        """Public market-data WebSocket endpoint."""

    def subscription_messages(self) -> Iterable[JsonObject]:
        """Messages sent once immediately after a connection is established."""
        return ()

    @property
    def heartbeat_interval(self) -> Optional[float]:
        """Seconds between application-level heartbeats, if the venue needs one."""
        return None

    def heartbeat_message(self) -> Optional[JsonObject]:
        """Return an application heartbeat; protocol ping frames are automatic."""
        return None

    @abstractmethod
    def parse(self, message: JsonObject, recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        """Return a normalized quote, or ``None`` for ack/heartbeat messages."""
