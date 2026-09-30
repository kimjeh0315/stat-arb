"""Exchange-specific BBO adapters and their public registry."""

from .aster import AsterAdapter
from .base import ExchangeAdapter
from .batch import BatchAdapter
from .bybit import BybitAdapter
from .grvt import GrvtAdapter
from .hyperliquid import HyperliquidAdapter

ADAPTERS = {
    "bybit": BybitAdapter,
    "hyperliquid": HyperliquidAdapter,
    "aster": AsterAdapter,
    "grvt": GrvtAdapter,
}

__all__ = [
    "ADAPTERS",
    "AsterAdapter",
    "BatchAdapter",
    "BybitAdapter",
    "ExchangeAdapter",
    "GrvtAdapter",
    "HyperliquidAdapter",
]
