from .aster import AsterAdapter
from .bybit import BybitAdapter
from .grvt import GrvtAdapter
from .hyperliquid import HyperliquidAdapter

ADAPTERS = {"bybit": BybitAdapter, "aster": AsterAdapter,
            "grvt": GrvtAdapter, "hyperliquid": HyperliquidAdapter}
