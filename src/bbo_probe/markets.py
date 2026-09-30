"""Canonical asset catalog and exchange-specific perpetual symbols.

The ranking is a point-in-time research universe, not a permanent claim about
market liquidity.  See ``docs/markets.md`` for the methodology and timestamp.
"""

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


VENUES: Tuple[str, ...] = ("bybit", "hyperliquid", "aster", "grvt")


@dataclass(frozen=True)
class Market:
    rank: int
    asset: str
    symbols: Dict[str, str]

    def symbol_for(self, venue: str) -> Optional[str]:
        return self.symbols.get(venue)


# Aggregated 24h perpetual quote/notional volume from Bybit, Hyperliquid and
# Aster at 2026-09-29 16:15 UTC. Tokenized equities and commodities excluded.
TOP_MARKETS: Tuple[Market, ...] = (
    Market(1, "BTC", {"bybit": "BTCUSDT", "hyperliquid": "BTC", "aster": "BTCUSDT", "grvt": "BTC_USDT_Perp"}),
    Market(2, "ETH", {"bybit": "ETHUSDT", "hyperliquid": "ETH", "aster": "ETHUSDT", "grvt": "ETH_USDT_Perp"}),
    Market(3, "ZEC", {"bybit": "ZECUSDT", "hyperliquid": "ZEC", "aster": "ZECUSDT", "grvt": "ZEC_USDT_Perp"}),
    Market(4, "SOL", {"bybit": "SOLUSDT", "hyperliquid": "SOL", "aster": "SOLUSDT", "grvt": "SOL_USDT_Perp"}),
    Market(5, "XRP", {"bybit": "XRPUSDT", "hyperliquid": "XRP", "aster": "XRPUSDT", "grvt": "XRP_USDT_Perp"}),
    Market(6, "HYPE", {"bybit": "HYPEUSDT", "hyperliquid": "HYPE", "aster": "HYPEUSDT", "grvt": "HYPE_USDT_Perp"}),
    Market(7, "NEAR", {"bybit": "NEARUSDT", "hyperliquid": "NEAR", "aster": "NEARUSDT", "grvt": "NEAR_USDT_Perp"}),
    Market(8, "PUMP", {"bybit": "PUMPFUNUSDT", "hyperliquid": "PUMP", "aster": "PUMPUSDT", "grvt": "PUMP_USDT_Perp"}),
    Market(9, "QNT", {"bybit": "QNTUSDT", "aster": "QNTUSDT"}),
    Market(10, "LINK", {"bybit": "LINKUSDT", "hyperliquid": "LINK", "aster": "LINKUSDT", "grvt": "LINK_USDT_Perp"}),
    Market(11, "HBAR", {"bybit": "HBARUSDT", "hyperliquid": "HBAR", "aster": "HBARUSDT", "grvt": "HBAR_USDT_Perp"}),
    Market(12, "SUI", {"bybit": "SUIUSDT", "hyperliquid": "SUI", "aster": "SUIUSDT", "grvt": "SUI_USDT_Perp"}),
    Market(13, "ENA", {"bybit": "ENAUSDT", "hyperliquid": "ENA", "aster": "ENAUSDT", "grvt": "ENA_USDT_Perp"}),
    Market(14, "ONDO", {"bybit": "ONDOUSDT", "hyperliquid": "ONDO", "aster": "ONDOUSDT", "grvt": "ONDO_USDT_Perp"}),
    Market(15, "AVAX", {"bybit": "AVAXUSDT", "hyperliquid": "AVAX", "aster": "AVAXUSDT", "grvt": "AVAX_USDT_Perp"}),
    Market(16, "AAVE", {"bybit": "AAVEUSDT", "hyperliquid": "AAVE", "aster": "AAVEUSDT", "grvt": "AAVE_USDT_Perp"}),
    Market(17, "DOGE", {"bybit": "DOGEUSDT", "hyperliquid": "DOGE", "aster": "DOGEUSDT", "grvt": "DOGE_USDT_Perp"}),
    Market(18, "WLD", {"bybit": "WLDUSDT", "hyperliquid": "WLD", "aster": "WLDUSDT", "grvt": "WLD_USDT_Perp"}),
    Market(19, "UNI", {"bybit": "UNIUSDT", "hyperliquid": "UNI", "aster": "UNIUSDT", "grvt": "UNI_USDT_Perp"}),
    Market(20, "TAO", {"bybit": "TAOUSDT", "hyperliquid": "TAO", "aster": "TAOUSDT", "grvt": "TAO_USDT_Perp"}),
)

MARKETS: Dict[str, Market] = {market.asset: market for market in TOP_MARKETS}
SYMBOL_TO_ASSET: Dict[Tuple[str, str], str] = {
    (venue, symbol): market.asset
    for market in TOP_MARKETS
    for venue, symbol in market.symbols.items()
}


class UnsupportedMarket(ValueError):
    pass


def canonical_asset(venue: str, instrument: str) -> Optional[str]:
    return SYMBOL_TO_ASSET.get((venue, instrument))


def normalize_assets(assets: Iterable[str]) -> List[str]:
    normalized = [asset.upper() for asset in assets]
    unknown = sorted(set(normalized) - set(MARKETS))
    if unknown:
        raise UnsupportedMarket(
            f"unknown assets: {', '.join(unknown)}; choose from {', '.join(MARKETS)}"
        )
    return normalized


def select_market_symbols(
    venues: Sequence[str], assets: Sequence[str], *, skip_unsupported: bool = False
) -> Tuple[Dict[str, List[str]], List[str]]:
    selected: Dict[str, List[str]] = {venue: [] for venue in venues}
    missing: List[str] = []
    for asset in normalize_assets(assets):
        market = MARKETS[asset]
        for venue in venues:
            symbol = market.symbol_for(venue)
            if symbol is None:
                missing.append(f"{venue}:{asset}")
            else:
                selected[venue].append(symbol)
    if missing and not skip_unsupported:
        raise UnsupportedMarket("unsupported venue/asset combinations: " + ", ".join(missing))
    return selected, missing
