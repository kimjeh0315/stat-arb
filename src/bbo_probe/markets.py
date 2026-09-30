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


# Aggregated 24h perpetual quote/notional turnover from all four venues, sampled
# 2026-09-30 10:50--11:09 UTC. Equivalent multiplier contracts are grouped;
# tokenized equities, leveraged ETFs and commodities are excluded.
TOP_MARKETS: Tuple[Market, ...] = (
    Market(1, "BTC", {"bybit": "BTCUSDT", "hyperliquid": "BTC", "aster": "BTCUSDT", "grvt": "BTC_USDT_Perp"}),
    Market(2, "ETH", {"bybit": "ETHUSDT", "hyperliquid": "ETH", "aster": "ETHUSDT", "grvt": "ETH_USDT_Perp"}),
    Market(3, "SOL", {"bybit": "SOLUSDT", "hyperliquid": "SOL", "aster": "SOLUSDT", "grvt": "SOL_USDT_Perp"}),
    Market(4, "ZEC", {"bybit": "ZECUSDT", "hyperliquid": "ZEC", "aster": "ZECUSDT", "grvt": "ZEC_USDT_Perp"}),
    Market(5, "XRP", {"bybit": "XRPUSDT", "hyperliquid": "XRP", "aster": "XRPUSDT", "grvt": "XRP_USDT_Perp"}),
    Market(6, "NEAR", {"bybit": "NEARUSDT", "hyperliquid": "NEAR", "aster": "NEARUSDT", "grvt": "NEAR_USDT_Perp"}),
    Market(7, "HYPE", {"bybit": "HYPEUSDT", "hyperliquid": "HYPE", "aster": "HYPEUSDT", "grvt": "HYPE_USDT_Perp"}),
    Market(8, "PUMP", {"bybit": "PUMPFUNUSDT", "hyperliquid": "PUMP", "aster": "PUMPUSDT", "grvt": "PUMP_USDT_Perp"}),
    Market(9, "LIT", {"bybit": "LITUSDT", "hyperliquid": "LIT", "aster": "LITUSDT", "grvt": "LIT_USDT_Perp"}),
    Market(10, "QNT", {"bybit": "QNTUSDT", "aster": "QNTUSDT"}),
    Market(11, "ENA", {"bybit": "ENAUSDT", "hyperliquid": "ENA", "aster": "ENAUSDT", "grvt": "ENA_USDT_Perp"}),
    Market(12, "AAVE", {"bybit": "AAVEUSDT", "hyperliquid": "AAVE", "aster": "AAVEUSDT", "grvt": "AAVE_USDT_Perp"}),
    Market(13, "SUI", {"bybit": "SUIUSDT", "hyperliquid": "SUI", "aster": "SUIUSDT", "grvt": "SUI_USDT_Perp"}),
    Market(14, "DOGE", {"bybit": "DOGEUSDT", "hyperliquid": "DOGE", "aster": "DOGEUSDT", "grvt": "DOGE_USDT_Perp"}),
    Market(15, "WLD", {"bybit": "WLDUSDT", "hyperliquid": "WLD", "aster": "WLDUSDT", "grvt": "WLD_USDT_Perp"}),
    Market(16, "AVAX", {"bybit": "AVAXUSDT", "hyperliquid": "AVAX", "aster": "AVAXUSDT", "grvt": "AVAX_USDT_Perp"}),
    Market(17, "HBAR", {"bybit": "HBARUSDT", "hyperliquid": "HBAR", "aster": "HBARUSDT", "grvt": "HBAR_USDT_Perp"}),
    Market(18, "ONDO", {"bybit": "ONDOUSDT", "hyperliquid": "ONDO", "aster": "ONDOUSDT", "grvt": "ONDO_USDT_Perp"}),
    Market(19, "LINK", {"bybit": "LINKUSDT", "hyperliquid": "LINK", "aster": "LINKUSDT", "grvt": "LINK_USDT_Perp"}),
    Market(20, "ASTER", {"bybit": "ASTERUSDT", "hyperliquid": "ASTER", "aster": "ASTERUSDT", "grvt": "ASTER_USDT_Perp"}),
    Market(21, "TAO", {"bybit": "TAOUSDT", "hyperliquid": "TAO", "aster": "TAOUSDT", "grvt": "TAO_USDT_Perp"}),
    Market(22, "PONS", {"bybit": "PONSUSDT", "hyperliquid": "PONS", "aster": "PONSUSDT"}),
    Market(23, "GRASS", {"bybit": "GRASSUSDT", "hyperliquid": "GRASS", "aster": "GRASSUSDT"}),
    Market(24, "BNB", {"bybit": "BNBUSDT", "hyperliquid": "BNB", "aster": "BNBUSDT", "grvt": "BNB_USDT_Perp"}),
    Market(25, "UNI", {"bybit": "UNIUSDT", "hyperliquid": "UNI", "aster": "UNIUSDT", "grvt": "UNI_USDT_Perp"}),
    Market(26, "PEPE", {"bybit": "1000PEPEUSDT", "hyperliquid": "kPEPE", "aster": "1000PEPEUSDT"}),
    Market(27, "ADA", {"bybit": "ADAUSDT", "hyperliquid": "ADA", "aster": "ADAUSDT", "grvt": "ADA_USDT_Perp"}),
    Market(28, "XPL", {"bybit": "XPLUSDT", "hyperliquid": "XPL", "aster": "XPLUSDT", "grvt": "XPL_USDT_Perp"}),
    Market(29, "ZRO", {"bybit": "ZROUSDT", "hyperliquid": "ZRO", "aster": "ZROUSDT"}),
    Market(30, "ARB", {"bybit": "ARBUSDT", "hyperliquid": "ARB", "aster": "ARBUSDT", "grvt": "ARB_USDT_Perp"}),
    Market(31, "SOON", {"bybit": "SOONUSDT", "aster": "SOONUSDT"}),
    Market(32, "XLM", {"bybit": "XLMUSDT", "hyperliquid": "XLM", "aster": "XLMUSDT", "grvt": "XLM_USDT_Perp"}),
    Market(33, "CRV", {"bybit": "CRVUSDT", "hyperliquid": "CRV", "aster": "CRVUSDT", "grvt": "CRV_USDT_Perp"}),
    Market(34, "FARTCOIN", {"bybit": "FARTCOINUSDT", "hyperliquid": "FARTCOIN", "aster": "FARTCOINUSDT", "grvt": "FARTCOIN_USDT_Perp"}),
    Market(35, "MOVR", {"bybit": "MOVRUSDT", "aster": "MOVRUSDT"}),
    Market(36, "LTC", {"bybit": "LTCUSDT", "hyperliquid": "LTC", "aster": "LTCUSDT", "grvt": "LTC_USDT_Perp"}),
    Market(37, "0G", {"bybit": "0GUSDT", "hyperliquid": "0G", "aster": "0GUSDT"}),
    Market(38, "BONK", {"bybit": "1000BONKUSDT", "hyperliquid": "kBONK", "aster": "1000BONKUSDT"}),
    Market(39, "GRAM", {"bybit": "GRAMUSDT", "hyperliquid": "GRAM", "aster": "GRAMUSDT"}),
    Market(40, "BTW", {"bybit": "BTWUSDT", "aster": "BTWUSDT"}),
    Market(41, "PENGU", {"bybit": "PENGUUSDT", "hyperliquid": "PENGU", "aster": "PENGUUSDT", "grvt": "PENGU_USDT_Perp"}),
    Market(42, "ICP", {"bybit": "ICPUSDT", "hyperliquid": "ICP", "aster": "ICPUSDT", "grvt": "ICP_USDT_Perp"}),
    Market(43, "TRUMP", {"bybit": "TRUMPUSDT", "hyperliquid": "TRUMP", "aster": "TRUMPUSDT", "grvt": "TRUMP_USDT_Perp"}),
    Market(44, "CASHCAT", {"bybit": "CASHCATUSDT", "hyperliquid": "CASHCAT", "aster": "CASHCATUSDT"}),
    Market(45, "USELESS", {"bybit": "USELESSUSDT", "hyperliquid": "USELESS", "aster": "USELESSUSDT"}),
    Market(46, "VIRTUAL", {"bybit": "VIRTUALUSDT", "hyperliquid": "VIRTUAL", "aster": "VIRTUALUSDT", "grvt": "VIRTUAL_USDT_Perp"}),
    Market(47, "US", {"bybit": "USUSDT", "aster": "USUSDT"}),
    Market(48, "ETHFI", {"bybit": "ETHFIUSDT", "hyperliquid": "ETHFI", "aster": "ETHFIUSDT"}),
    Market(49, "JUP", {"bybit": "JUPUSDT", "hyperliquid": "JUP", "aster": "JUPUSDT", "grvt": "JUP_USDT_Perp"}),
    Market(50, "MON", {"bybit": "MONUSDT", "hyperliquid": "MON", "aster": "MONUSDT", "grvt": "MON_USDT_Perp"}),
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
