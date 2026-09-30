# Perpetual market universe

이 목록은 2026-09-29 16:15 UTC에 Bybit `turnover24h`, Hyperliquid
`dayNtlVlm`, Aster `quoteVolume`을 USD/USDT notional로 간주해 합산한 뒤,
토큰화 주식·ETF·원자재를 제외한 상위 20개 crypto perpetual이다. 24시간
거래량은 계속 변하므로 영구적인 순위가 아니다.

GRVT는 순위 산출에 포함하지 않고 active perpetual 목록을 이용해 지원 여부만
검증했다. `QNT`는 조사 시점에 Hyperliquid와 GRVT에서 지원되지 않았다.

| 순위 | 자산 | Bybit | Hyperliquid | Aster | GRVT |
|---:|---|---|---|---|---|
| 1 | BTC | `BTCUSDT` | `BTC` | `BTCUSDT` | `BTC_USDT_Perp` |
| 2 | ETH | `ETHUSDT` | `ETH` | `ETHUSDT` | `ETH_USDT_Perp` |
| 3 | ZEC | `ZECUSDT` | `ZEC` | `ZECUSDT` | `ZEC_USDT_Perp` |
| 4 | SOL | `SOLUSDT` | `SOL` | `SOLUSDT` | `SOL_USDT_Perp` |
| 5 | XRP | `XRPUSDT` | `XRP` | `XRPUSDT` | `XRP_USDT_Perp` |
| 6 | HYPE | `HYPEUSDT` | `HYPE` | `HYPEUSDT` | `HYPE_USDT_Perp` |
| 7 | NEAR | `NEARUSDT` | `NEAR` | `NEARUSDT` | `NEAR_USDT_Perp` |
| 8 | PUMP | `PUMPFUNUSDT` | `PUMP` | `PUMPUSDT` | `PUMP_USDT_Perp` |
| 9 | QNT | `QNTUSDT` | — | `QNTUSDT` | — |
| 10 | LINK | `LINKUSDT` | `LINK` | `LINKUSDT` | `LINK_USDT_Perp` |
| 11 | HBAR | `HBARUSDT` | `HBAR` | `HBARUSDT` | `HBAR_USDT_Perp` |
| 12 | SUI | `SUIUSDT` | `SUI` | `SUIUSDT` | `SUI_USDT_Perp` |
| 13 | ENA | `ENAUSDT` | `ENA` | `ENAUSDT` | `ENA_USDT_Perp` |
| 14 | ONDO | `ONDOUSDT` | `ONDO` | `ONDOUSDT` | `ONDO_USDT_Perp` |
| 15 | AVAX | `AVAXUSDT` | `AVAX` | `AVAXUSDT` | `AVAX_USDT_Perp` |
| 16 | AAVE | `AAVEUSDT` | `AAVE` | `AAVEUSDT` | `AAVE_USDT_Perp` |
| 17 | DOGE | `DOGEUSDT` | `DOGE` | `DOGEUSDT` | `DOGE_USDT_Perp` |
| 18 | WLD | `WLDUSDT` | `WLD` | `WLDUSDT` | `WLD_USDT_Perp` |
| 19 | UNI | `UNIUSDT` | `UNI` | `UNIUSDT` | `UNI_USDT_Perp` |
| 20 | TAO | `TAOUSDT` | `TAO` | `TAOUSDT` | `TAO_USDT_Perp` |

코드의 canonical symbol은 첫 번째 열의 자산명이다. 거래소별 실제 심볼은
`src/bbo_probe/markets.py`에서 변환한다.

```bash
make markets
make collect VENUES="bybit aster" SYMBOLS="BTC ETH SOL" DURATION=300
make collect-top20 VENUES="bybit hyperliquid aster grvt" DURATION=300
```

명시한 조합이 지원되지 않으면 `collect`는 오류를 반환한다. `collect-top20`은
지원하지 않는 조합을 stderr에 표시하고 나머지만 수집한다.

공식 데이터 정의:

- [Bybit tickers](https://bybit-exchange.github.io/docs/v5/market/tickers)
- [Hyperliquid perpetual metadata and asset contexts](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)
- [Aster futures market data](https://asterdex.github.io/aster-api-website/futures/market-data/)
- [GRVT instruments API](https://api-docs.grvt.io/market_data_api/)
