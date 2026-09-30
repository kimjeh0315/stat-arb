# STAT-arbitrage

## 실행
Python 3.9+ 환경에서:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
bbo-probe --duration 60 --symbols BTC --output data/bbo.jsonl
```

거래소와 자산을 선택하거나 24시간 거래대금 상위 50개 perpetual을 거래소당 한 WebSocket으로
묶어 수집할 수 있다.

```bash
make collect VENUES="bybit aster" SYMBOLS="BTC ETH SOL" DURATION=300
make collect-top50 VENUES="bybit hyperliquid aster grvt" DURATION=300
```

가설은 [simple_mean_reversion.py](hypotheses/simple_mean_reversion.py)의 짧은
`decide()` 함수만 수정하고 같은 프레임워크에서 실행한다.

```bash
make strategy DATA=data/bbo.jsonl VENUES="bybit aster" SYMBOLS="BTC"
```

시장 목록과 프레임워크 설명은 [markets.md](docs/markets.md),
[strategy-framework.md](docs/strategy-framework.md)에 정리되어 있다.

## 코드 구조

```text
src/bbo_probe/
├── cli.py                 # CLI 인자와 거래소별 심볼 매핑
├── collector.py           # WebSocket 수명주기, heartbeat, 재연결, JSONL 저장
├── models.py              # 정규화된 BBO 모델
├── markets.py             # canonical asset과 거래소별 심볼
├── strategy/              # 정렬, feature, 체결 시뮬레이션과 결과 집계
└── exchanges/
    ├── base.py            # 거래소 adapter 계약과 공통 파싱 도구
    ├── bybit.py           # V5 linear orderbook.1
    ├── hyperliquid.py     # block 기반 bbo feed
    ├── aster.py           # futures bookTicker
    └── grvt.py            # JSON-RPC v1.book.d + local order book
```

## 거래소별 현재 설정

| 거래소 | public endpoint / feed | 현재 특성 |
|---|---|---|
| Bybit | linear `orderbook.1` | 10ms 표기, L1 snapshot-only, 20초 JSON ping, 3초 무변경 시 동일 update 재전송 가능 |
| Hyperliquid | `bbo` | BBO가 block에서 바뀔 때 전송, 한쪽이 없으면 null 가능, 60초 무전송 시 연결 종료 |
| Aster | futures `bookTicker` | BBO 변경 실시간 전송, protocol ping/pong, 연결 수명 24시간 |
| GRVT | `v1.book.d`, `@50` | 최초 full snapshot 후 50ms delta, 로컬 book 반영, gap 시 재연결 |

[Bybit orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook), [Hyperliquid subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), [Aster futures streams](https://asterdex.github.io/aster-api-website/futures/websocket-market-streams/), [GRVT market-data streams](https://api-docs.grvt.io/market_data_streams/).
