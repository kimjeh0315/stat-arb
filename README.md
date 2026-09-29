# STAT-arbitrage

## 실행
Python 3.9+ 환경에서:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
bbo-probe --duration 60 --symbol BTC --output data/bbo.jsonl
```

일부 거래소만 

```bash
bbo-probe --venues bybit hyperliquid --duration 30
```

## 코드 구조

```text
src/bbo_probe/
├── cli.py                 # CLI 인자와 거래소별 심볼 매핑
├── collector.py           # WebSocket 수명주기, heartbeat, 재연결, JSONL 저장
├── models.py              # 정규화된 BBO 모델
└── exchanges/
    ├── base.py            # 거래소 adapter 계약과 공통 파싱 도구
    ├── bybit.py           # V5 linear orderbook.1
    ├── hyperliquid.py     # block 기반 bbo feed
    ├── aster.py           # futures bookTicker
    └── grvt.py            # JSON-RPC v1.book.s snapshot
```

## 거래소별 현재 설정

| 거래소 | public endpoint / feed | 현재 특성 |
|---|---|---|
| Bybit | linear `orderbook.1` | 10ms 표기, L1 snapshot-only, 20초 JSON ping, 3초 무변경 시 동일 update 재전송 가능 |
| Hyperliquid | `bbo` | BBO가 block에서 바뀔 때 전송, 한쪽이 없으면 null 가능, 60초 무전송 시 연결 종료 |
| Aster | futures `bookTicker` | BBO 변경 실시간 전송, protocol ping/pong, 연결 수명 24시간 |
| GRVT | `v1.book.s`, `@500-10` | 500ms snapshot, 나노초 event time, sequence/prev_sequence 제공 |

[Bybit orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook), [Hyperliquid subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), [Aster futures streams](https://asterdex.github.io/aster-api-website/futures/websocket-market-streams/), [GRVT market-data streams](https://api-docs.grvt.io/market_data_streams/).
