# Quant — cross-venue BBO probe

Bybit, Hyperliquid, Aster, GRVT의 USDT perpetual BBO를 같은 JSONL 스키마로 수집하고 초기 데이터 품질을 확인하는 프로토타입이다. 주문/인증 기능은 포함하지 않는다.

## 바로 실행

Python 3.9+ 환경에서:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
bbo-probe --duration 60 --symbol BTC --output data/bbo.jsonl
```

일부 거래소만 확인할 수도 있다.

```bash
bbo-probe --venues bybit hyperliquid --duration 30
```

각 줄에는 거래소/상품, bid·ask 가격과 수량, 거래소 이벤트 시각, 로컬 wall/monotonic 수신시각, sequence, spread와 기본 유효성 결과가 저장된다. 종료 요약에는 메시지 수, 재연결 수, sequence 중복·역행·gap, crossed/invalid quote 수와 event-age/inter-arrival p50·p95·p99가 나온다.

## 초기 단계에서 반드시 확인할 것

1. **상품 동질성** — 현재 기본값은 BTC-USDT perpetual이다. contract multiplier, quote/settlement asset, tick/lot size, funding 시간과 index 구성을 별도 instrument master로 고정해야 한다.
2. **시간 의미와 clock sync** — `event_age_ms`는 거래소 clock과 로컬 clock 차이를 포함한다. 수집 호스트를 NTP/PTP로 동기화하고 `recv_mono_ns`로 수신 간격을 따로 측정한다.
3. **feed 의미** — Bybit/Aster는 실시간 top-of-book, Hyperliquid BBO는 block마다 변경 시, GRVT snapshot은 최소 500ms다. 같은 freshness SLA를 적용하지 않는다.
4. **정합성** — bid ≤ ask, 양수 가격/수량, stale quote, sequence 역행/중복/gap, snapshot 재초기화를 기록한다. sequence 규칙은 거래소별이며 단순 `+1` 규칙을 공통 적용하면 안 된다.
5. **장애 복구** — 강제 disconnect, DNS 실패, 24시간 연결 만료(Aster), heartbeat 누락, reconnect 중 데이터 공백을 시험한다. reconnect 직후 quote는 새 snapshot 전까지 사용하지 않는다.
6. **저장 성능** — 이 프로토타입의 매 메시지 flush는 진단 우선 설계다. 장시간/다심볼 수집 전에는 bounded queue, batch write, Parquet partition과 backpressure/drop counter가 필요하다.
7. **전략 관측치** — 단순 mid spread가 아니라 `venue A ask`에서 사고 `venue B bid`에 파는 executable spread에서 taker fee, funding, slippage, transfer/재고 비용을 차감한다. 양쪽 quote의 최대 시각 차이와 staleness limit도 신호에 포함한다.

## 거래소별 현재 설정

| 거래소 | public endpoint / feed | 현재 특성 |
|---|---|---|
| Bybit | linear `orderbook.1` | 10ms 표기, L1 snapshot-only, 3초 무변경 시 동일 update 재전송 가능 |
| Hyperliquid | `bbo` | BBO가 block에서 바뀔 때 전송, 한쪽이 없으면 null 가능 |
| Aster | futures `bookTicker` | BBO 변경 실시간 전송, 연결 수명 24시간 |
| GRVT | `v1.book.s`, `@500-10` | 500ms snapshot, sequence/prev_sequence 제공 |

## 다음 합격 기준(권장)

- 24시간 연속 수집에서 각 venue의 예상치 못한 무수신 구간과 reconnect 원인이 모두 설명될 것
- crossed/invalid quote 0건, GRVT sequence gap 0건(강제 장애 구간 제외)
- 로컬 clock offset을 측정한 뒤 venue별 event-to-receive 지연 p50/p95/p99 산출
- 동일 상품 metadata와 fee schedule snapshot을 버전 관리
- 2개 이상 지역/VPS에서 동일 probe를 돌려 네트워크 경로에 따른 지연을 비교

공식 사양 링크: [Bybit orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook), [Hyperliquid subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), [Aster futures streams](https://asterdex.github.io/aster-api-website/futures/websocket-market-streams/), [GRVT market-data streams](https://api-docs.grvt.io/market_data_streams/).
