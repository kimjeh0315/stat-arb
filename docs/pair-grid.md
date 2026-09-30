# Pair-trading grid simulation

이 스크립트는 같은 전략 파라미터를 지정한 모든 `자산 × 거래소 쌍`에 적용한다.
예를 들어 자산 5개와 거래소 4개를 지정하면 `5 × C(4, 2) = 30`개 조합을 한
번의 JSONL 스캔으로 평가한다.

## 1. 데이터 수집

먼저 비교할 모든 거래소와 자산의 BBO가 같은 파일에 있어야 한다.

```bash
make collect \
  VENUES="bybit hyperliquid aster grvt" \
  SYMBOLS="BTC ETH SOL XRP DOGE" \
  DURATION=3600 \
  DATA=data/pair-grid-bbo.jsonl
```

상위 50개 전체를 수집하려면 다음 명령을 사용할 수 있지만, 지원되지 않는
거래소/자산 조합은 자동으로 제외되고 stderr에 표시된다.

```bash
make collect-top50 DURATION=3600 DATA=data/top50-bbo.jsonl
```

## 2. 기본 grid 실행

```bash
make strategy-grid \
  DATA=data/pair-grid-bbo.jsonl \
  VENUES="bybit hyperliquid aster grvt" \
  SYMBOLS="BTC ETH SOL XRP DOGE" \
  WINDOW=120 \
  FRESHNESS_MS=1000 \
  FEE_BPS=5 \
  MIN_TRADES=5 \
  RESULT=data/pair-grid-result.json
```

기본값은 기존 BTC Bybit–GRVT 가설과 동일하다.

- warm-up/window: 120 aligned samples
- entry: `|z| >= 2.5`, rolling 평균 대비 괴리 15bp 이상
- exit: 방향별 `z = ±0.35` 복귀
- stop: `|z| >= 4.5`
- maximum holding time: 180초

임계값까지 바꾸려면 스크립트를 직접 실행한다.

```bash
.venv/bin/python scripts/simulate_pair_grid.py \
  --data data/pair-grid-bbo.jsonl \
  --venues bybit hyperliquid aster grvt \
  --symbols BTC ETH SOL XRP DOGE \
  --window 240 \
  --warmup-samples 120 \
  --entry-z 2.75 \
  --min-deviation-bps 18 \
  --exit-z 0.25 \
  --stop-z 4.5 \
  --max-hold-seconds 180 \
  --freshness-ms 1000 \
  --fee-bps 5 \
  --min-trades 10 \
  --output data/pair-grid-result.json
```

`window`는 `warmup-samples` 이상이어야 한다. `fee-bps`를 바꾸면 비용 필터인
`min-deviation-bps`도 함께 검토해야 한다.

## 3. 결과 해석

결과 JSON의 `pairs`에는 요청한 모든 조합이 들어간다.

- `status=no_aligned_data`: 양쪽 거래소의 신선한 BBO가 한 번도 정렬되지 않음
- `status=insufficient_warmup`: 표본이 warm-up보다 적음
- `status=no_signal`: 표본은 충분하지만 진입 조건을 만족하지 않음
- `status=traded`: 한 번 이상 포지션을 열었음
- `samples`: freshness 필터를 통과한 의사결정 횟수. 독립적인 통계 표본 수는 아님
- `trades`: 청산까지 완료된 pair trade 수
- `gross_bps`: 비용 차감 전 거래별 수익률 합계
- `net_bps`: 설정한 taker 비용을 차감한 거래별 수익률 합계
- `average_net_bps`: 거래 한 건당 평균 순수익률
- `win_rate`: `net_bps > 0`인 거래 비율
- `max_drawdown_bps`: 해당 조합의 거래별 누적 순수익 곡선에서 최대 낙폭

`ranking`은 `trades >= min-trades`인 조합만 `net_bps` 순으로 정렬한 편의 목록이다.
따라서 ranking에 없다고 손실 조합이라는 뜻은 아니며, `pairs`의 status를 함께
확인해야 한다. `net_bps`는 투자 금액을 반영한 손익이나 연환산 수익률이 아니다.

`BTC:bybit-grvt`처럼 앞 거래소가 left다. basis가 양수이고 z-score가 높으면
left를 매도하고 right를 매수하며, 음수이면 반대로 거래한다.

## 4. 비교할 때 지켜야 할 기준

조합 수가 늘수록 우연히 좋아 보이는 결과가 반드시 생긴다. 전체 기간의 최고
`net_bps`만 골라 실거래 대상으로 삼으면 안 된다.

1. 시간순으로 train/validation/test 기간을 분리한다.
2. 파라미터 선택은 train에서만 하고 validation에서 후보 수를 줄인다.
3. 최종 test 기간은 한 번만 평가한다.
4. 최소 거래 수, 최대 낙폭, 평균 거래 수익이 수수료 변화에도 유지되는지 본다.
5. 서로 다른 변동성 구간과 날짜에서도 같은 방향의 결과가 나는지 확인한다.
6. 거래소별 실제 taker fee, funding, latency와 주문 가능 수량을 추가한 뒤 판단한다.

현재 엔진은 BBO 즉시 전량 체결을 가정하고 funding, slippage, latency, partial
fill, venue별 exposure 한도를 포함하지 않는다. 결과는 후보를 탈락시키는 연구
도구로 해석해야 하며, 그대로 실거래 기대수익으로 해석하면 안 된다.
