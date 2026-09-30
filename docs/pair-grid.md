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
  --hypothesis hypotheses/cross_venue_pair_grid.py \
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

## 3. 더 많은 자산 실행

거래대금 순위 상위 N개를 일일이 입력하지 않고 선택할 수 있다. 단, 입력 JSONL에
해당 자산 데이터가 실제로 수집되어 있어야 한다.

```bash
make strategy-grid-top \
  DATA=data/top50-bbo.jsonl \
  TOP=20 \
  VENUES="bybit hyperliquid aster grvt" \
  RESULT=data/top20-grid-result.json
```

`TOP`은 1~50을 지원한다. 특정 자산 묶음을 비교할 때는 기존 `strategy-grid`의
`SYMBOLS`를 사용한다. 자산 수가 N이고 거래소 수가 V이면 평가 조합 수는
`N × V × (V-1) / 2`다.

## 4. 파라미터 sweep

기본 조건에서 win이 없다면 먼저 `pairs`의 status를 확인한다. 데이터가 충분한데
`no_signal`이면 진입 z 또는 최소 괴리가 너무 엄격할 수 있고, 거래는 있지만
win이 없다면 수수료·청산 기준·보유시간을 검토해야 한다.

다음 명령은 18개 조합(`entry 3 × deviation 3 × exit 2`)을 순차 평가한다.

```bash
make strategy-sweep-top \
  DATA=data/top50-bbo.jsonl \
  TOP=20 \
  VENUES="bybit hyperliquid aster grvt" \
  MIN_TRADES=5 \
  MAX_RUNS=100 \
  RESULT=data/pair-sweep.json
```

이 기본 sweep 범위는 실행 스크립트가 아니라
`hypotheses/cross_venue_pair_grid.py`의 `HYPOTHESIS.sweep`에 기록되어 있다.
일회성으로 범위를 바꾸려면 `SWEEP_ARGS`만 추가한다.

```bash
make strategy-sweep-top \
  DATA=data/top50-bbo.jsonl \
  TOP=20 \
  RESULT=data/custom-sweep.json \
  SWEEP_ARGS="--windows 120 240 --entry-z 1.5 1.75 2.0 --min-deviation-bps 8 10 --max-hold-seconds 180 300"
```

특정 자산만 sweep하려면 `strategy-sweep-top` 대신 다음처럼 실행한다.

```bash
make strategy-sweep \
  DATA=data/pair-grid-bbo.jsonl \
  VENUES="bybit hyperliquid aster grvt" \
  SYMBOLS="BTC ETH SOL XRP DOGE" \
  RESULT=data/pair-sweep.json
```

`MAX_RUNS`는 실수로 지나치게 큰 Cartesian product를 실행하는 것을 막는다.
각 run은 입력 파일을 한 번씩 읽으므로 총 시간은 대략 단일 grid 실행 시간에
run 수를 곱한 값이다.

처음에는 한 번에 한 축만 바꾸는 것이 좋다.

1. 거래가 없으면 `entry-z`: 2.5 → 2.0 → 1.75
2. 여전히 없으면 `min-deviation-bps`: 15 → 10
3. 포지션이 잘 닫히지 않으면 `exit-z`: 0.35 → 0.75 또는 보유시간 180 → 300초
4. `window`: 120, 240처럼 신호의 시간 척도를 따로 비교

기본 fee가 fill당 5bp이면 엔진의 pair 왕복비용은 10bp다. 따라서 5bp deviation은
민감도 확인용일 뿐이며, 비용 차감 후 수익 가설로 채택하기에는 지나치게 낮을 수
있다. win 수를 만들기 위해 비용보다 작은 신호를 선택해서는 안 된다.

Sweep 결과의 `runs`는 전체 `net_bps` 순으로 정렬되며 각 항목에 다음이 포함된다.

- `variant`: 해당 run의 `variant_id`, window와 모든 유효 파라미터
- `summary.wins`, `losses`, `trades`, `win_rate`, `net_bps`
- `summary.profitable_pairs`: 최소 거래 수를 넘고 순손익이 양수인 pair 수
- `pair_statuses`: 데이터 없음, warm-up 부족, 무신호, 거래 발생 조합 수
- `top_pairs`: 해당 run에서 상위 pair 목록

## 5. Hypothesis와 결과 매칭

모든 실행 가능한 가설은 `hypotheses/` 아래 Python 파일로 정의하며
`HYPOTHESIS = PairHypothesis(...)`를 export한다. 공통 진입·청산 구현은
`src/bbo_probe/strategy/pair_rules.py` 한 곳에만 있고, hypothesis 파일에는 가설의
ID, 버전, 설명, 적용 범위, 기본 파라미터와 권장 sweep 범위만 선언한다.

현재 grid 가설은 `hypotheses/cross_venue_pair_grid.py`다. 다른 연구 가설을
추가할 때는 파일을 복사해 최소한 다음을 바꾼다.

- `hypothesis_id`: 중복되지 않는 lowercase snake_case ID
- `version`: 가설 의미나 기본 파라미터가 바뀌면 증가
- `title`, `thesis`: 검증할 명제
- `window`, `parameters`, `sweep`
- 필요하면 `assets`, `venue_pairs`로 적용 범위 제한

실행할 파일은 `GRID_HYPOTHESIS`로 명시한다.

```bash
make strategy-sweep-top \
  GRID_HYPOTHESIS=hypotheses/cross_venue_pair_grid.py \
  DATA=data/top50-bbo.jsonl TOP=20 \
  RESULT=data/pair-sweep.json
```

단일 grid 결과의 `hypothesis`, sweep 결과의 최상위 `hypothesis`에는 다음이
기록된다.

- `id`, `version`, `title`, `thesis`
- `source`: 사용한 hypothesis 파일
- `source_sha256`: 실행 당시 파일 내용의 해시
- `scope`: 허용 자산과 거래소 pair
- `sweep_defaults`: hypothesis가 선언한 비교 범위

각 실행 결과에는 `variant_id`와 실제 window/파라미터가 별도로 기록된다. 같은
ID와 버전이더라도 파일이 바뀌면 `source_sha256`이 달라지고, 파라미터나 window가
바뀌면 `variant_id`가 달라진다. 따라서 결과를 해석할 때 파일 이름만 비교하지
말고 이 두 값을 함께 확인해야 한다.

## 6. 단일 grid 결과 해석

결과 JSON의 `pairs`에는 요청한 모든 조합이 들어간다.

- `status=no_aligned_data`: 양쪽 거래소의 신선한 BBO가 한 번도 정렬되지 않음
- `status=outside_hypothesis_scope`: 가설이 해당 자산/거래소 pair를 대상으로 하지 않음
- `status=insufficient_warmup`: 표본이 warm-up보다 적음
- `status=no_signal`: 표본은 충분하지만 진입 조건을 만족하지 않음
- `status=traded`: 한 번 이상 포지션을 열었음
- `samples`: freshness 필터를 통과한 의사결정 횟수. 독립적인 통계 표본 수는 아님
- `trades`: 청산까지 완료된 pair trade 수
- `wins`, `losses`: 비용 차감 후 양수/비양수 거래 횟수
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

## 7. 비교할 때 지켜야 할 기준

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
