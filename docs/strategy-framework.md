# BBO hypothesis framework

## 목표

가설 코드는 `hypotheses/simple_mean_reversion.py`의 `decide()` 함수 하나로
제한한다. 데이터 로딩, 거래소 심볼 변환, 비동기 BBO 정렬, rolling z-score,
포지션, 비용과 결과 집계는 프레임워크가 담당한다.

```text
BBO JSONL
  → canonical asset 변환
  → receive time 기준 event alignment
  → stale quote 제거
  → venue pair별 basis/z-score
  → decide(frame)
  → bid/ask 체결 시뮬레이션
  → JSON 결과
```

## 사용법

```bash
make check

make collect \
  VENUES="bybit aster" \
  SYMBOLS="BTC ETH" \
  DURATION=300 \
  DATA=data/bbo.jsonl

make strategy \
  DATA=data/bbo.jsonl \
  VENUES="bybit aster" \
  SYMBOLS="BTC" \
  HYPOTHESIS=hypotheses/simple_mean_reversion.py \
  WINDOW=120 \
  FRESHNESS_MS=1000 \
  FEE_BPS=5 \
  RESULT=data/result.json
```

`VENUES`를 3개 이상 지정하면 모든 2개 거래소 조합을 독립적으로 평가한다.

## 가설 입력

`HypothesisInput`에는 canonical asset과 두 거래소, 현재 basis와 rolling
z-score, sample 수, 현재 포지션 방향, 보유 시간과 receive timestamp가
제공된다. `Action.LONG_LEFT`, `SHORT_LEFT`, `CLOSE`, `HOLD` 중 하나를 반환한다.

기본 가설은 `|z| >= 2.5`에서 비싼 venue를 매도하고 싼 venue를 매수한다.
`|z| <= 0.5` 또는 300초가 지나면 청산한다.

## 현재 시뮬레이터의 범위

- 진입과 청산 모두 관측된 bid/ask에서 즉시 전량 체결된다고 가정
- `fee_bps`는 fill 한 번의 taker fee 가정
- funding, slippage, latency, partial fill, portfolio margin은 아직 미포함
- 여러 venue pair는 독립적으로 거래하므로 venue별 총 exposure 제약은 미포함
- 결과는 수익 보장이 아닌 가설 비교용 연구 결과

따라서 이 단계의 결과로 실거래를 결정하면 안 된다. 다음 확장 순서는 latency,
top-of-book size, funding, partial fill, portfolio risk 순이다.
