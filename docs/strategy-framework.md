# BBO hypothesis framework

## 목표

가설 코드는 `hypotheses/*.py`의 `decide()` 함수 하나로 제한한다. 데이터 로딩,
거래소 심볼 변환, 비동기 BBO 정렬, rolling 평균·표준편차·z-score, 포지션,
비용과 결과 집계는 프레임워크가 담당한다.

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
  VENUES="bybit grvt" \
  SYMBOLS="BTC" \
  HYPOTHESIS=hypotheses/btc_bybit_grvt_pair.py \
  WINDOW=120 \
  FRESHNESS_MS=1000 \
  FEE_BPS=5 \
  RESULT=data/result.json
```

`VENUES`를 3개 이상 지정하면 모든 2개 거래소 조합을 독립적으로 평가한다.

## 가설 입력

`HypothesisInput`에는 canonical asset과 두 거래소, 현재 basis, rolling
평균·표준편차·평균 대비 괴리·z-score, sample 수, 현재 포지션 방향, 보유 시간과
receive timestamp가 제공된다. `Action.LONG_LEFT`, `SHORT_LEFT`, `CLOSE`,
`HOLD` 중 하나를 반환한다.

## 구현된 BTC Bybit–GRVT pair 가설

가설은 두 거래소의 BTC log-mid basis가 짧은 구간에서 정상적(stationary)이고,
일시적인 유동성 충격으로 생긴 큰 괴리가 rolling 평균으로 돌아온다는 것이다.

`basis = 10,000 × log(Bybit mid / GRVT mid)`로 정의하고 120개 동기화 표본을
사용한다.

- 진입: `|z| >= 2.5`이면서 평균 대비 괴리가 15bp 이상
- 방향: 양의 z이면 Bybit 매도/GRVT 매수, 음의 z이면 반대
- 이익 실현: short-left는 `z <= 0.35`, long-left는 `z >= -0.35`
- 손절: 진입 뒤 괴리가 `|z| >= 4.5`로 더 확대
- 시간 청산: 180초
- 대상 제한: BTC의 Bybit–GRVT 조합만 거래

15bp 필터는 기본 설정의 예상 왕복 비용 10bp와 추가 5bp 완충값이다. 실제
수수료 설정이 달라지면 hypothesis의 `MIN_DEVIATION_BPS`도 함께 조정해야 한다.
이 임계값들은 사전 가설이며 수집 데이터에 맞춰 사후 최적화한 값이 아니다.

`simple_mean_reversion.py`는 여러 자산과 venue pair를 빠르게 실험하기 위한
범용 예제로 남겨 둔다.

## 현재 시뮬레이터의 범위

- 진입과 청산 모두 관측된 bid/ask에서 즉시 전량 체결된다고 가정
- `fee_bps`는 fill 한 번의 taker fee 가정
- funding, slippage, latency, partial fill, portfolio margin은 아직 미포함
- 여러 venue pair는 독립적으로 거래하므로 venue별 총 exposure 제약은 미포함
- 결과는 수익 보장이 아닌 가설 비교용 연구 결과

따라서 이 단계의 결과로 실거래를 결정하면 안 된다. 다음 확장 순서는 latency,
top-of-book size, funding, partial fill, portfolio risk 순이다.
