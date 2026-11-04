# Subsecond z-score 연구 (Rust)

PR [#3 Feat: stat arb framework](https://github.com/KFAC-2026FALL/stat-arb/pull/3)의 cross-venue log-mid basis와 parameter sweep을 현재 Rust/S3 데이터에 연결한 독립 실행 파일입니다. **1초 analysis 데이터는 읽지 않습니다.**

## 설정 파일로 짧게 실행

요청 구간의 예시는 `config/zscore-oct20-30.json`입니다. **2026-10-20 00:00 UTC부터 2026-10-30 00:00 UTC 직전까지** Bybit/Aster의 BTC·ETH·SOL·XRP·DOGE USDT 계약을 100ms 간격으로 비교합니다. 코인별 18개, 총 90개 조합입니다. 파일 수 제한 없이 전체 기간의 입력을 사용합니다.

```bash
cargo build --release --locked --bin zscore
# 설정 파일을 직접 지정
./target/release/zscore --config config/zscore-oct20-30.json
# 같은 실행을 담은 샘플 명령 파일
./scripts/run-zscore-oct20-30.sh
# AWS 접속이나 다운로드 없이 설정만 검증하고 출력
./scripts/run-zscore-oct20-30.sh --print-config
# 파일을 수정하지 않고 특정 값만 덮어쓰기
./scripts/run-zscore-oct20-30.sh --entry-z 2,3 --output data/reports/zscore-oct20-30-v2
```

우선순위는 **명령줄 > JSON 파일 > 기본값**입니다. JSON 키는 옵션 이름의 하이픈을 밑줄로 바꾼 `sample_ms`, `entry_z` 형태이며, sweep은 JSON 배열로 작성합니다. 명령줄의 배열은 파일의 배열 전체를 대체합니다. 필수값은 `start`, `end`, `output`과 대상 지정입니다. 대상은 `assets` 배열 또는 `left_instrument`/`right_instrument` 두 값을 지정하며, 둘을 함께 쓰지 않습니다. 모르는 키·잘못된 타입·빈 배열은 오류 처리하며, 선택 옵션의 `null`은 기본값/미지정으로 취급합니다. 기존의 긴 명령 방식도 그대로 사용할 수 있습니다.

상대 경로(`env_file`, `cache_dir`, `input`, `output`)는 **실행 디렉터리 기준**입니다. 샘플 스크립트는 저장소 루트로 이동하므로 어느 디렉터리에서 호출해도 같은 경로를 사용합니다. 반복 실행은 새 `--output`을 지정하세요. 한국 시각을 원하면 날짜 문자열의 `Z`를 `+09:00`으로 바꾸면 됩니다. 설정에는 AWS 비밀키를 넣지 않고 기존 `.env`나 AWS credential chain을 사용합니다.

## 파라미터 조합 수의 의미

"코인별 18개"는 코인마다 아래 설정의 모든 조합으로 백테스트를 18번 수행한다는 뜻입니다. 거래 횟수가 아닙니다.

| 파라미터 | 예시 설정 | 선택지 수 |
|---|---|---|
| 평균·표준편차 계산 구간 `windows_ms` | 5초, 10초, 30초 | 3 |
| 진입 기준 `entry_z` | 1.5, 2.0, 2.5 | 3 |
| 청산 기준 `exit_z` | 0.25, 0.5 | 2 |

3 × 3 × 2 = 코인당 18개 설정입니다. 예를 들어 BTC에 5초/1.5/0.25를 적용한 결과와 5초/1.5/0.5를 적용한 결과는 별도 실험입니다. 5개 코인에 같은 조합들을 적용하므로 총 90개 결과가 나옵니다. 실제 거래 횟수는 각 실험의 신호에 따라 달라집니다.

코인마다 한 가지 설정만 실행하려면 배열을 `"windows_ms": [10000]`, `"entry_z": [2.0]`, `"exit_z": [0.5]`로 바꾸면 됩니다. 이 경우 5개 코인 × 1개 설정으로 총 5개 결과가 나옵니다.

## 여러 코인 비교

설정 파일의 목록을 수정하거나 명령줄로 대체하세요.

```json
"assets": ["BTC", "ETH", "SOL", "XRP", "DOGE"]
```

```bash
# ETH와 SOL만 비교 (파일의 목록 전체를 대체)
./scripts/run-zscore-oct20-30.sh --assets ETH,SOL --output data/reports/zscore-eth-sol
# 코인 수 × 파라미터 조합 수가 기본 100을 넘으면 한도를 명시
./scripts/run-zscore-oct20-30.sh --assets BTC,ETH,SOL,XRP,DOGE,ADA --max-runs 200
```

`assets`는 Bybit/Aster의 동일 코인 USDT 계약에 대응합니다. 예를 들어 ETH는 `bybit:ETHUSDT` / `aster:ETHUSDT`입니다. 다른 거래소나 계약은 기존 명시적 instrument ID 방식으로 지정하세요. 소문자 코인명은 대문자로 변환하고 중복 목록은 거절합니다. 실제 입력에 없는 코인은 제외하지 않고 `no_data`로 남깁니다. 예시 5개 코인은 기존 10월 25일 수집 보고서에서 양쪽 계약을 확인했지만, 요청한 10일 전체의 가용성을 보장하는 것은 아닙니다.

S3 조회·다운로드는 전체 코인에 한 번 수행합니다. 이후 메모리 사용을 제한하도록 코인별로 Parquet를 순차 스캔하며, 각 코인의 파라미터 조합은 동일한 시계열과 rolling 통계를 재사용합니다. `max_runs`는 **모든 코인을 합친 조합 수**의 한도입니다.

결과는 `output/BTC/summary.json`, `output/ETH/summary.json`처럼 나뉘고, 최상위 `output/summary.json`에서 코인별 품질·거래 결과를 비교할 수 있습니다. 이는 코인별 독립 실험이며 공통 자본을 사용하는 포트폴리오 백테스트가 아닙니다. 단일 코인만 선택할 때도 `--assets BTC`로 지정할 수 있습니다.

## 실행

```bash
cargo build --release --locked --bin zscore
target/release/zscore \
  --start 2026-11-03T00:00:00Z --end 2026-11-04T00:00:00Z \
  --left bybit --right aster \
  --left-instrument bybit:BTCUSDT --right-instrument aster:BTCUSDT \
  --sample-ms 100 --windows-ms 5000,10000,30000 \
  --entry-z 1.5,2,2.5 --exit-z 0.25,0.5 --stop-z 4 \
  --latency-ms 100 --book-max-age-ms 500 \
  --left-fee-bps 5 --right-fee-bps 5 \
  --max-hold-seconds 300 --notional-per-leg 1000 \
  --output data/reports/zscore-btc-20261103
```

UTC `[start,end)`이며 RFC3339의 시간대 오프셋과 소수초도 허용합니다. 샘플링 간격은 1~999ms, 기본 100ms입니다. `--windows-ms`는 이 간격의 정수배인 **시간 길이**입니다. 위 명령은 18개 조합을 비교합니다. `--min-deviation-bps`로 평균과의 최소 괴리를 추가할 수 있습니다. `--max-runs` 기본 100으로 과도한 grid를 방지합니다.

`--max-files 8`을 추가하면 거래소·날짜마다 처음 8개 파일만 사용한 빠른 연결 점검이 가능합니다. 파일명은 시간순이 아니므로 이 옵션은 기간 전체 실험이 아니며, 양쪽 데이터가 겹치지 않아 `no_data`가 나올 수도 있습니다. 출력의 `partial`을 확인하세요. 기간 전체를 평가할 때는 생략합니다.

## AWS 입력과 빠른 반복

- 경로: `quant/normalized/dataset=book_state_20/exchange=.../date=YYYY-MM-DD/*.parquet`.
- `.env`의 AWS/S3 변수만 읽고 실행하지 않습니다. 프로세스 환경이 우선하며, 자격 증명을 지정하지 않으면 AWS SDK 기본 credential chain을 사용합니다. `--env-file`, `--bucket`, `--prefix`, `--region`으로 설정할 수 있습니다.
- S3 목록 페이지 전체를 조회하고, 저장 파티션은 거래소 event-time 날짜이므로 요청 날짜의 앞뒤 1일도 기본 포함하고 (`--partition-pad-days 0..7`), 실제 `received_at_ns`로 범위를 자릅니다. 날짜 단위 파티션이고 파일명이 시간 정보를 포함하지 않아 **짧은 기간도 최초 실행 시 해당 날짜 및 padding 날짜 파일 전체 다운로드가 필요**합니다. 거래소 두 곳만 읽으며 내려받기는 기본 8개 병렬입니다.
- 수신 지연/거래소 시계 오차가 padding 범위를 넘는 이벤트는 발견하지 못할 수 있습니다. 빠진 파티션은 summary에 기록하고 partial로 표시합니다.
- 로컬 캐시는 `data/cache/books`. 원격 ETag·크기와 일치할 때 재사용하고, 다운로드는 조건부 GET과 임시 파일을 사용합니다. S3에는 쓰지 않습니다.
- Parquet는 필요한 열만 읽고, 두 상품의 호가 이벤트만 메모리에 유지합니다. 시간 정렬과 샘플링은 한 번, rolling 통계는 window마다 한 번만 수행하여 threshold sweep에 재사용합니다.
- 완전 오프라인 반복: 위 명령에 `--input data/cache/books`를 추가하고 새 `--output`을 지정합니다. 로컬 입력은 완전성 검증이 없으므로 `partial=true`입니다. 다른 스키마 파일을 같은 입력 디렉터리에 섞지 마세요.

## 시간·전략·비용 규칙

- 이벤트 시각 대신 **수신 시각 `received_at_ns`**을 사용합니다. 각 100ms 경계까지 수신된 마지막 유효 호가만 사용하며 미래 호가를 당겨오거나 선형 보간하지 않습니다. 원천 업데이트가 느리면 100ms 샘플링이 새로운 정보를 만들어 주지는 않습니다.
- `book_valid`, 양수 price/qty, bid < ask와 최대 호가 나이를 검사합니다. 무효 호가가 오면 이전 호가를 무효화합니다. 동일 거래소/상품/수신 시각 중복은 순서를 추측하지 않고 제외합니다. 두 계약의 base/quote 자산이 다르면 실패합니다.
- basis = `10000 × (ln(mid_left) − ln(mid_right))`. z는 **현재 샘플을 제외한 직전 window**의 평균과 모집단 표준편차로 계산합니다. 표준편차가 0이면 거래하지 않습니다. 워밍업은 지정 기간 안에서 시작하며, 유효 샘플 누락 후 다시 시작합니다.
- 양의 z: left short/right long, 음의 z: left long/right short. 평균 복귀(0 통과 포함), stop-z, 최대 보유시간에 청산을 결정합니다.
- 결정 후 `latency-ms` 이상 지난 첫 유효 샘플에서 BBO로 체결합니다. 실효 지연은 샘플링 간격으로 올림됩니다. 결측 시 미체결 진입은 취소하고 보유 포지션은 데이터 복구 후 지연을 적용해 청산합니다. 종료 시 미체결 주문과 미청산 포지션은 남겨 표시합니다.
- 각 다리 진입 명목은 동일하며 고정 기초 수량으로 선형 PnL을 계산합니다. 매수는 ask/매도는 bid, 진입·청산 실제 명목에 각각 해당 거래소 수수료를 적용합니다. net bps의 분모는 두 다리 합산 **진입 gross 명목**입니다.
- `final_liquidation_equity`는 실현손익 + 마지막 유효 시점에서 열린 포지션을 청산한다고 가정한 손익입니다. `max_drawdown`도 각 샘플의 같은 평가 기준으로 계산합니다. 이는 자본 수익률이 아닙니다. 결측 구간 내 가격 변화는 관측할 수 없습니다.

## 결과 및 한계

`summary.json`: 설정, PR, 구현 SHA-256, 입력 manifest hash, 입력 품질, 모든 variant 요약. `sources.json`: 파일 경로·크기·ETag. `<variant_id>.json`: 개별 거래(시각/수량/가격/수수료/손익/청산 이유), 미청산 상태, drawdown. `no_data`, `insufficient_warmup_or_zero_variance`, `no_closed_trades`, `open_position`, `traded`를 구분합니다. 완성된 출력 경로는 덮어쓰지 않습니다.

현재는 **BBO 기반 연구용 체결 근사**입니다. 원본 20레벨을 읽지만 체결은 최우선 호가를 사용하며, 주문 크기별 깊이/VWAP, 부분 체결, 펀딩, 마진, 거래소별 주문 제약은 모델링하지 않습니다. 성과는 실거래 수익성으로 해석하지 말고 후보 파라미터 비교에 사용하세요. 실행 시간대·상품·캐시 파일 집합과 함께 결과를 보존하세요.

```bash
cargo test --locked --bin zscore
```

## 실제 검증 기록 (2026-11-04)

- PR #3가 열린 상태임을 GitHub API로 확인하고 engine / parameter sweep 설계를 참고했습니다.
- 2026-11-03 Bybit/Aster `book_state_20` 각각 8개, 총 16개 S3 파일의 140,987행을 읽었습니다. BTCUSDT 수신 이벤트는 Bybit 3,894개 / Aster 874개였고, 각각 연속 관측 간격 중 1초 미만인 경우는 3,886 / 866개였습니다.
- 100ms 샘플링에서 양쪽 유효 샘플 1,041개, window 1/5/10초 × entry-z 1.5/2/2.5의 9개 설정을 실행했습니다. 부분 표본이며 연속 하루 검증이 아닙니다. 기본 수수료 가정에서는 9개 모두 비용 차감 후 손실이었습니다.
- 같은 파일을 로컬 캐시에서 release 실행한 결과가 S3 실행 결과와 동일했고, 이 소규모 표본의 재실행은 약 0.20초였습니다. 대규모 기간의 처리 속도를 뜻하지 않습니다.
- 별도 30초 범위 실행으로 기간 필터와 Parquet row-group 제외를 확인했습니다. workspace 테스트 11개 및 `cargo clippy --locked --bin zscore -- -D warnings` 통과.
- 로컬 결과: `data/reports/zscore-native-s3-smoke/summary.json`, `data/reports/zscore-native-cache-repeat/summary.json`, `data/reports/zscore-native-period-check/summary.json`. 데이터/보고서는 기존 `.gitignore`에 따라 커밋 대상에서 제외됩니다.
