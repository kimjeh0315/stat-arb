# 1초 분석 데이터 검증

수집기를 실행하거나 S3 객체를 수정하지 않는 Rust 검사 프로그램입니다. Python은 사용하지 않습니다.

## 실행

저장소 루트의 `.env`에서 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `QUANT_S3_BUCKET`, `QUANT_S3_PREFIX`를 읽습니다. 셸 환경변수가 있으면 우선합니다. GitHub 토큰은 읽지 않습니다. IAM에는 해당 prefix의 `s3:ListBucket`과 `s3:GetObject`가 필요합니다.

```bash
# Rust 1.99.0이 필요하며, 최초 실행 또는 소스 변경 시 자동 빌드
bash scripts/validate-analysis.sh --date 2026-10-25 --write-masks

# 네트워크·파일·스키마 연결을 확인하는 소규모 실행 (하루 전체 검증 아님)
bash scripts/validate-analysis.sh --date 2026-10-25 --exchange bybit \
  --max-files 3 --output data/reports/analysis-smoke

# 이미 내려받은 analysis_state_1s 파일 또는 전용 디렉터리 검사
bash scripts/validate-analysis.sh --date 2026-10-25 \
  --input data/cache/analysis/quant/derived/dataset=analysis_state_1s \
  --output data/reports/analysis-local
```

`--input`에는 오더북 등 다른 데이터셋을 섞지 않습니다. 기본 S3 실행은 해당 날짜의 4개 거래소 분석 파티션을 끝까지 페이지네이션합니다. UUID 파일명은 시간 순서로 간주하지 않습니다. 기본 병렬 다운로드는 8개이며 `--download-concurrency`로 조절합니다. 약 2.2GB인 10월 25일 분석 파일을 로컬 캐시에 저장합니다. 객체 크기와 ETag가 일치하는 캐시를 재사용합니다.

기본 결과 경로는 `data/reports/analysis-2026-10-25/`입니다. 완료된 보고서는 덮어쓰지 않으며, 재실행할 때 다른 `--output`을 지정합니다. 중단된 다운로드의 `.part` 파일은 해당 요청이 실패하면 정리됩니다.

## 당시 수집 대상 지정

기본값은 파일에서 한 번이라도 관측된 상품을 종일 대상이라고 가정합니다. 한 번도 나타나지 않은 상품, 당시 상장·수집 시작 시각은 이 방식으로 알 수 없습니다. 이를 구분하려면 **당시 실제 대상 목록**을 다음 JSON으로 준비하고 `--expected-instruments FILE.json`을 지정합니다.

```json
[
  {
    "exchange": "bybit",
    "instrument_id": "bybit:BTCUSDT",
    "start_ns": null,
    "end_ns": null
  }
]
```

위 JSON은 문법 예시이며 전체 수집 대상 목록이 아닙니다. 거래소와 native contract ID를 모두 구분해 전체 목록을 작성합니다. `start_ns`, `end_ns`는 UTC Unix nanoseconds이며, null이면 검사일 시작·종료입니다. 기대 초는 이 구간 안의 정수 초 `[start, end)`로 계산합니다. 목록의 상품이 완전히 없어도 누락으로 보고하며, 목록에 없는 상품은 별도로 오류 처리합니다.

## 출력

- `summary.json`: 검사 범위, 오류, 상품별 통계, NULL·오래된 값·품질 플래그, age 분포와 임계값.
- `instruments.csv`: 상품별 기대 초·존재 초·누락 초·중복·충돌·최장 공백·용도별 사용 가능 초.
- `incidents.jsonl`: 상품별 문제의 연속 구간. 종료 시각은 exclusive입니다. 겹치는 원인은 독립적으로 기록됩니다.
- `timeline_1m.csv`: 거래소별 분 단위 누락·가격 사용 불가·연결 끊김의 **상품×초** 합계. 예를 들어 상품 10개가 1초씩 누락되면 10입니다.
- `manifest.json`: 검사한 파일과 S3 크기·ETag.
- `usability.parquet`: `--write-masks`를 지정한 경우, 누락된 초까지 포함한 가격·체결·OI·펀딩 사용 가능 마스크. `issue_bits` 정의는 `summary.json`에 있습니다.

마스크에서 `price_usable`은 **Mid 기반 오더북 사용 가능 여부**입니다. Mark·Index·Oracle·Last 전체가 정상이라는 뜻이 아닙니다. 이 값들의 NULL·age 통계는 상품별 `fields`에서 따로 확인합니다. `trade_usable`은 공유 연결 상태와 품질 플래그 및 거래량 일관성을 기준으로 하며, 채널별 수신 정상성을 직접 증명하지 않습니다. OI·펀딩 마스크는 보유 값과 age를 기준으로 합니다. 충돌하는 중복 행은 모든 마스크를 false로 만듭니다.

기본 최신성 기준은 book 1초, 일반 가격 10초, OI 180초, 펀딩 900초입니다. 이 값은 운영 실측으로 확정된 기준이 아니며 `--book-max-age-ms`, `--price-max-age-ms`, `--oi-max-age-ms`, `--funding-max-age-ms`로 조절합니다. book age 1/3/5/15초 초과 횟수도 기록합니다. age 분위수는 고정 histogram의 **상한값**이며 정확한 분위수는 아닙니다. 필드 NULL·age·플래그 통계는 중복을 포함한 입력 행 기준이고, 연속성·마스크 통계는 고유 상품×초 기준입니다.

## 결과 해석과 종료 코드

- `audit_complete`는 파일 검사에 실패가 없었다는 의미이며 데이터 품질 정상 판정이 아닙니다.
- 로컬 입력과 `--max-files`는 `partial_scope=true`로 표시하며 하루 전체 정상 판정에 사용하지 않습니다.
- 필드가 한 번도 관측되지 않았다는 사실만으로 거래소 미제공으로 확정하지 않습니다. 거래소 제공 범위와 대조해야 합니다.
- 분석 데이터의 시각·age로 실제 거래소→서버 지연이나 생성 지연을 계산하지 않습니다.
- 정제 오더북·체결 재계산, 서버 재시작 원인, 가격 급변의 진위는 이 프로그램의 범위 밖입니다.
- 종료 코드 0: 검사 완료(품질 문제가 발견될 수 있음), 1: 실행 실패, 2: `--fail-on-issues`를 지정했고 누락·중복·수치·파일 오류 등이 발견됨.
- 불완전한 파일도 나머지 파일 검사는 계속하며 `summary.json.file_errors`와 `audit_complete=false`로 보고합니다.

검증 프로그램 테스트: `cargo test --release --locked --bin validate-analysis`.
