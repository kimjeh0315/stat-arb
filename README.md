# Quant market-data collector

Bybit · Aster · GRVT · Hyperliquid의 공개 무기한 선물 데이터를 수집합니다. 수집·오더북 복원·1초 분석·Parquet 생성·S3 업로드는 모두 Rust로 실행합니다.

## 실행 환경 요구사항

- 빌드: Rust 1.99.0 (`rust-toolchain.toml`), C 링커, CMake, Perl, 인터넷 연결. Ubuntu에서는 `build-essential`, `cmake`, `pkg-config`, `perl`을 준비합니다. 재현 가능한 빌드는 `Cargo.lock`과 `--locked`를 사용합니다.
- 실행: Linux 또는 macOS, 거래소 HTTPS·WebSocket 접속, 쓰기 가능한 디스크. Python은 필요하지 않습니다.
- S3 사용 시 버킷과 AWS 자격 증명, 객체 업로드·조회 권한. 싱가포르 리전이 기본입니다.
- 저장 설정과 50개 자산 범위는 `config/`에서 관리합니다. `QUANT_SPOOL_DIR`, `QUANT_S3_BUCKET`, `QUANT_S3_PREFIX`, `AWS_REGION`으로 덮어쓸 수 있습니다.

```bash
cargo build --release --locked
target/release/quant-collector --config config/collector.json discover
# 두 프로세스를 별도 터미널 또는 systemd 서비스로 실행
target/release/quant-collector --config config/collector.json collect
target/release/quant-collector --config config/collector.json archive
```

`collect --duration 30`은 제한 시간 수집, `flush`는 완료된 저널 변환·업로드, `verify`는 로컬 불변식 검증입니다. `import-legacy FILE.ready`로 이전 JSONL 저널을 복사 변환하고, `replay FILE.wal.ready --output analysis.parquet`로 정규화 이벤트에서 분석을 재생성합니다. 이전 저널 원본은 자동 삭제하지 않습니다. 실행 중인 `.open` 파일은 archiver가 읽지 않습니다.

Lightsail에는 Linux용으로 빌드한 바이너리를 `/opt/quant/bin/quant-collector`에 설치하고, `deploy/`의 수집·저장 서비스 두 개를 사용합니다. macOS 빌드 결과는 Linux에서 실행할 수 없습니다.

## 계층 구조

```text
config/                 자산 범위 · 실행 설정
native/src/
  bin/validate-analysis.rs  독립 데이터 검증 명령
  validation/           S3·로컬 입력 → 1초 품질 검사 → 보고서·사용 가능 마스크
  main.rs               명령 · 신호 처리 · 재생 및 검증
  service.rs            연결별 작업 · 재연결 · 수신 큐 · 종료 관리
  exchanges.rs          거래소별 상품 탐색 · 구독 · 메시지 및 REST 해석
  model.rs              이벤트 모델 · Decimal128 단위 정규화
  book.rs               전체 구독 깊이 복원 · 상위 20레벨 추출
  analysis.rs           1초 분석 · 중복 제거 · 누락/오래된 데이터 표시
  journal.rs            바이너리 저널 · 체크섬 · 동기화 · 장애 복구
  schema.rs             Arrow/Parquet 스키마
  storage.rs            묶음 변환 · gzip · Parquet · S3 검증 및 재시도
deploy/                 수집/저장 systemd 서비스 · 환경 변수 · S3 권한
scripts/                S3 샘플 다운로드 · 용량 집계 · 데이터 검증 실행
```

수집 프로세스: **거래소 → 정규화·오더북 → 1초 분석 → 바이너리 저널**.
저장 프로세스: **완료 저널 묶음 → raw gzip / Parquet → S3 → 검증 후 로컬 파일 삭제**.

오더북은 Bybit 50레벨, Aster REST 1,000레벨과 delta, GRVT 전체 depth delta, Hyperliquid l2Book으로 복원하며 저장은 양쪽 각각 20레벨입니다. Mid는 직접 구독하지 않고 계산합니다. OI는 수집합니다. GRVT·Hyperliquid 공개 청산은 미제공으로 표시하며, 이 Rust 단계에서는 외부 청산 provider 설정을 허용하지 않습니다.

Parquet 경로와 컬럼은 기존 형식을 유지하지만 Rust 이벤트 ID 규칙과 숫자 처리에는 `schema_version=2`를 사용합니다. 18자리보다 긴 입력 정밀도는 거절하고, 곱셈·나눗셈 결과는 18자리에서 0 방향으로 절삭합니다. 완전한 청산 건수나 재시작 전체에 걸친 전역 중복 제거를 보장하지 않습니다.

기본 저장 묶음은 완료 저널 64MiB 또는 가장 오래된 완료 저널의 대기 시간 120초입니다. 이는 S3 파일 생성 주기이며, 수신 주기나 1초 분석 주기와 다릅니다. CPU 압축 작업은 저장 프로세스에서 한 번에 하나씩 실행합니다. 실제 Lightsail 처리 한계는 전체 종목 부하 검증 후 확정해야 합니다.

## Subsecond z-score 연구

`zscore`는 S3의 **normalized `book_state_20` 원본 호가**를 읽는 독립 Rust CLI입니다. UTC 기간과 밀리초 샘플링 간격을 지정하고 window·진입/청산 z를 한 번에 비교합니다. 1초 분석 데이터는 사용하지 않습니다. 캐시와 결과 JSON, 실행 예시 및 체결 모델의 범위는 [사용 안내](scripts/zscore.md)를 참고하세요.

```bash
cargo build --release --locked --bin zscore
target/release/zscore --config config/zscore-oct20-30.json
# 또는 ./scripts/run-zscore-oct20-30.sh
```
