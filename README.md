# Quant market-data collector

Bybit · Aster · GRVT · Hyperliquid의 공개 무기한 선물 데이터를 수집하고, 20레벨 오더북과 1초 분석 데이터를 로컬 및 S3에 저장합니다.

## 실행 환경 요구사항

- Python 3.9 이상. Lightsail Ubuntu에서는 Python 3.11 이상을 권장합니다.
- 의존성: `aiohttp`, `boto3`, `pyarrow`, `sortedcontainers`. 설치 시 함께 설치됩니다.
- 거래소의 HTTPS·WebSocket API에 접속할 수 있는 네트워크 환경.
- 수집 파일과 업로드 대기 파일을 보관할 쓰기 가능한 디스크. 기본 저장 경로는 `var/spool/`, 용량 제한은 10GiB입니다.
- S3 사용 시 버킷, AWS 자격 증명, 객체 업로드·조회 권한이 필요합니다. 기본 리전은 싱가포르(`ap-southeast-1`)입니다.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/quant-collector run
```

수집 범위와 저장 설정은 `config/`에서 지정합니다. S3 버킷은 `QUANT_S3_BUCKET`으로 설정하며, 미설정 시 로컬에만 저장합니다.

## 계층 구조

```text
config/                  자산 범위 및 실행 설정
src/quant_collector/
  cli.py                 명령 진입점: discover / run / flush
  service.py             수집 작업 실행 및 종료 관리
  adapters/              거래소별 상품 탐색 · 구독 · 메시지 해석
  transport.py           HTTP 통신 및 요청 속도 제한
  external.py            외부 공개 청산 피드 연결
  domain.py              상품 · 이벤트 모델 및 단위 정규화
  orderbook.py           오더북 복원 및 상위 20레벨 추출
  pipeline.py            수신 큐 · 중복 제거 · 저널 기록
  analysis.py            1초 단위 분석 데이터 생성
  schema.py              데이터별 Parquet 스키마
  storage.py             로컬 저장 · 복구 · 압축 · S3 업로드
deploy/                 Lightsail 서비스 및 환경 변수 · S3 권한 예시
```

처리 흐름은 **거래소 수신 → 정규화·오더북 복원 → 수신 큐·저널 → Parquet 변환 → S3 업로드**입니다. 분석 계층은 수신 상태를 바탕으로 매초 분석 데이터를 생성합니다.
