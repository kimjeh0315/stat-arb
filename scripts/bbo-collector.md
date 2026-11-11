# BBO 수집 모드

`config/collector-bbo.json`으로 기존 Rust 실행 파일의 `collect`와 `archive`를 실행한다. 기존 full 모드는 유지한다.

- 거래소/유니버스: 네 거래소, 기존 50개 자산. 상장된 상품만 수집하고 USDT/USDC 및 native symbol을 구분한다.
- Bybit: `orderbook.1` 스냅샷. Aster: `bookTicker`. GRVT: `v1.mini.s` 200ms 스냅샷. Hyperliquid: `bbo`.
- 출력: `bbo`, `instrument_metadata`, `collection_status` Parquet만 저장. raw, funding/OI/trades/liquidation, 1초 파생 분석은 생성하지 않는다.
- BBO 필드: bid/ask 가격 및 기초자산 수량, 거래소/수신/단조 시각, 상품 ID, 세션, 공급자 시퀀스(제공 시), 유효성 표시.
- 기초자산 배수는 가격을 나누고 수량을 곱해 정규화한다. Mid나 Z-score는 저장하지 않는다.
- GRVT mini 메시지에 포함되는 다른 가격 필드는 버린다. 별도 요청은 하지 않는다. 시작 시 상품 명세 조회만 수행한다.
- 빈 호가·교차 호가는 `quote_valid=false`. 연결 종료 상태 및 세션 경계를 적용해 과거 호가를 무제한 채우지 않는다. 무변경 BBO가 오래됐다는 것과 연결 장애는 다르므로 분석에서 구분한다.
- 최우선 잔량을 넘는 주문의 VWAP, 시장 충격, 펀딩 포함 손익은 이 자료로 평가할 수 없다. 호가통화가 다른 상품은 직접 비교하지 않는다.

## 서버 배포

`cargo build --release --locked --bin quant-collector` 후 `sudo bash deploy/install-bbo.sh BUILD_DIRECTORY`.
새 서비스는 `quant-bbo-collector` / `quant-bbo-archiver`, 로컬 대기 영역 `/var/lib/quant-bbo`, S3 prefix `quant/bbo-v1`.
`/etc/quant/collector.env`의 기존 자격증명을 재사용하고 마지막에 `/etc/quant/bbo.env`로 새 경로만 덮어쓴다.
기존 바이너리/설정/데이터는 보존한다. 전환 시 기존 두 서비스를 중지·비활성화하고, 제한 시간 수집 및 업로드 검증 후 새 서비스를 활성화한다.
롤백은 새 두 서비스를 중지하고 기존 서비스를 시작한다. 기존 spool이 한도에 찼다면 먼저 보존 데이터의 업로드 실패를 해결해야 한다.

## 확인할 항목

네 거래소 구독 승인과 종목별 BBO 행, 오류/재연결, 큐 길이, CPU·메모리, 완료 WAL과 S3 업로드 로그를 확인한다. 단기 smoke 성공은 하루 운영 안정성을 보장하지 않는다.

공식 채널 문서: [Bybit](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook), [Aster](https://github.com/asterdex/api-docs), [GRVT](https://api-docs.grvt.io/market_data_streams/), [Hyperliquid](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions).
