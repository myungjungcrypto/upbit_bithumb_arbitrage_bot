# kimp — 업비트/빗썸 김프 아비트라지 봇

설계 문서: **[PLAN.md](PLAN.md)** (살아있는 문서 — 모든 결정과 미결 논제가 여기 있음)

## 현재 단계: P0 — 시세 파이프라인

거래소 5곳(업비트·빗썸·바이낸스·바이비트·OKX) WebSocket + USD/KRW 환율을 수집해서:

- **이원 김프** 계산 — 이론 김프(고시환율 기준) / 실행 김프(USDT/KRW 실거래가 기준)를 별도 산출
- **호가 깊이 기반 왕복 엣지** — 금액대별(기본 $5k/$20k/$50k)로 "해외매수→국내매도→USDT복귀" 왕복을 호가창 VWAP로 시뮬레이션한 순엣지 (inbound/outbound 양방향)
- Parquet 시계열 저장 (백테스트 원료: 호가 15단 스냅샷, 체결, 김프 틱, 환율)
- 텔레그램 알림 (순엣지 임계 초과, 피드 이상)

주문·출금 기능은 P0에 없다. 공개 시세만 사용하므로 API 키 불필요.

## 실행

```bash
pip install -e .
cp .env.example .env        # 텔레그램 토큰 입력 (비우면 로그로만 알림)
kimp-collect --config config/default.yaml
```

## 테스트

```bash
pip install -e .[dev]
pytest
```

## 구조

```
src/kimp/
  collectors/   # 거래소별 WS 수집기 (재연결·스테일 감시 공통 베이스) + 환율 폴러
  engine/       # 호가 상태 + 이원 김프·왕복 VWAP 엣지 계산
  storage/      # Parquet 버퍼 라이터 (date=YYYY-MM-DD 파티션)
  alerts/       # 텔레그램 (심각도 계층·쿨다운)
  bus.py        # 인프로세스 pub/sub (핫패스)
  app.py        # 배선·감독·종료 처리
config/default.yaml   # 코인 유니버스, 수수료 추정, 임계값, 저장 정책
```
