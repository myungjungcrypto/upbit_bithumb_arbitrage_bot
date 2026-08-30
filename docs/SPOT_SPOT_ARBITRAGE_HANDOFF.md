# 현물-현물 아비트라지 봇 구현 인계서

이 문서는 현재 `hyunsungap_bot`에서 구현하고 검증한 내용을 다른 프로젝트의 Claude가
현물-현물 아비트라지 봇에 재사용할 수 있도록 정리한 기술 인계 문서다.

현재 프로젝트는 **KRW 현물 매수 + 해외 무기한 선물 숏** 구조다. 새 프로젝트는
**거래소 A 현물 매수 + 거래소 B 현물 매도** 구조이므로, 데이터 수집과 실행 가격 계산은
대부분 재사용할 수 있지만 포지션, 잔고, 주문 복구 모델은 현물-현물에 맞게 바꿔야 한다.

---

## 1. 먼저 확정할 전략 범위

첫 실거래 버전은 아래의 **재고 선배치형**을 권장한다.

```text
거래소 A: quote 자산(KRW/USDT 등)을 미리 보유
거래소 B: 매도할 base 코인을 미리 보유

기회 발생:
  A에서 base 현물 매수
  B에서 같은 수량의 base 현물 매도
```

이 방식은 주문 시점에 블록체인 전송을 기다리지 않는다. 두 주문 완료 후 생긴 거래소별
재고 편차는 별도의 리밸런싱 작업으로 복구한다.

전송형은 후속 단계로 둔다.

```text
싼 거래소에서 매수 -> 출금 -> 비싼 거래소 입금 -> 매도
```

전송형에는 출금 중 가격 변동, 네트워크 선택, 출금 수수료, 컨펌 시간, 입출금 중단,
주소/메모 오류가 추가된다. 이것을 실시간 양방 주문 엔진과 한 상태 머신에 섞지 않는다.

### 권장 모드 구분

| 모드 | 목적 | 첫 버전 포함 |
| --- | --- | --- |
| `INVENTORY_ARBITRAGE` | 양 거래소에 재고를 선배치하고 즉시 동시 매매 | 포함 |
| `REBALANCE` | 거래 후 거래소별 base/quote 재고 복구 | Paper부터 |
| `TRANSFER_ARBITRAGE` | 매수 후 온체인 전송하여 매도 | 후순위 |

---

## 2. 현재 프로젝트에서 구현된 내용

### 2.1 실행 구조

현재 엔진은 외부 패키지 없이 Python 표준 라이브러리로 실행된다.

```text
main.py
  -> config.py                 환경변수와 런타임 설정
  -> exchanges.py              거래소 REST 인증, 공개/개인 API, 호가 모델
  -> ws_client.py              WebSocket 프레임과 연결
  -> multi_market.py           전체 마켓 탐색, 호가 캐시, 경로 평가
  -> engine.py                 판단, 포지션, 알림, 주문 오케스트레이션
  -> storage.py                SQLite 영속화와 이력 집계
  -> server.py                 대시보드 HTTP/JSON API
  -> static/                   운영형 대시보드
```

모듈별로 계산, 저장, 거래소 통신, UI 책임을 분리했다. 현물-현물 프로젝트에서도 이
경계를 유지한다.

### 2.2 전체 마켓 자동 탐색

- Bithumb와 Upbit의 전체 KRW 현물 마켓을 자동 탐색한다.
- Binance, Bybit, OKX, Hyperliquid, Lighter 무기한 선물을 자동 탐색한다.
- base ticker가 정확히 같은 경로만 조합한다.
- 현물 거래소와 선물 거래소 조합을 각각 독립 route로 유지한다.
- Lighter 합성자산처럼 ticker만 우연히 같은 자산을 1차 격리한다.
- 비정상적으로 큰 premium은 identity sanity gate로 차단한다.

현물-현물에서는 같은 구조로 `spot venue A x spot venue B`의 양방향 route를 만든다.
`A 매수/B 매도`와 `B 매수/A 매도`는 서로 다른 기회다.

### 2.3 WebSocket 기반 이벤트 판단

- 거래소별 depth orderbook WebSocket을 여러 chunk로 나누어 수신한다.
- 호가 업데이트가 들어오는 순간 해당 route를 dirty queue에 넣는다.
- 같은 route의 연속 업데이트는 coalescing하여 최신 스냅샷만 계산한다.
- 보유 포지션 route는 전체시장 cold scan과 분리된 priority queue에서 즉시 평가한다.
- WebSocket 연결 종료 시 reconnect/backoff를 적용한다.
- 기존 호가가 오래되면 stale로 표시하고 주문 판단에서 제외한다.
- 진입 후보와 오래된 현물 호가는 REST로 재검증한다.
- 더 오래된 메시지가 새 REST/WS book을 덮어쓰지 못하게 exchange timestamp를 비교한다.

중요한 구분은 다음과 같다.

```text
대시보드 갱신: 보통 500ms
거래 판단: WebSocket 이벤트 도착 즉시
```

화면 주기를 빠르게 하는 것으로 주문 반응 속도를 만들지 않는다.

### 2.4 고정 금액의 실제 체결 VWAP

최우선 호가 한 줄의 가격 차이가 아니라, 사용자가 정한 금액을 실제로 긁었을 때의
평균 체결가를 사용한다. 기본 기준 금액은 `$5,000`이며 대시보드에서 변경 가능하다.

핵심 함수는 다음 두 가지다.

- `fill_quote`: quote 금액을 소진할 때 base 수량, VWAP, 사용 level 계산
- `fill_base`: 같은 base 수량을 소진할 때 VWAP, notional, 사용 level 계산

현재 현선 구조에서는 한 레그에서 기준 금액으로 base 수량을 구하고 다른 레그도 같은
base 수량으로 다시 계산한다. 이 방식 덕분에 서로 다른 수량을 비교해서 생기는 가짜 갭을
막는다.

현물-현물에서도 반드시 아래 순서를 사용한다.

```text
1. 매수 거래소 ask에서 설정 quote 금액으로 매수 가능한 base 수량 계산
2. 매도 거래소 bid에서 같은 base 수량이 실제로 매도 가능한지 계산
3. 양쪽 깊이, 잔고, 최대 주문 한도 중 가장 작은 base 수량으로 다시 양쪽 VWAP 계산
```

### 2.5 가격 지표 분리

현재 프로젝트는 진입용 지표와 청산용 지표를 분리한다.

- `entry spread`: 현물 ask 매수와 선물 bid 숏의 실행 가능 차이
- `exit premium`: 현물 bid 매도와 선물 ask 환매의 실행 가능 차이

현물-현물에서는 다음처럼 바꾼다.

```text
buy_cost_common = buy_qty * buy_vwap * buy_fx_ask
sell_proceeds_common = sell_qty * sell_vwap * sell_fx_bid

gross_profit = sell_proceeds_common - buy_cost_common
gross_spread_pct = gross_profit / buy_cost_common * 100

net_profit = gross_profit
             - buy_fee
             - sell_fee
             - expected_rebalance_cost
             - expected_withdrawal_cost

net_spread_pct = net_profit / buy_cost_common * 100
```

FX에는 mid 가격이 아니라 실제 방향에 맞는 bid/ask 또는 실행 VWAP을 사용한다. 예를 들어
USDT 수익을 KRW로 환산해 확정하려면 `KRW-USDT`의 실제 매도 가능 가격을 사용한다.

### 2.6 freshness와 가짜 기회 방지

- 현물/선물/FX 각 book의 age를 따로 관리한다.
- 하나라도 stale이면 `tradeable=false`로 전환한다.
- 깊이가 부족하면 수치 대신 명시적인 `DEPTH_INSUFFICIENT` 상태를 반환한다.
- entry 후보는 REST 깊이 재검증을 통과해야 실행 후보가 된다.
- 입출금 중단과 유의 종목은 가격 기회를 숨기지 않고 `RISK_BLOCKED`로 표시한다.
- 열린 포지션은 게시 임계값 아래로 내려가도 목록과 priority stream에서 유지한다.

현물-현물에서는 다음 검증을 추가한다.

- 두 거래소 base 자산의 contract/address/network identity
- 두 거래소의 거래 가능 상태
- base 입금/출금 상태와 quote 입금/출금 상태
- 거래소별 최소 주문 금액, 가격 tick, 수량 step
- 실제 계정 수수료 tier

### 2.7 펀딩 및 입출금 위험 모델

현재 프로젝트는 선물 펀딩 주기를 8시간 기준으로 정규화한다.

```text
funding_rate_8h = native_rate * 8 / native_interval_hours
```

현물-현물에는 펀딩이 없으므로 이 열을 아래 값으로 교체한다.

- 매수/매도 수수료 합계
- 예상 리밸런싱 비용
- 출금 수수료와 최소 출금 수량
- 입출금 상태
- 최근 또는 예상 전송 시간
- 재고 runway: 현재 재고로 동일 주문을 몇 회 더 실행할 수 있는지

### 2.8 입출금 및 자산 위험

- Upbit/Bithumb의 입출금 상태를 정규화한다.
- 유의/주의, 입금 중단, 출금 중단, 상태 조회 실패를 별도 코드로 표시한다.
- 위험 상태에서도 관찰은 계속하지만 자동 진입은 차단할 수 있다.

현물-현물에서는 **실행 자체**와 **리밸런싱 가능성**을 분리한다.

- 재고 선배치형 양방 주문은 입출금 중단 중에도 실행 가능할 수 있다.
- 하지만 재고를 다시 맞출 수 없으므로 `rebalance_risk`가 높아진다.
- 대시보드에는 `거래 가능`과 `리밸런싱 가능`을 별도 상태로 표시한다.

### 2.9 이력 저장과 메모리 관리

갭 이력은 메모리에 7일치를 계속 쌓지 않는다. SQLite에 아래 해상도로 저장한다.

| 조회 범위 | 해상도 | 보존 |
| --- | --- | --- |
| 1시간 | 1초 원본 | 약 2시간 |
| 24시간 | 30초 OHLC + 평균 | 약 25시간 |
| 7일 | 5분 OHLC + 평균 | 약 8일 |

대시보드의 `1h/24h/7d` 버튼은 같은 배열을 늘려 그리는 것이 아니라 조회 범위에 맞는
집계 테이블을 읽는다. 각 응답에는 실제 데이터 coverage도 포함한다.

현물-현물에서도 `gross_spread`, `net_spread`, 양쪽 VWAP, FX, 수수료 snapshot을 함께
저장해야 과거 기회의 실현 가능성을 재현할 수 있다.

### 2.10 Paper 포지션과 부분청산

- 실제 orderbook으로 Paper 진입과 청산을 계산한다.
- 고정 포지션 수량 기준으로 현재 청산 VWAP과 PnL을 다시 계산한다.
- 얕은 호가에서는 최대 `$500` 단위의 부분청산 조각을 계산한다.
- 조건이 500ms 유지되고 새 orderbook snapshot일 때만 다음 조각을 실행한다.
- 조각별 가격, 수량, 수수료 추정, PnL, 잔량을 SQLite에 저장한다.
- 양 레그 수량이 달라지면 `RECONCILE_REQUIRED`로 정지한다.

현물-현물 Paper에서는 포지션보다 `execution cycle`과 `inventory delta`가 핵심이다.

```text
base_delta[buy_venue]  += bought_qty
base_delta[sell_venue] -= sold_qty
quote_delta[buy_venue] -= buy_cost
quote_delta[sell_venue] += sell_proceeds
```

Paper 체결 뒤 이 delta가 목표 재고 범위를 벗어나면 다음 진입을 제한해야 한다.

### 2.11 수동 외부 포지션과 실제 EXIT-ONLY 주문

현재 프로젝트는 거래소에서 수동으로 만든 현선 포지션을 불러와 자동청산할 수 있다.

- 실제 Bithumb KITE 현물 잔고와 OKX KITE 숏 계약을 API로 동기화한다.
- 현물/선물 중 작은 hedge 가능 수량을 MAX로 계산한다.
- 25/50/75/MAX 슬라이더와 직접 수량 입력을 제공한다.
- 수량 승인과 실주문 승인을 분리한다.
- 포지션별 `orders_managed_by_bot` 승인이 있어야 주문할 수 있다.
- 목표 exit premium이 설정 시간 유지되면 부분청산을 시작한다.
- OKX `Buy reduce-only IOC`를 먼저 실행한다.
- OKX의 **실제 체결 base 수량만큼** Bithumb `Sell IOC`를 실행한다.
- 양쪽 실제 평균가와 수량을 주문 저널과 청산 slice에 기록한다.
- 수량 불일치, stale, timeout, 인증 오류가 발생하면 다음 조각을 중지한다.

현물-현물 실행에서도 이 “첫 레그 실제 체결량만큼 두 번째 레그 실행” 패턴을 재사용한다.
단, 어떤 레그를 먼저 실행할지는 재고 위험 정책으로 결정한다.

### 2.12 주문 저널과 재시작 복구

`external_exit_orders`는 주문 제출 전 intent부터 기록한다.

```text
INTENT_RECORDED
  -> FIRST_LEG_SUBMITTED
  -> FIRST_LEG_FILLED
  -> SECOND_LEG_SUBMITTED
  -> FILLS_CONFIRMED
  -> COMPLETE 또는 RECONCILE_REQUIRED
```

- 포지션당 active journal은 하나만 허용한다.
- 각 레그에 client order ID를 사용한다.
- API timeout 후 같은 주문을 바로 다시 내지 않는다.
- client order ID로 기존 주문을 조회한 뒤 상태를 복구한다.
- 프로세스 재시작 시 active journal을 찾아 거래소 주문 상태부터 확인한다.
- 이미 slice가 기록된 journal은 중복 기록하지 않는다.

이 구조는 현물-현물 실거래에서 가장 중요하게 재사용해야 할 부분이다.

### 2.13 다중 안전 잠금

현재 실주문은 한 토글로 켜지지 않는다.

- 전역 `DRY_RUN`
- 일반 자동매매 `AUTO_TRADING`
- 일반 실거래 환경 잠금 `LIVE_TRADING_ALLOWED`
- 외부 청산 전용 환경 잠금 `EXTERNAL_EXIT_LIVE_ALLOWED`
- 포지션별 수량 승인
- 포지션별 실주문 승인과 확인 문자열
- 승인 TTL

현물-현물에서는 아래 잠금을 권장한다.

```text
LIVE_TRADING_ALLOWED
SPOT_SPOT_LIVE_ALLOWED
route allowlist
asset allowlist
per-trade max notional
per-day max loss
per-venue max exposure
manual arm + confirmation text + expiry
```

API 키에는 조회와 거래 권한만 주고 출금 권한은 첫 실거래 버전에서 제외한다.

### 2.14 알림

- 대시보드 이벤트
- 브라우저 알림
- Telegram 알림
- 조건 최초 도달, 조각 완료, 잔량, 오류, 수량 불균형을 구분한다.
- 같은 상태의 반복 알림을 억제한다.

현물-현물에는 아래 알림을 추가한다.

- 순스프레드 조건 도달/이탈
- 첫 레그만 체결됨
- inventory band 초과
- 거래소 잔고 부족
- 입출금 중단으로 리밸런싱 불가
- 일일 손실/노출 circuit breaker 작동

### 2.15 테스트 현황

현재 프로젝트는 78개 단위/통합 테스트를 통과한다. 주요 범위는 다음과 같다.

- depth VWAP와 동일 base 수량 계산
- stale timestamp와 이전 메시지 차단
- WebSocket snapshot/delta 처리
- route queue coalescing과 priority route
- 거래소별 contract size/step/minimum
- Paper 부분청산과 PnL
- 외부 포지션 수량 동기화
- 주문 fill parser
- 주문 저널 단일 active 보장
- OKX 첫 레그 실제 체결량만큼 Bithumb 두 번째 레그 실행
- 실제 체결 slice 기록과 포지션 잔량 갱신

### 2.16 아직 구현되지 않았거나 제한된 부분

아래 항목은 현재 프로젝트에서도 완료된 것으로 가정하면 안 된다.

- 전체 마켓의 자동 실거래 진입은 구현하지 않았다.
- 실제 자동 부분청산은 Bithumb 현물 + OKX 선물의 수동 등록 포지션만 지원한다.
- 두 번째 레그 부족 체결을 자동으로 시장에서 보정하지 않고 안전하게 정지한다.
- private order WebSocket 대신 주문 제출 후 REST 조회를 사용한다.
- 거래소 응답의 실제 수수료를 모든 경로에서 자동 집계하지 않는다.
- Hyperliquid/Lighter 개인 주문과 서명은 모니터링 범위 밖이다.
- 자동 출금, 온체인 전송, 주소록, 네트워크 선택은 구현하지 않았다.
- 다중 자산 실거래의 자본 배분과 일일 손실 한도는 아직 없다.
- 현재 hot path는 Python이며 Rust 이전은 계획 단계다.

현물-현물 프로젝트는 이 제한을 그대로 복사하지 말고 아래 구현 순서에서 보완한다.

---

## 3. 현물-현물에 그대로 재사용할 것

| 현재 구성 | 재사용 방법 |
| --- | --- |
| `Book`, `BookLevel` | 모든 현물 거래소의 bids/asks 공통 모델 |
| `fill_quote`, `fill_base` | 고정 금액과 동일 base 수량 VWAP 계산 |
| WebSocket chunk/reconnect | 거래소별 구독 제한에 맞게 파라미터만 변경 |
| route dirty queue/coalescing | `BUY venue -> SELL venue` route 평가 |
| priority route | 열린 실행, 미정산 imbalance, 보유 inventory route 우선 처리 |
| stale/REST revalidation | 실주문 직전 양쪽 호가 재검증 |
| SQLite history buckets | gross/net spread 이력 저장 |
| events/alerts | 운영 감사와 Telegram 알림 |
| Paper slice 모델 | 작은 조각의 양 레그 체결 시뮬레이션 |
| execution preflight | 잔고, minimum, step, 환경 잠금 검사 |
| client ID 주문 저널 | timeout/restart 중복 주문 방지 |
| 대시보드 구조 | 전체 기회, 상세 차트, 실행 상태, 이벤트 UI |

---

## 4. 현물-현물에서 교체할 것

| 현선 전용 개념 | 현물-현물 대체 개념 |
| --- | --- |
| 선물 숏 포지션 | 매도 거래소의 실제 base 가용 잔고 |
| `reduce-only`, `posSide` | spot `ask` 주문과 잔고 reservation |
| contract size/계약 수 | 현물 수량 step과 최소 주문 금액 |
| 펀딩률/펀딩 runway | 거래 수수료, 출금 비용, inventory runway |
| 진입 후 포지션 청산 | 매매 cycle 완료 후 inventory 리밸런싱 |
| 현물-선물 exit premium | 양방향 spot net executable spread |
| 선물 청산가 위험 | 한 레그 미체결 inventory/가격 노출 |

현물-현물은 양쪽 주문이 완료되면 시장 중립 포지션이 남는 것이 아니라 거래소별 재고 위치가
바뀐다. 따라서 `position`보다 `cycle`, `leg`, `inventory`, `rebalance plan`을 중심으로 모델링한다.

---

## 5. 권장 핵심 데이터 모델

### `MarketRef`

```text
venue
market
base_asset
quote_asset
asset_identity
price_tick
quantity_step
min_quantity
min_notional
fee_tier
networks[]
```

### `SpotSpotOpportunity`

```text
route_id = BASE:BUY_VENUE:SELL_VENUE:BUY_QUOTE:SELL_QUOTE
symbol
buy_venue / buy_market / buy_vwap / buy_fx
sell_venue / sell_market / sell_vwap / sell_fx
base_qty
buy_notional_common
sell_notional_common
gross_spread_pct
net_spread_pct
estimated_fees_common
expected_rebalance_cost_common
depth_levels
freshness_ms
tradeable
risk_codes[]
snapshot_id
```

### `InventorySnapshot`

```text
venue
asset
available
locked
reserved_by_bot
target_min
target_max
updated_at
```

### `ArbitrageCycle`

```text
id / route_id / state
requested_base_qty
approved_base_qty
buy_remaining_qty
sell_remaining_qty
expected_net_profit
realized_net_profit
first_leg_policy
armed_at / expires_at
created_at / completed_at
```

### `ExecutionJournal`

```text
cycle_id / slice_sequence
intent_id
snapshot_id
buy_client_id / buy_order_id / buy_fill_qty / buy_avg_price
sell_client_id / sell_order_id / sell_fill_qty / sell_avg_price
state / error
created_at / updated_at / completed_at
```

### `RebalancePlan`

```text
asset
from_venue / to_venue
quantity
network
withdrawal_fee
expected_confirm_time
state
transaction_id
```

첫 버전의 `RebalancePlan`은 추천과 수동 완료 기록만 구현하고 자동 출금은 하지 않는다.

---

## 6. 권장 실거래 상태 머신

```text
DETECTED
  -> CONFIRMING
  -> PREFLIGHT
  -> ARMED
  -> INTENT_RECORDED
  -> FIRST_LEG_SUBMITTED
  -> FIRST_LEG_TERMINAL
  -> SECOND_LEG_SUBMITTED
  -> SECOND_LEG_TERMINAL
  -> RECONCILING
  -> COMPLETE
```

예외 상태:

- `PAUSED_STALE`
- `PAUSED_DEPTH`
- `PAUSED_BALANCE`
- `PAUSED_RISK`
- `PAUSED_MANUAL`
- `ORDER_STATE_UNKNOWN`
- `RECONCILE_REQUIRED`
- `DUST_REMAINDER`
- `CIRCUIT_BREAKER`

### 첫 레그 정책

기본값은 **유동성이 더 낮거나 체결 불확실성이 큰 레그를 IOC로 먼저 실행하고, 그 실제
체결 수량만큼 두 번째 레그를 실행**하는 방식이다.

단, 첫 레그 선택은 inventory 목표에 따라 달라질 수 있다.

- base 재고가 부족한 상태: 매수 우선 위험이 더 작을 수 있음
- base 재고가 상한에 가까운 상태: 매도 우선 위험이 더 작을 수 있음
- 양쪽 모두 여유: 유동성이 낮은 레그 우선

첫 실거래에서는 작은 조각 IOC로 제한하고, 두 번째 레그 미체결 시 다음 cycle을 중지한다.

---

## 7. 대시보드 구현 지침

현재 프로젝트와 같은 **운영형 스캐너** 맥락으로 만든다. 랜딩 페이지나 소개용 hero를
만들지 말고 첫 화면에서 바로 전체 기회와 실행 상태를 보여준다.

### 7.1 상단 바

- 서비스명: `Spot Arbitrage Monitor`
- 현재 시각
- 모드 배지: `MONITOR`, `PAPER`, `LIVE`, `PAUSED`
- 알림 버튼
- 전략 설정 버튼
- 새로고침 아이콘 버튼

`LIVE` 배지는 전역 실거래 가능 여부가 아니라 실제 armed cycle이 있는지도 반영한다.

### 7.2 요약 밴드

한 줄의 조밀한 지표로 구성한다.

- 연결 거래소 `n / total`
- 수신 중인 orderbook 수
- 공통 자산 수
- 평가 중 route 수
- 순이익 기준 진입 기회 수
- 최대 freshness
- 열린 cycle / reconcile 건수

### 7.3 전체 기회 테이블

권장 열:

| 열 | 내용 |
| --- | --- |
| Asset | symbol, quote 조합 |
| Buy spot | 거래소, market, ask VWAP |
| Sell spot | 거래소, market, bid VWAP |
| Gross spread | 수수료 전 실행 스프레드 |
| Net spread | 수수료/리밸런싱 비용 차감 후 |
| Fees | buy fee + sell fee |
| Inventory | 양쪽 실행 가능 수량과 runway |
| Transfer | 입출금/네트워크 상태 |
| Liquidity | 기준 금액, 사용 호가 level |
| Freshness | 최대 book age |
| State | 진입 가능, 잔고 부족, stale, risk blocked 등 |

필터:

- symbol/거래소 검색
- 매수 거래소
- 매도 거래소
- quote 통화
- 최소 net spread
- 기준 금액
- 최대 freshness
- 입출금/자산 위험 상태
- `전체 / 진입 후보 / 실행 중 / 조정 필요`

행이 잠깐 사라졌다 나타나는 현상을 줄이기 위해 다음 정책을 적용한다.

- 게시 임계값에 진입/이탈 hysteresis 사용
- 마지막 정상 quote를 짧은 visibility TTL 동안 stale 표시로 유지
- armed/open/reconcile route는 조건과 무관하게 항상 유지
- 정렬 순위가 바뀌어도 선택한 상세 route는 고정

### 7.4 상세 화면

상단:

- `BUY venue -> SELL venue`
- 현재 gross/net spread
- 설정 금액 기준 base 수량
- 예상 순이익
- 실행 상태

중앙 차트:

- 기본 지표 `net executable spread`
- 전환 지표 `gross spread`, `buy price`, `sell price`, `FX`
- 범위 `1h / 24h / 7d`
- 현재값, 최고, 최저, 평균
- 진입 임계값 수평선
- 실제 cycle 시작/완료 marker

오른쪽 실행 rail:

- BUY ask/VWAP, 사용 level, 가용 quote 잔고
- SELL bid/VWAP, 사용 level, 가용 base 잔고
- buy fee / sell fee
- 예상 리밸런싱 비용
- gross/net spread와 예상 순이익
- 최신 호가 age

### 7.5 실행/재고 패널

- 승인 수량 slider와 직접 수량 입력
- `25 / 50 / 75 / MAX`
- 양쪽 잔고 중 작은 실행 가능 수량을 MAX로 사용
- 목표 net spread
- confirm duration
- 한 조각 최대 금액과 최소 금액
- Paper 실행 승인
- Live 실행 승인
- 일시정지/재개/중지
- 현재 journal state
- 양쪽 실제 체결량과 불균형 수량
- cycle별 실현 PnL
- 거래소별 base/quote 재고와 목표 band
- 리밸런싱 필요 수량

Live 승인 dialog에는 symbol 또는 route 확인 문자열을 요구하고 승인 만료시각을 표시한다.

### 7.6 알림 설정

- symbol 또는 route
- 조건: gross spread, net spread, inventory 부족, transfer 상태, reconcile
- 임계값
- cooldown
- 브라우저/Telegram 채널

### 7.7 이벤트 로그

운영자가 바로 원인을 알 수 있는 event code를 사용한다.

```text
BOT_STARTED
VENUE_WS_RECONNECTING
BOOK_STALE
OPPORTUNITY_CONFIRMED
CYCLE_ARMED
ORDER_INTENT_RECORDED
FIRST_LEG_FILLED
SECOND_LEG_FILLED
CYCLE_COMPLETE
RECONCILE_REQUIRED
INVENTORY_LIMIT_REACHED
CIRCUIT_BREAKER
```

반복 reconnect 메시지는 집계하고 동일 오류를 매초 새 행으로 만들지 않는다.

### 7.8 반응형 및 시각 원칙

- 어두운 운영형 UI와 높은 정보 밀도
- page section을 카드처럼 띄우지 않음
- 반복 항목, modal, 실제 도구만 card 사용
- 표 header sticky
- 색상만으로 상태를 전달하지 않고 텍스트 병기
- 숫자는 tabular-nums 사용
- 긴 market명과 오류 메시지가 container 밖으로 나오지 않게 함
- 모바일에서는 열을 무작정 숨기기보다 route summary와 상세 drawer로 전환
- icon button에는 tooltip 제공

---

## 8. 현물-현물 구현 순서

### Phase 0. 범위와 계약 고정

- 재고 선배치형만 실거래 대상으로 확정
- 지원 거래소, quote 통화, 첫 allowlist 자산 확정
- common accounting currency 확정(KRW 또는 USDT)
- fee tier와 FX 변환 정책 확정
- `SpotSpotOpportunity` JSON 계약부터 테스트 작성

완료 기준: 같은 fixture로 gross/net spread가 항상 동일하게 계산됨.

### Phase 1. 공개 마켓 데이터

- 거래소별 market discovery
- asset identity와 network 정보 정규화
- WebSocket depth snapshot/delta
- sequence gap, reconnect, stale 처리
- route dirty queue/coalescing
- fixed notional VWAP와 양방향 route 계산

완료 기준: UI 없이도 전체 route snapshot을 안정적으로 출력하고 가짜 gap fixture를 차단함.

### Phase 2. 수수료, FX, 위험

- 거래소별 maker/taker fee tier
- quote별 실행 가능한 FX 변환
- 입출금/거래 상태
- withdrawal fee와 network 정보
- gross/net spread 분리
- identity mismatch와 risk gate

완료 기준: 수수료 차감 후 음수 기회가 `ENTRY_READY`로 나오지 않음.

### Phase 3. 계정과 재고

- read-only private API
- base/quote available/locked balance
- 봇 주문 reservation
- inventory target band와 runway
- 잔고 mismatch preflight

완료 기준: 실제 양 거래소 잔고 중 작은 수량 이상을 승인할 수 없음.

### Phase 4. Paper cycle

- 실제 orderbook을 소진하는 두 레그 Paper 체결
- 부분 체결, 수수료, inventory delta
- 작은 조각 반복
- stale/depth/condition pause
- journal과 restart recovery simulation
- 24시간 이상 shadow run

완료 기준: duplicate snapshot이나 재시작으로 같은 slice가 두 번 실행되지 않음.

### Phase 5. 대시보드와 알림

- 전체 기회 테이블과 필터
- route 상세 차트
- inventory/rebalance 패널
- Paper 승인과 상태 머신
- event log와 Telegram
- 1h/24h/7d 집계 이력

완료 기준: 서버 로그 없이도 UI만 보고 대기 이유와 잔고 불일치를 알 수 있음.

### Phase 6. 실주문 어댑터

- 각 거래소 IOC 시장성 지정가
- client order ID
- 주문 조회/취소/체결 조회
- private order WebSocket
- fee/average price 실제 응답 파싱
- 거래소별 minimum/tick/step 직전 재검증

완료 기준: 각 거래소에서 최소 금액 단일 주문을 내고 재조회 결과가 DB와 일치함.

### Phase 7. 최소 금액 실거래

- route/asset allowlist 한 개로 제한
- 작은 조각 한 번만 실행
- 첫 레그 실제 체결량만큼 두 번째 레그 실행
- unknown order 상태에서 재주문 금지
- mismatch 시 `RECONCILE_REQUIRED`
- 일일 손실/노출 circuit breaker

완료 기준: API timeout과 프로세스 재시작 테스트에서 중복 주문이 없음.

### Phase 8. 반복 실행과 리밸런싱

- inventory band 기반 route 제한
- 여러 cycle의 자본 배분
- venue/asset별 동시성 제한
- 수동 리밸런싱 추천
- 리밸런싱 비용을 다음 기회의 net spread에 반영
- 충분히 검증한 뒤에만 자동 전송을 별도 서비스로 검토

완료 기준: 수익이 나더라도 한 거래소 재고를 소진하는 방향으로 무한 진입하지 않음.

### Phase 9. 운영 강화

- private WebSocket과 REST reconciliation
- rate-limit token bucket
- NTP/clock drift 감시
- 거래소 장애 circuit breaker
- 메트릭, health endpoint, structured log
- DB backup과 schema migration
- chaos test와 장시간 soak test
- 검증된 hot path의 Rust 이전 검토

---

## 9. 우선순위가 높은 안전장치

1. 주문 intent를 DB에 먼저 기록하고 API를 호출한다.
2. timeout이면 client order ID로 조회하기 전 재주문하지 않는다.
3. 한 레그 체결량보다 두 번째 레그 주문량을 크게 만들지 않는다.
4. 두 레그 불균형 상태에서는 신규 cycle을 차단한다.
5. WebSocket book이 stale이면 REST 가격만으로 자동 실행하지 않는다. REST는 확인용이다.
6. 계정 잔고와 DB reservation이 다르면 자동 정지한다.
7. 실제 수수료와 fee currency를 fill마다 기록한다.
8. 일일 손실, 최대 unmatched exposure, 최대 주문 금액을 서버에서 강제한다.
9. 출금 권한 없는 API 키로 시작한다.
10. 모든 Live 기능은 환경 잠금 + route 승인 + TTL을 요구한다.

---

## 10. Claude에게 전달할 작업 지시

아래 문장을 새 프로젝트 요청의 시작에 붙이면 된다.

```text
첨부한 SPOT_SPOT_ARBITRAGE_HANDOFF.md를 구현 기준으로 사용해라.
현재 코드베이스를 먼저 읽고 기존 구조와 스타일을 존중해라.

첫 실거래 범위는 INVENTORY_ARBITRAGE다. 주문 과정에서 자동 출금은 하지 않는다.
가격 차이는 최우선 호가가 아니라 설정 금액의 양쪽 executable VWAP로 계산하고,
수수료, 방향별 FX, 예상 리밸런싱 비용을 차감한 net spread를 진입 기준으로 사용해라.

실주문 전에 public scanner -> fee/risk -> private inventory -> Paper journal -> dashboard
순서로 구현해라. 주문 API를 조기에 연결하지 마라.

모든 주문은 intent-first journal, client order ID, restart recovery를 가져야 한다.
첫 레그 실제 체결량만큼만 두 번째 레그를 실행하고, 불균형이면 신규 주문을 중지해라.

대시보드는 랜딩 페이지가 아니라 첫 화면부터 운영형 전체 기회 테이블을 보여줘라.
구현 단계마다 테스트를 추가하고 실행 결과와 남은 위험을 보고해라.
```

---

## 11. 첫 MVP 완료 기준

- 최소 2개 현물 거래소의 전체 공통 마켓 탐색
- 양방향 route의 고정 금액 executable VWAP
- 수수료/FX를 차감한 net spread
- stale, depth, identity, 입출금 위험 gate
- 실제 계정 잔고와 inventory runway
- Paper 두 레그 부분 체결과 재고 delta
- SQLite 주문 journal과 재시작 복구 테스트
- 1h/24h/7d 이력 대시보드
- 최소 금액 Live 1개 allowlist route
- 한 레그 미체결과 timeout에서 중복 주문 없음
- reconcile 완료 전 신규 cycle 차단
- 거래소별 실제 잔고와 DB 수량 일치

이 기준을 만족하기 전에는 “전체 마켓 자동 실거래”를 켜지 않는다.
