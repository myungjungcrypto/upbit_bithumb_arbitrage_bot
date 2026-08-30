"""실주문 어댑터 계층 (M3ⓑ) — IOC 시장성 지정가 + client order ID + 실체결 파싱.

계약 (§4.1 방어선 6, T7):
  - 어댑터는 '멍청한' API 클라이언트다: 가격·수량·client_id를 받아 그대로 발사하고 결과를 정규화한다.
    사이징·엣지 판단·저널은 상위(ExecutionJournal, M3ⓒ)의 몫.
  - 모든 place_*는 이중 잠금 뒤에 있다: 환경 LIVE_TRADING_ALLOWED=1 ∧ 생성자 allow_live=True.
    어느 하나라도 없으면 LiveLockError — 상위 버그가 있어도 주문이 나가지 않는다.
  - timeout 후 재주문 금지: get_order(client_id)로 기존 주문을 조회해 상태를 복구한다 (인계서 §9).
  - 거래(주문) 키는 조회 키와 별도 환경변수 (*_TRADE_*) — 출금 권한 없는 키만 (§4.1 키 3계층).
"""
from .base import LiveLockError, OrderAdapter, OrderResult, live_allowed, make_client_id

__all__ = ["LiveLockError", "OrderAdapter", "OrderResult", "live_allowed", "make_client_id"]
