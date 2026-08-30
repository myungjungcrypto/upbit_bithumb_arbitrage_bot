"""빗썸 주문 어댑터 — 신 API v1 (업비트 호환 스키마) + 빗썸 JWT(timestamp 포함).

업비트 어댑터를 상속해 엔드포인트·서명만 교체한다. 응답 스키마가 동일해 파서 공용.
⚠️ 실측 확인 항목 (P3 전, V9로 등재): 빗썸 신 API의 time_in_force=ioc 지원 여부 —
미지원 응답이면 주문이 거부되어 안전하게 실패하며, 그 경우 ord_type 조합을 실측해 조정한다.
키: BITHUMB_TRADE_API_KEY / SECRET — 주문 권한만, 출금 권한 금지 (§4.1 키 3계층).
"""
from __future__ import annotations

from ..collectors.wallet_bithumb import make_bithumb_jwt
from .upbit import UpbitOrderAdapter

ORDERS_URL = "https://api.bithumb.com/v1/orders"
ORDER_URL = "https://api.bithumb.com/v1/order"


class BithumbOrderAdapter(UpbitOrderAdapter):
    exchange = "bithumb"
    orders_url = ORDERS_URL
    order_url = ORDER_URL

    def _jwt(self, params: dict) -> str:
        return make_bithumb_jwt(self.access_key, self.secret_key, params)
