"""업비트 주문 어댑터 — POST /v1/orders (ord_type=limit + time_in_force=ioc), identifier 멱등 조회.

특징:
  - identifier(우리 client_id)를 주문 생성에 실어 보내면 GET /v1/order?identifier=로 재조회 가능
    — timeout 후 재주문 금지·상태 복구의 근거 (인계서 §9). identifier는 재사용 불가(멱등)
  - 접수 응답은 state=wait로 시작할 수 있음 → 최종 체결은 GET 상세(trades 포함)로 확인
  - KRW 마켓 수수료는 KRW로 부과 (paid_fee)
키: UPBIT_TRADE_ACCESS_KEY / SECRET — 주문 권한만, 출금 권한 금지 (§4.1 키 3계층).
"""
from __future__ import annotations

from decimal import Decimal

import aiohttp

from ..collectors.wallet_upbit import make_jwt
from ..models import D
from .base import OrderAdapter, OrderError, OrderResult

ORDERS_URL = "https://api.upbit.com/v1/orders"
ORDER_URL = "https://api.upbit.com/v1/order"


def parse_upbit_order(d: dict, exchange: str = "upbit", client_id: str = "") -> OrderResult:
    """주문 상세 응답 → OrderResult (순수 함수 — 빗썸 신 API도 동일 스키마라 공용).

    상태 매핑: done→filled / cancel→partial(체결분 있음)·canceled(0) / wait·watch→open.
    평균가는 trades의 (Σfunds/Σvolume) — 상세 조회에만 trades가 있고, 없으면 None."""
    if not isinstance(d, dict) or "state" not in d:
        return OrderResult(exchange, "", client_id, "unknown", raw=d if isinstance(d, dict) else {})
    state = d.get("state", "")
    filled = D(d.get("executed_volume") or 0)
    if state == "done":
        status = "filled"
    elif state == "cancel":
        status = "partial" if filled > 0 else "canceled"
    elif state in ("wait", "watch"):
        status = "open"
    else:
        status = "unknown"
    trades = d.get("trades") or []
    funds = sum((D(t.get("funds") or 0) for t in trades), D(0))
    vol = sum((D(t.get("volume") or 0) for t in trades), D(0))
    market = str(d.get("market") or "")
    return OrderResult(
        exchange=exchange,
        order_id=str(d.get("uuid") or ""),
        client_id=str(d.get("identifier") or client_id),
        status=status,
        filled_qty=filled,
        avg_price=(funds / vol) if vol > 0 else None,
        fee=D(d.get("paid_fee") or 0),
        fee_currency=market.split("-", 1)[0] if "-" in market else "KRW",
        raw=d,
    )


class UpbitOrderAdapter(OrderAdapter):
    exchange = "upbit"
    orders_url = ORDERS_URL
    order_url = ORDER_URL

    def __init__(self, access_key: str, secret_key: str, allow_live: bool = False) -> None:
        super().__init__(allow_live)
        self.access_key = access_key
        self.secret_key = secret_key

    def _jwt(self, params: dict) -> str:
        return make_jwt(self.access_key, self.secret_key, params)

    async def place_ioc(
        self, sess: aiohttp.ClientSession, side: str, base: str, quote: str,
        price: Decimal, qty: Decimal, client_id: str,
    ) -> OrderResult:
        self._guard()
        params = {
            "market": f"{quote}-{base}",
            "side": "bid" if side == "buy" else "ask",
            "volume": str(qty),
            "price": str(price),
            "ord_type": "limit",
            "time_in_force": "ioc",
            "identifier": client_id,
        }
        async with sess.post(
            self.orders_url, json=params,
            headers={"Authorization": f"Bearer {self._jwt(params)}"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise OrderError(f"{self.exchange} 주문 거부: {data}")
        # 접수 직후 state=wait일 수 있음 — IOC는 곧 종결되므로 상세(trades 포함)로 최종 확인
        return await self.get_order(sess, base, quote, client_id=client_id)

    async def get_order(
        self, sess: aiohttp.ClientSession, base: str, quote: str,
        order_id: str = "", client_id: str = "",
    ) -> OrderResult:
        params = {"uuid": order_id} if order_id else {"identifier": client_id}
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        async with sess.get(
            f"{self.order_url}?{qs}",
            headers={"Authorization": f"Bearer {self._jwt(params)}"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json(content_type=None)
        return parse_upbit_order(data if isinstance(data, dict) else {}, self.exchange, client_id)
