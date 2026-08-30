"""OKX 주문 어댑터 — POST /api/v5/trade/order (ordType=ioc), clOrdId 멱등 조회.

P3 1순위 라이브 거래소 (§6.1 M3). 특징:
  - ordType "ioc" = 시장성 지정가 즉시체결·잔량취소 — T7 확정 전술 그대로
  - 주문 접수 응답에는 체결 정보가 없음 → 접수 직후 GET으로 최종 상태 조회 (IOC는 즉시 종결)
  - clOrdId(≤32 영숫자)로 timeout 후 재조회 — 재주문 없이 상태 복구 (인계서 §9)
  - 규칙(tickSz/lotSz/minSz)은 public instruments에서 조회·캐시 — 정적 표 불필요
키: OKX_TRADE_API_KEY / SECRET / PASSPHRASE — 거래 권한만, 출금 권한 금지 (§4.1 키 3계층).
"""
from __future__ import annotations

import json
from decimal import Decimal

import aiohttp

from ..collectors.wallet_okx import okx_timestamp, sign_okx
from ..models import D
from .base import OrderAdapter, OrderError, OrderResult

BASE = "https://www.okx.com"


def parse_okx_order(data: dict, client_id: str = "") -> OrderResult:
    """GET /api/v5/trade/order 응답 → OrderResult (순수 함수).

    상태 매핑: filled→filled / canceled→partial(체결분 있음)·canceled(0) / live·partially_filled→open.
    OKX fee는 지불 시 음수 → 양수로 정규화."""
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not rows:
        return OrderResult("okx", "", client_id, "unknown", raw=data if isinstance(data, dict) else {})
    r = rows[0]
    state = r.get("state", "")
    filled = D(r.get("accFillSz") or 0)
    if state == "filled":
        status = "filled"
    elif state in ("canceled", "mmp_canceled"):
        status = "partial" if filled > 0 else "canceled"
    elif state in ("live", "partially_filled"):
        status = "open"
    else:
        status = "unknown"
    avg = r.get("avgPx")
    fee = D(r.get("fee") or 0)
    return OrderResult(
        exchange="okx",
        order_id=str(r.get("ordId") or ""),
        client_id=str(r.get("clOrdId") or client_id),
        status=status,
        filled_qty=filled,
        avg_price=D(avg) if avg not in (None, "") else None,
        fee=-fee if fee < 0 else fee,
        fee_currency=str(r.get("feeCcy") or ""),
        raw=r,
    )


class OkxOrderAdapter(OrderAdapter):
    exchange = "okx"

    def __init__(self, api_key: str, api_secret: str, passphrase: str, allow_live: bool = False) -> None:
        super().__init__(allow_live)
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self._rules: dict[str, tuple[Decimal, Decimal, Decimal]] = {}  # inst → (tick, lot, min_sz)

    def _headers(self, method: str, path: str, body: str = "") -> dict:
        ts = okx_timestamp()
        return {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign_okx(self.api_secret, ts, method, path, body),
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }

    async def instrument_rules(self, sess: aiohttp.ClientSession, base: str, quote: str = "USDT") -> tuple[Decimal, Decimal, Decimal]:
        """(tick, lot, min_sz) — 공개 API, 캐시. 주문 직전 가격·수량 정규화의 근거 (T7 재검증)."""
        inst = f"{base}-{quote}"
        if inst in self._rules:
            return self._rules[inst]
        async with sess.get(
            f"{BASE}/api/v5/public/instruments?instType=SPOT&instId={inst}",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        rows = (data or {}).get("data") or []
        if not rows:
            raise OrderError(f"okx instrument 없음: {inst}")
        r = rows[0]
        rules = (D(r["tickSz"]), D(r["lotSz"]), D(r["minSz"]))
        self._rules[inst] = rules
        return rules

    async def place_ioc(
        self, sess: aiohttp.ClientSession, side: str, base: str, quote: str,
        price: Decimal, qty: Decimal, client_id: str,
    ) -> OrderResult:
        self._guard()
        path = "/api/v5/trade/order"
        body = json.dumps({
            "instId": f"{base}-{quote}", "tdMode": "cash", "side": side,
            "ordType": "ioc", "px": str(price), "sz": str(qty), "clOrdId": client_id,
        })
        async with sess.post(
            f"{BASE}{path}", data=body, headers=self._headers("POST", path, body),
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json(content_type=None)
        rows = (data or {}).get("data") or []
        if data.get("code") != "0" or not rows or rows[0].get("sCode") != "0":
            msg = rows[0].get("sMsg") if rows else data.get("msg")
            raise OrderError(f"okx 주문 거부: {msg} (code={data.get('code')})")
        # 접수 응답엔 체결 정보 없음 — IOC는 즉시 종결되므로 바로 최종 상태 조회
        return await self.get_order(sess, base, quote, client_id=client_id)

    async def get_order(
        self, sess: aiohttp.ClientSession, base: str, quote: str,
        order_id: str = "", client_id: str = "",
    ) -> OrderResult:
        q = f"instId={base}-{quote}&" + (f"ordId={order_id}" if order_id else f"clOrdId={client_id}")
        path = f"/api/v5/trade/order?{q}"
        async with sess.get(
            f"{BASE}{path}", headers=self._headers("GET", path),
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json(content_type=None)
        return parse_okx_order(data if isinstance(data, dict) else {}, client_id)
