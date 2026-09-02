"""바이낸스 주문 어댑터 — POST /api/v3/order (LIMIT + IOC, newClientOrderId 멱등), 규칙은 exchangeInfo.

첫 실거래 경로(2026-09-02 운용자 결정: 바낸 IN). 특징:
  - newOrderRespType=FULL → 접수 응답에 fills(체결가·수수료·수수료통화)가 포함돼 추가 조회 불필요
  - IOC 종결 상태: FILLED / EXPIRED(잔량 취소 — executedQty>0이면 partial)
  - origClientOrderId로 재조회 (timeout 복구), 수수료는 myTrades에서 보강
  - 서명: 쿼리스트링 HMAC-SHA256 (wallet_binance.sign_query 재사용 — percent-encoding 규칙 동일)
키: BINANCE_TRADE_API_KEY / SECRET — Spot Trading만, 출금 권한 금지, IP 제한 (§4.1 키 3계층).
"""
from __future__ import annotations

import time
from decimal import Decimal

import aiohttp

from ..collectors.wallet_binance import sign_query
from ..models import D
from .base import OrderAdapter, OrderError, OrderResult

BASE = "https://api.binance.com"


def _fmt(x: Decimal) -> str:
    return format(x.normalize(), "f")


def parse_binance_order(d: dict, client_id: str = "", trades: list | None = None) -> OrderResult:
    """주문 응답(FULL) 또는 조회 응답 → OrderResult (순수 함수). 수수료는 fills 또는 trades에서 합산."""
    if not isinstance(d, dict) or "status" not in d:
        return OrderResult("binance", "", client_id, "unknown", raw=d if isinstance(d, dict) else {})
    st = str(d.get("status", ""))
    filled = D(d.get("executedQty") or 0)
    if st == "FILLED":
        status = "filled"
    elif st in ("EXPIRED", "CANCELED", "EXPIRED_IN_MATCH"):
        status = "partial" if filled > 0 else "canceled"
    elif st in ("NEW", "PARTIALLY_FILLED"):
        status = "open"
    else:
        status = "unknown"
    cum = D(d.get("cummulativeQuoteQty") or 0)
    fee, fee_ccy = D(0), ""
    for f in (d.get("fills") or []) + list(trades or []):
        fee += D(f.get("commission") or 0)
        fee_ccy = fee_ccy or str(f.get("commissionAsset") or "")
    return OrderResult(
        exchange="binance", order_id=str(d.get("orderId") or ""),
        client_id=str(d.get("clientOrderId") or d.get("origClientOrderId") or client_id),
        status=status, filled_qty=filled,
        avg_price=(cum / filled) if filled > 0 else None,
        fee=fee, fee_currency=fee_ccy, raw=d,
    )


def parse_exchange_info(data: dict, symbol: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """exchangeInfo → (tick, step, min_qty, min_notional). 순수 함수."""
    for s in (data or {}).get("symbols") or []:
        if s.get("symbol") != symbol:
            continue
        tick = step = min_qty = min_notional = D(0)
        for f in s.get("filters") or []:
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick = D(f.get("tickSize") or 0)
            elif t == "LOT_SIZE":
                step, min_qty = D(f.get("stepSize") or 0), D(f.get("minQty") or 0)
            elif t in ("NOTIONAL", "MIN_NOTIONAL"):
                min_notional = D(f.get("minNotional") or 0)
        return tick, step, min_qty, min_notional
    raise OrderError(f"binance symbol 없음: {symbol}")


class BinanceOrderAdapter(OrderAdapter):
    exchange = "binance"

    def __init__(self, api_key: str, api_secret: str, allow_live: bool = False) -> None:
        super().__init__(allow_live)
        self.api_key = api_key
        self.api_secret = api_secret
        self._rules: dict[str, tuple[Decimal, Decimal, Decimal, Decimal]] = {}

    def _qs(self, params: dict) -> str:
        return sign_query(self.api_secret, {**params, "timestamp": int(time.time() * 1000), "recvWindow": 10000})

    async def instrument_rules(self, sess: aiohttp.ClientSession, base: str, quote: str = "USDT"):
        sym = f"{base}{quote}"
        if sym in self._rules:
            return self._rules[sym]
        async with sess.get(f"{BASE}/api/v3/exchangeInfo?symbol={sym}", timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json(content_type=None)
        rules = parse_exchange_info(data, sym)
        self._rules[sym] = rules
        return rules

    async def place_ioc(self, sess, side, base, quote, price: Decimal, qty: Decimal, client_id: str) -> OrderResult:
        self._guard()
        params = {"symbol": f"{base}{quote}", "side": side.upper(), "type": "LIMIT", "timeInForce": "IOC",
                  "quantity": _fmt(qty), "price": _fmt(price), "newClientOrderId": client_id,
                  "newOrderRespType": "FULL"}
        async with sess.post(f"{BASE}/api/v3/order?{self._qs(params)}", headers={"X-MBX-APIKEY": self.api_key},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
            if r.status >= 400:
                raise OrderError(f"binance 주문 거부: {data}")
        return parse_binance_order(data, client_id)

    async def get_order(self, sess, base, quote, order_id: str = "", client_id: str = "") -> OrderResult:
        sym = f"{base}{quote}"
        p = {"symbol": sym, **({"orderId": order_id} if order_id else {"origClientOrderId": client_id})}
        async with sess.get(f"{BASE}/api/v3/order?{self._qs(p)}", headers={"X-MBX-APIKEY": self.api_key},
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
        trades: list = []
        if isinstance(data, dict) and data.get("orderId"):
            try:
                async with sess.get(f"{BASE}/api/v3/myTrades?{self._qs({'symbol': sym, 'orderId': data['orderId']})}",
                                    headers={"X-MBX-APIKEY": self.api_key}, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                    t = await r2.json(content_type=None)
                    trades = t if isinstance(t, list) else []
            except Exception:
                pass
        return parse_binance_order(data if isinstance(data, dict) else {}, client_id, trades)
