"""이원 김프 + 호가 깊이 기반 왕복 엣지 계산 (PLAN §2).

- 이론 김프: 국내mid / (해외mid × USD/KRW 고시환율) − 1        → 분석·시장상황용
- 실행 김프: 국내mid / (해외mid × USDT/KRW 실거래가) − 1        → 트리거·손익용
- 왕복 엣지: 금액대별로 호가창을 실제로 걸어가며(VWAP) 3-레그 왕복을 시뮬레이션
    inbound  = 해외 매수(asks) → 국내 매도(bids) → 국내 USDT 매수(asks)   [USDT 기준 수익률]
    outbound = 국내 매수(asks) → 해외 매도(bids) → 국내 USDT 매도(bids)   [KRW 기준 수익률]
  복귀 레그(USDT)가 수식에 포함되므로 USDT 김프를 되뱉는 비용이 자동 반영된다 (PLAN §1.1).
"""
from __future__ import annotations

from decimal import Decimal

from ..models import Book, Level


def vwap_buy(asks: tuple[Level, ...], quote_notional: Decimal) -> tuple[Decimal, Decimal] | None:
    """quote_notional 만큼 asks를 걸어가며 매수. (체결 base 수량, 평균가) 반환.
    깊이가 부족하면 None — 그 금액대에서는 기회가 존재하지 않는 것으로 취급."""
    if quote_notional <= 0:
        return None
    remaining = quote_notional
    qty = Decimal(0)
    for lv in asks:
        level_cost = lv.price * lv.size
        if level_cost >= remaining:
            qty += remaining / lv.price
            return qty, quote_notional / qty
        qty += lv.size
        remaining -= level_cost
    return None


def vwap_sell(bids: tuple[Level, ...], base_qty: Decimal) -> tuple[Decimal, Decimal] | None:
    """base_qty 만큼 bids를 걸어가며 매도. (수취 quote 금액, 평균가) 반환. 깊이 부족 시 None."""
    if base_qty <= 0:
        return None
    remaining = base_qty
    out = Decimal(0)
    for lv in bids:
        take = min(remaining, lv.size)
        out += take * lv.price
        remaining -= take
        if remaining == 0:
            return out, out / base_qty
    return None


def inbound_gross_edge(
    ovs: Book, dom: Book, usdtkrw: Book, notional_usdt: Decimal
) -> Decimal | None:
    """김프 방향 왕복 총엣지 (수수료·전송비 미포함).
    해외에서 notional_usdt로 코인 매수 → 국내에서 매도 → 원화로 USDT 재매수했을 때
    (돌아온 USDT / 투입 USDT) − 1."""
    r1 = vwap_buy(ovs.asks, notional_usdt)
    if r1 is None:
        return None
    qty, _ = r1
    r2 = vwap_sell(dom.bids, qty)
    if r2 is None:
        return None
    krw, _ = r2
    r3 = vwap_buy(usdtkrw.asks, krw)
    if r3 is None:
        return None
    usdt_out, _ = r3
    return usdt_out / notional_usdt - 1


def outbound_gross_edge(
    dom: Book, ovs: Book, usdtkrw: Book, notional_krw: Decimal
) -> Decimal | None:
    """역프 방향 왕복 총엣지. 국내에서 notional_krw로 코인 매수 → 해외에서 매도 →
    받은 USDT를 국내 시세(bids)로 원화 환산했을 때 (환산 KRW / 투입 KRW) − 1."""
    r1 = vwap_buy(dom.asks, notional_krw)
    if r1 is None:
        return None
    qty, _ = r1
    r2 = vwap_sell(ovs.bids, qty)
    if r2 is None:
        return None
    usdt, _ = r2
    r3 = vwap_sell(usdtkrw.bids, usdt)
    if r3 is None:
        return None
    krw_out, _ = r3
    return krw_out / notional_krw - 1


def theo_premium(dom: Book, ovs: Book, usd_krw: Decimal) -> Decimal | None:
    dm, om = dom.mid, ovs.mid
    if dm is None or om is None or usd_krw <= 0:
        return None
    return dm / (om * usd_krw) - 1


def exec_premium(dom: Book, ovs: Book, usdtkrw: Book) -> Decimal | None:
    dm, om, um = dom.mid, ovs.mid, usdtkrw.mid
    if dm is None or om is None or um is None:
        return None
    return dm / (om * um) - 1
