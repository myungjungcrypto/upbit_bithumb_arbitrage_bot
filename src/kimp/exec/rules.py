"""주문 규칙 — 호가 단위(tick)·수량 단위(step)·최소 주문금액, IOC 시장성 지정가 산출.

원칙: 규칙이 틀리면 거래소가 주문을 '거부'한다 = 안전한 실패. 그래도 거부는 기회 상실이므로
국내 호가 단위 표는 공식 개편(2023-10 업비트 / 2024 빗썸) 기준으로 유지하고 config로 덮어쓸 수 있게 한다.
OKX는 instruments API가 정확한 tickSz/lotSz/minSz를 주므로 정적 표가 필요 없다.

IOC 가격 방향 (T7 — 시장성 지정가):
  매수 = best_ask × (1+margin)을 tick으로 **올림** (내림하면 ask 아래로 떨어져 미체결 위험)
  매도 = best_bid × (1−margin)을 tick으로 **내림**
margin(기본 0.2%)이 슬리피지 상한 역할을 겸한다 — 그 이상 불리한 레벨은 IOC가 자동으로 남기고 취소.
"""
from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_DOWN, Decimal

from ..models import D

# (하한가, tick) — 가격이 하한가 이상이면 그 tick. 내림차순 평가. 2023-10 개편 반영
UPBIT_KRW_TICKS: tuple[tuple[Decimal, Decimal], ...] = tuple(
    (D(a), D(b)) for a, b in [
        (2_000_000, 1000), (1_000_000, 500), (500_000, 100), (100_000, 50),
        (10_000, 10), (1_000, 1), (100, "0.1"), (10, "0.01"),
        (1, "0.001"), ("0.1", "0.0001"), ("0.01", "0.00001"), (0, "0.000001"),
    ]
)

# 빗썸 KRW 호가 단위 — 공식 표 기준, P3 전 실측 재확인 대상 (틀리면 주문 거부로 안전 실패)
BITHUMB_KRW_TICKS: tuple[tuple[Decimal, Decimal], ...] = tuple(
    (D(a), D(b)) for a, b in [
        (1_000_000, 1000), (500_000, 500), (100_000, 100), (50_000, 50),
        (10_000, 10), (5_000, 5), (1_000, 1), (100, "0.1"),
        (10, "0.01"), (1, "0.001"), (0, "0.0001"),
    ]
)

MIN_NOTIONAL_KRW = D(5000)      # 업비트·빗썸 최소 주문금액
QTY_STEP_KRW = D("0.00000001")  # 국내 수량 소수 8자리


def krw_tick(price: Decimal, table: tuple[tuple[Decimal, Decimal], ...]) -> Decimal:
    for floor_price, tick in table:
        if price >= floor_price:
            return tick
    return table[-1][1]


def round_to_tick(price: Decimal, tick: Decimal, up: bool) -> Decimal:
    q = (price / tick).quantize(Decimal(1), rounding=ROUND_CEILING if up else ROUND_DOWN)
    return q * tick


def ioc_buy_price(best_ask: Decimal, margin: Decimal, tick: Decimal) -> Decimal:
    return round_to_tick(best_ask * (1 + margin), tick, up=True)


def ioc_sell_price(best_bid: Decimal, margin: Decimal, tick: Decimal) -> Decimal:
    return round_to_tick(best_bid * (1 - margin), tick, up=False)


def quantize_qty(qty: Decimal, step: Decimal) -> Decimal:
    """수량은 항상 내림 — 잔고·상대 깊이를 초과하지 않는 방향."""
    if step <= 0:
        return qty
    return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step


def check_min_notional(price: Decimal, qty: Decimal, min_notional: Decimal) -> bool:
    return price * qty >= min_notional
