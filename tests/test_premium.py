"""왕복 엣지·VWAP 수학 검증 — 이 수식이 시스템의 심장 (PLAN §2)."""
from decimal import Decimal

from kimp.models import Book, D, Level
from kimp.engine.premium import (
    exec_premium,
    inbound_gross_edge,
    outbound_gross_edge,
    theo_premium,
    vwap_buy,
    vwap_sell,
)


def levels(*pairs) -> tuple[Level, ...]:
    return tuple(Level(D(p), D(s)) for p, s in pairs)


def book(exchange, base, quote, bids, asks) -> Book:
    return Book(exchange, base, quote, bids, asks, None, 0)


# ---------- VWAP ----------

def test_vwap_buy_single_level():
    qty, avg = vwap_buy(levels((100, 10)), D(500))
    assert qty == D(5)
    assert avg == D(100)


def test_vwap_buy_multi_level():
    # 100원 × 1개 소진 후 110원 레벨에서 나머지 110원어치(1개) → 총 2개, 평균 105
    qty, avg = vwap_buy(levels((100, 1), (110, 2)), D(210))
    assert qty == D(2)
    assert avg == D(105)


def test_vwap_buy_insufficient_depth():
    assert vwap_buy(levels((100, 1)), D(1000)) is None


def test_vwap_sell_multi_level():
    out, avg = vwap_sell(levels((100, 1), (90, 5)), D(3))
    assert out == D(100) + D(180)
    assert avg == D(280) / 3


def test_vwap_sell_insufficient_depth():
    assert vwap_sell(levels((100, 1)), D(2)) is None


def test_vwap_zero_notional():
    assert vwap_buy(levels((100, 1)), D(0)) is None
    assert vwap_sell(levels((100, 1)), D(0)) is None


# ---------- 왕복 엣지 ----------

def _books(dom_bid=141000, usdt_ask=1400, usdt_bid=1399):
    ovs = book("binance", "XRP", "USDT", levels((99, 1000)), levels((100, 1000)))
    dom = book("upbit", "XRP", "KRW", levels((dom_bid, 100000)), levels((dom_bid + 500, 100000)))
    usdtkrw = book("upbit", "USDT", "KRW", levels((usdt_bid, 10**7)), levels((usdt_ask, 10**7)))
    return ovs, dom, usdtkrw


def test_inbound_roundtrip_positive_edge():
    # 해외 100 USDT/XRP, 국내 141,000 KRW/XRP, USDT/KRW 1,400
    # 1000 USDT → 10 XRP → 1,410,000 KRW → 1,007.142857... USDT → +0.714...%
    ovs, dom, usdtkrw = _books()
    edge = inbound_gross_edge(ovs, dom, usdtkrw, D(1000))
    expected = D(1410000) / D(1400) / D(1000) - 1
    assert abs(edge - expected) < Decimal("1e-15")
    assert edge > 0


def test_inbound_roundtrip_zero_edge_when_premium_equals_usdt():
    # 코인 김프 == USDT 김프이면 실행 김프 0 → 왕복해도 남는 게 없어야 함 (PLAN §1.1)
    ovs, dom, usdtkrw = _books(dom_bid=140000, usdt_ask=1400)
    edge = inbound_gross_edge(ovs, dom, usdtkrw, D(1000))
    assert edge == 0


def test_outbound_roundtrip():
    # 국내에서 141,500 KRW로 매수, 해외 99에 매도, USDT를 1,399에 원화 환산
    ovs, dom, usdtkrw = _books()
    notional_krw = D(1415000)  # 10 XRP어치
    edge = outbound_gross_edge(dom, ovs, usdtkrw, notional_krw)
    # 10 XRP → 990 USDT → 1,385,010 KRW → 손실 (김프 상황의 outbound는 음수여야 정상)
    expected = D(990) * D(1399) / notional_krw - 1
    assert abs(edge - expected) < Decimal("1e-15")
    assert edge < 0


def test_roundtrip_depth_exhaustion_returns_none():
    ovs, dom, usdtkrw = _books()
    thin_dom = book("upbit", "XRP", "KRW", levels((141000, 1)), levels((141500, 1)))
    assert inbound_gross_edge(ovs, thin_dom, usdtkrw, D(1000)) is None


# ---------- capacity (사이징 자동화) ----------

def test_capacity_at_threshold_basic():
    from kimp.engine.premium import capacity_at_threshold

    # net(n): 5000 이하에서 1%, 그 위에서 0% — 경계가 [5000, 10000) 사이에서 잡혀야 함
    def net(n):
        return D("0.01") if n <= 5000 else D("0")

    cap = capacity_at_threshold(net, D("0.005"), lo=D(1000))
    assert cap is not None
    assert D(4999) <= cap <= D(10000)
    assert net(cap) >= D("0.005")


def test_capacity_none_when_no_opportunity():
    from kimp.engine.premium import capacity_at_threshold

    assert capacity_at_threshold(lambda n: D("0.001"), D("0.005")) is None
    assert capacity_at_threshold(lambda n: None, D("0.005")) is None


def test_capacity_with_real_books():
    from kimp.engine.premium import capacity_at_threshold, inbound_gross_edge

    # 해외 asks 100×50개(=5000 USDT어치)만 존재 → 그 너머는 깊이 부족(None)
    ovs = book("binance", "XRP", "USDT", levels((99, 10**6)), levels((100, 50)))
    dom = book("upbit", "XRP", "KRW", levels((145000, 10**6)), levels((145500, 10**6)))
    usdtkrw = book("upbit", "USDT", "KRW", levels((1399, 10**8)), levels((1400, 10**8)))

    def net(n):
        g = inbound_gross_edge(ovs, dom, usdtkrw, n)
        return None if g is None else g  # 수수료 0 가정

    cap = capacity_at_threshold(net, D("0.005"), lo=D(1000))
    # 엣지 자체는 ~3.5%로 임계 초과, 깊이 한계(5000 USDT)가 capacity를 결정
    assert cap is not None
    assert D(4000) <= cap <= D(5000)


# ---------- mid 김프 ----------

def test_theo_and_exec_premium():
    ovs, dom, usdtkrw = _books()
    # dom mid = 141250, ovs mid = 99.5
    theo = theo_premium(dom, ovs, D(1380))
    execp = exec_premium(dom, ovs, usdtkrw)
    assert abs(theo - (D(141250) / (D("99.5") * 1380) - 1)) < Decimal("1e-15")
    usdt_mid = (D(1399) + D(1400)) / 2
    assert abs(execp - (D(141250) / (D("99.5") * usdt_mid) - 1)) < Decimal("1e-15")
    # 이론 김프(환율 1380) > 실행 김프(USDT 1399.5) — USDT 김프만큼 차이
    assert theo > execp
