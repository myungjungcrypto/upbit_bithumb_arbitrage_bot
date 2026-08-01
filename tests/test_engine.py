"""PremiumEngine 동작 검증 — 스테일 배제, 행 산출, USDT 마켓 전파."""
import asyncio

from kimp.bus import Bus
from kimp.config import Config
from kimp.engine.books import BookStore
from kimp.engine.runner import PremiumEngine
from kimp.models import Book, D, Fx, Level, now_ms


CFG = Config(
    raw={
        "coins": ["XRP"],
        "domestic": ["upbit"],
        "overseas": ["binance"],
        "overseas_ref": "binance",
        "ladder_usd": [1000],
        "staleness": {"book_ms": 5000, "fx_ms": 120000},
        "engine": {"min_interval_ms": 0},
        "fees": {"taker": {"upbit": 0.0005, "binance": 0.001}},
        "withdraw_fee_usd_est": {"default": 2.0, "USDT": 1.0, "XRP": 0.2},
    }
)


def _mk_book(exchange, base, quote, bid, ask, size=10**6, ts=None):
    return Book(
        exchange,
        base,
        quote,
        (Level(D(bid), D(size)),),
        (Level(D(ask), D(size)),),
        None,
        ts if ts is not None else now_ms(),
    )


def _engine_with_fresh_books():
    bus = Bus()
    store = BookStore()
    eng = PremiumEngine(bus, store, CFG)
    store.update(_mk_book("binance", "XRP", "USDT", "99.9", "100"))
    store.update(_mk_book("upbit", "XRP", "KRW", "141000", "141100"))
    store.update(_mk_book("upbit", "USDT", "KRW", "1399", "1400"))
    return bus, store, eng


def test_compute_produces_row_with_net_edges():
    _, _, eng = _engine_with_fresh_books()
    eng.on_fx(Fx("USD/KRW", D(1380), "test", now_ms()))
    rows = eng.compute("XRP")
    assert len(rows) == 1
    r = rows[0]
    assert r["coin"] == "XRP" and r["dom_ex"] == "upbit"
    # gross: 1000 USDT → 10 XRP → 1,410,000 KRW → /1400 → +0.714...%
    assert abs(r["in_gross"] - (1410000 / 1400 / 1000 - 1)) < 1e-9
    # net = gross − (binance 0.001 + upbit 0.0005×2) − (0.2+1.0)/1000
    assert abs(r["in_net"] - (r["in_gross"] - 0.002 - 0.0012)) < 1e-9
    assert r["theo_mid"] is not None and r["exec_mid"] is not None
    assert r["out_net"] < 0  # 김프 상황에서 outbound는 음수


def test_stale_domestic_book_excluded():
    _, store, eng = _engine_with_fresh_books()
    store.update(_mk_book("upbit", "XRP", "KRW", "141000", "141100", ts=now_ms() - 60_000))
    assert eng.compute("XRP") == []


def test_missing_fx_still_computes_exec_side():
    # 환율 결측 → 이론 김프만 None, 실행 김프·엣지는 산출 (트리거는 살아있어야 함)
    _, _, eng = _engine_with_fresh_books()
    rows = eng.compute("XRP")
    assert len(rows) == 1
    assert rows[0]["theo_mid"] is None
    assert rows[0]["exec_mid"] is not None
    assert rows[0]["in_net"] is not None


def test_usdt_book_update_triggers_all_coins():
    bus, _, eng = _engine_with_fresh_books()
    q = bus.subscribe("premium")

    async def go():
        eng.on_book(_mk_book("upbit", "USDT", "KRW", "1399", "1400"))
        return q.get_nowait()

    rows = asyncio.run(go())
    assert rows and rows[0]["coin"] == "XRP"
