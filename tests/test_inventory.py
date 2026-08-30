"""§1.5 재고 선배치형(INVENTORY) 페이퍼 엔진 검증 — 양방 동시 체결·밴드·리밸런싱."""
import asyncio

from kimp.alerts.telegram import Alerter
from kimp.bus import Bus
from kimp.config import Config
from kimp.cycle.store import CycleStore
from kimp.engine.books import BookStore
from kimp.models import Book, D, Level, now_ms
from kimp.paper.inventory import InventoryEngine


def _cfg(tmp_path):
    return Config(raw={
        "staleness": {"book_ms": 60_000},
        "fees": {"taker": {"upbit": 0.0005, "binance": 0.001}},
        "withdraw_fee_usd_est": {"default": 1.0, "XRP": 1.0, "USDT": 1.0},
        "withdraw_fee_pct": {},
        "storage": {"root": str(tmp_path)},
        "paper": {
            "enabled": True, "max_edge": 0.05, "transfer_minutes": {"default": 1},
            "inventory": {
                "enabled": True,
                "venues_usd": {"upbit": 5000, "binance": 5000},
                "coins": ["XRP"],
                "dom_base_pct": 0.5, "ovs_base_pct": 0.2,
                "band_pct": 0.5, "slice_usd": 500, "slice_interval_sec": 0,
                "entry_threshold": 0.005,
            },
        },
    })


def _mk(tmp_path, verified=None, blocklist=None):
    books = BookStore()
    rows = []
    class FakeLedger:
        def add(self, row): rows.append(row)
    eng = InventoryEngine(
        Bus(), books, CycleStore(tmp_path / "c.db"), Alerter("", ""), {},
        _cfg(tmp_path), FakeLedger(), blocklist or set(), verified,
    )
    return eng, books, rows


def _book(ex, base, quote, bid, ask, size=10**6):
    return Book(ex, base, quote, (Level(D(bid), D(size)),), (Level(D(ask), D(size)),), None, now_ms())


def _feed_kimp(books):
    """IN 김프 상황: 국내 142,000/142,100 vs 해외 99.9/100, USDT 1399/1400 → gross ~1.4%."""
    books.update(_book("binance", "XRP", "USDT", "99.9", "100"))
    books.update(_book("upbit", "XRP", "KRW", "142000", "142100"))
    books.update(_book("upbit", "USDT", "KRW", "1399", "1400"))


ROW = {"coin": "XRP", "dom_ex": "upbit", "ovs_ex": "binance", "in_net": 0.01, "out_net": None}


def test_inventory_in_slice_moves_stock_and_locks_pnl(tmp_path):
    eng, books, rows = _mk(tmp_path)
    _feed_kimp(books)
    eng.consider(dict(ROW))
    assert len(rows) == 1 and rows[0]["kind"] == "inv_in"
    r = rows[0]
    assert r["pnl_usd"] > 0 and r["notional_usd"] <= 500
    # 코인 총량 불변: 국내 −q / 해외 +q, quote는 반대로
    dom0 = eng.base_target[("upbit", "XRP")]
    assert eng.base_qty[("upbit", "XRP")] < dom0
    assert eng.base_qty[("binance", "XRP")] > eng.base_target[("binance", "XRP")]
    moved = dom0 - eng.base_qty[("upbit", "XRP")]
    assert abs(float(moved - (eng.base_qty[("binance", "XRP")] - eng.base_target[("binance", "XRP")]))) < 1e-9
    assert eng.quote_usd["binance"] < eng.quote_target["binance"]   # 해외 USDT 소모
    assert eng.quote_usd["upbit"] > eng.quote_target["upbit"]       # 국내 매도대금(USDT 환전) 증가


def test_inventory_band_limits_and_triggers_rebalance(tmp_path):
    eng, books, rows = _mk(tmp_path)
    _feed_kimp(books)
    for _ in range(20):
        eng.consider(dict(ROW))
    slices = [r for r in rows if r["kind"] == "inv_in"]
    rebal = [r for r in rows if r["kind"] == "rebalance"]
    # 밴드 하한(목표의 50%)에서 진입이 멈춘다 — 무한 진입 금지 (§1.5)
    floor = eng.base_target[("upbit", "XRP")] * D("0.5")
    assert eng.base_qty[("upbit", "XRP")] >= floor - D("0.0001")
    assert 1 <= len(slices) < 20
    # 리밸런싱 시뮬: 해외 잉여분 → 국내 (출금비가 여기 붙는다)
    assert len(rebal) == 1 and rebal[0]["pnl_usd"] < 0
    assert eng.pending_rebalance and eng.pending_rebalance[0]["to"] == "upbit"


def test_inventory_out_slice_mirrors(tmp_path):
    eng, books, rows = _mk(tmp_path)
    eng.slice_usd = D(200)  # 해외 base 배분($1k≈10개)의 리밸런싱 트리거(75%)를 안 건드리는 크기
    # 역프: 국내 137,900/138,000 vs 해외 bid 99.9 → 국내 매수 ∥ 해외 매도
    books.update(_book("binance", "XRP", "USDT", "99.9", "100"))
    books.update(_book("upbit", "XRP", "KRW", "137900", "138000"))
    books.update(_book("upbit", "USDT", "KRW", "1399", "1400"))
    eng.consider({**ROW, "in_net": None, "out_net": 0.01})
    assert rows and rows[0]["kind"] == "inv_out" and rows[0]["pnl_usd"] > 0
    assert eng.base_qty[("upbit", "XRP")] > eng.base_target[("upbit", "XRP")]      # 국내 코인 증가
    assert eng.base_qty[("binance", "XRP")] < eng.base_target[("binance", "XRP")]  # 해외 코인 감소
    assert not eng.pending_rebalance                                               # 트리거 미발동 확인


def test_inventory_respects_verified_legs_and_blocklist(tmp_path):
    eng, books, rows = _mk(tmp_path, verified={"XRP": {"UPBIT>OKX"}})
    _feed_kimp(books)
    eng.consider(dict(ROW))                      # upbit>binance 레그 미검증 → 차단
    assert rows == []
    eng2, books2, rows2 = _mk(tmp_path, blocklist={"XRP>BINANCE"})
    _feed_kimp(books2)
    eng2.consider(dict(ROW))
    assert rows2 == []


def test_inventory_state_persists_across_restart(tmp_path):
    eng, books, rows = _mk(tmp_path)
    _feed_kimp(books)
    eng.consider(dict(ROW))
    qty = eng.base_qty[("upbit", "XRP")]
    eng2, _, _ = _mk(tmp_path)                   # 같은 storage root → 상태 파일 재적재
    assert eng2.base_qty[("upbit", "XRP")] == qty
    assert eng2.quote_usd["binance"] == eng.quote_usd["binance"]
