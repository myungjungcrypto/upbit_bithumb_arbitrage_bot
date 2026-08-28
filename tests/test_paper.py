"""P2 페이퍼 트레이딩 검증 — T13 한도 집행, 사이클 영속·복구, 정산 수학, 게이트."""
import asyncio
from decimal import Decimal

from kimp.alerts.telegram import Alerter
from kimp.bus import Bus
from kimp.config import Config
from kimp.cycle.model import IN_FLIGHT, SETTLED, Cycle
from kimp.cycle.risk import RiskManager
from kimp.cycle.store import CycleStore
from kimp.engine.books import BookStore
from kimp.models import Book, D, Level, now_ms
from kimp.paper.engine import PaperEngine

T13 = {
    "cycle_cap_usd": 2000, "inflight_cap_usd": 6000, "per_coin_cap_usd": 3000,
    "hourly_entries": 10, "daily_loss_limit_usd": 300, "max_consecutive_failures": 3,
}


# ---------- RiskManager (T13) ----------

def test_risk_caps():
    r = RiskManager(T13)
    assert r.check_entry("XRP", D(2500)) is not None          # 사이클 상한
    assert r.check_entry("XRP", D(2000)) is None
    r.on_entry("c1", "XRP", D(2000))
    r.on_entry("c2", "XRP", D(1000))
    assert r.check_entry("XRP", D(500)) is not None            # 코인당 $3k 도달
    assert r.check_entry("SOL", D(2000)) is None
    r.on_entry("c3", "SOL", D(2000))
    assert r.check_entry("DOGE", D(1500)) is not None          # in-flight 총액 $6k 초과
    r.on_close("c1", "XRP", 10.0)
    assert r.check_entry("DOGE", D(1500)) is None              # 정산 후 여유 복원


def test_risk_daily_loss_halts():
    r = RiskManager(T13)
    r.on_entry("c1", "XRP", D(2000))
    halt = r.on_close("c1", "XRP", -350.0)
    assert halt and "일손실" in halt
    assert r.check_entry("XRP", D(100)) is not None            # L1 — 신규 차단


def test_risk_consecutive_failures_halt():
    r = RiskManager(T13)
    for i in range(3):
        r.on_entry(f"c{i}", "XRP", D(100))
        halt = r.on_close(f"c{i}", "XRP", -1.0, failed=True)
    assert halt and "연속 실패" in halt


def test_risk_hourly_rate():
    r = RiskManager(dict(T13, hourly_entries=2))
    r.on_entry("a", "XRP", D(100))
    r.on_entry("b", "SOL", D(100))
    assert r.check_entry("ADA", D(100)) is not None            # 시간당 2건 소진


# ---------- CycleStore (T6 영속·복구) ----------

def test_cycle_store_roundtrip(tmp_path):
    s = CycleStore(tmp_path / "cycles.db")
    c = Cycle(kind="in", coin="XRP", dom_ex="upbit", ovs_ex="binance", notional_usd=D(2000))
    c.qty = D("700.5")
    c.stamp(IN_FLIGHT)
    s.save(c)
    loaded = s.load_open()
    assert len(loaded) == 1
    lc = loaded[0]
    assert lc.id == c.id and lc.qty == D("700.5") and lc.state == IN_FLIGHT
    lc.pnl_usd = 12.5
    lc.stamp(SETTLED)
    s.save(lc)
    assert s.load_open() == []                                  # 정산되면 open 목록에서 제외


# ---------- PaperEngine ----------

def _mk_engine(tmp_path):
    cfg = Config(raw={
        "coins": ["XRP"], "overseas_ref": "binance",
        "staleness": {"book_ms": 60_000},
        "fees": {"taker": {"upbit": 0.0005, "binance": 0.001}},
        "withdraw_fee_usd_est": {"default": 0, "USDT": 0, "XRP": 0},  # 검증 단순화: 수수료 비율만
        "paper": {"enabled": True, "entry_threshold": 0.005, "max_edge": 0.05,
                  "hedge_out": True, "transfer_minutes": {"default": 1},
                  "risk": T13},
    })
    bus = Bus()
    books = BookStore()
    ledger_rows = []
    class FakeLedger:
        def add(self, row): ledger_rows.append(row)
    eng = PaperEngine(bus, books, CycleStore(tmp_path / "c.db"), RiskManager(T13),
                      Alerter("", ""), {}, cfg, FakeLedger())
    return eng, books, ledger_rows


def _book(ex, base, quote, bid, ask, size=10**6):
    return Book(ex, base, quote, (Level(D(bid), D(size)),), (Level(D(ask), D(size)),), None, now_ms())


def _feed(books, dom_bid="141000", dom_ask="141100", ovs_bid="99.9", ovs_ask="100", u_bid="1399", u_ask="1400"):
    books.update(_book("binance", "XRP", "USDT", ovs_bid, ovs_ask))
    books.update(_book("upbit", "XRP", "KRW", dom_bid, dom_ask))
    books.update(_book("upbit", "USDT", "KRW", u_bid, u_ask))


def _row(in_net=0.01, out_net=None, cap=9000.0):
    return {"coin": "XRP", "dom_ex": "upbit", "ovs_ex": "binance",
            "in_net": in_net, "out_net": out_net,
            "in_capacity_usd": cap, "out_capacity_usd": cap}


def test_paper_wallet_gate_blocks_unknown_and_suspended(tmp_path):
    eng, books, _ = _mk_engine(tmp_path)
    _feed(books)
    async def go():
        eng.consider(_row())                                   # 지갑 미확인 → 진입 금지
        assert eng.risk.open_notional == {}
        eng.wallet_state[("upbit", "XRP")] = (False, True)     # 국내 입금 정지
        eng.consider(_row())
        assert eng.risk.open_notional == {}
        eng.wallet_state[("upbit", "XRP")] = (True, True)
        eng.wallet_state[("binance", "XRP")] = (True, False)   # 해외 출금 정지 (V8)
        eng.consider(_row())
        assert eng.risk.open_notional == {}
    asyncio.run(go())


def test_paper_in_cycle_entry_and_settle(tmp_path):
    eng, books, ledger = _mk_engine(tmp_path)
    _feed(books)
    async def go():
        eng.wallet_state[("upbit", "XRP")] = (True, True)
        eng.wallet_state[("binance", "XRP")] = (True, True)
        eng.consider(_row(in_net=0.01))
        assert len(eng.risk.open_notional) == 1                # 진입됨 (notional=$2000 cap)
        c = eng.store.load_open()[0]
        assert c.kind == "in" and c.notional_usd == D(2000)
        assert c.qty == D(2000) / D(100)                       # 해외 ask 100에 매수
        # 같은 기회 중복 진입 방지
        eng.consider(_row(in_net=0.01))
        assert len(eng.risk.open_notional) == 1
        # 도착: 국내가가 140000으로 하락(드리프트) → 정산
        _feed(books, dom_bid="140000")
        assert eng._try_settle(c) is True
        assert len(ledger) == 1
        row = ledger[0]
        # 20 XRP × 140,000 = 2,800,000 KRW → /1400 = 2000 USDT → gross 0, 수수료만큼 손실
        expected_fee = 2000 * (0.001 + 0.0005 * 2)
        assert abs(row["pnl_usd"] + expected_fee) < 1e-6
        assert row["state"] == SETTLED
        assert eng.risk.open_notional == {}                    # 정산 후 한도 복원
    asyncio.run(go())


def test_paper_out_cycle_hedge_lock(tmp_path):
    eng, books, ledger = _mk_engine(tmp_path)
    # 역프: 국내가 낮음 — 국내 137,900/138,000, 해외 bid 99.9
    _feed(books, dom_bid="137900", dom_ask="138000", u_bid="1400", u_ask="1400")
    async def go():
        eng.wallet_state[("upbit", "XRP")] = (True, True)
        eng.wallet_state[("binance", "XRP")] = (True, True)
        eng.consider({"coin": "XRP", "dom_ex": "upbit", "ovs_ex": "binance",
                      "in_net": None, "out_net": 0.012, "out_capacity_usd": 9000.0})
        c = eng.store.load_open()[0]
        assert c.kind == "out" and c.hedged and c.locked_usdt is not None
        # qty = 2000×1400 / 138000 KRW ask, 락 = qty×99.9
        assert abs(float(c.locked_usdt) - float(c.qty) * 99.9) < 1e-6
        # 도착 전에 해외가가 폭락해도 (헤지 락이므로) 정산은 락 기준
        _feed(books, ovs_bid="90", dom_bid="137900", dom_ask="138000")
        assert eng._try_settle(c) is True
        row = ledger[0]
        expected_gross = float(c.locked_usdt) - 2000.0
        expected_fee = 2000 * (0.001 + 0.0005 * 2)
        assert abs(row["pnl_usd"] - (expected_gross - expected_fee)) < 1e-6
    asyncio.run(go())


def test_paper_resume_reloads_open_cycles(tmp_path):
    eng, books, _ = _mk_engine(tmp_path)
    _feed(books)
    async def go():
        eng.wallet_state[("upbit", "XRP")] = (True, True)
        eng.consider(_row(in_net=0.01))
        # 새 엔진 (재기동 시뮬레이션) — 같은 DB
        eng2, _, _ = _mk_engine(tmp_path)
        eng2.store = eng.store
        n = eng2.resume()
        assert n == 1
        assert len(eng2.risk.open_notional) == 1               # 한도에 in-flight 반영됨
        for t in list(eng._tasks) + list(eng2._tasks):
            t.cancel()
    asyncio.run(go())


def test_paper_max_edge_and_threshold(tmp_path):
    eng, books, _ = _mk_engine(tmp_path)
    _feed(books)
    async def go():
        eng.wallet_state[("upbit", "XRP")] = (True, True)
        eng.consider(_row(in_net=0.004))                       # 임계 미달
        eng.consider(_row(in_net=0.08))                        # max_edge 초과 (V7/V8 의심)
        assert eng.risk.open_notional == {}
    asyncio.run(go())
