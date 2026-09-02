"""전송형 라이브 사이클 러너 검증 (M3ⓔⓕ) — 잠금 스택·해피패스·중단·복구. 시뮬 부품만 사용, 네트워크 없음."""
import asyncio
from decimal import Decimal

from kimp.alerts.telegram import Alerter
from kimp.bus import Bus
from kimp.config import Config
from kimp.cycle.risk import RiskManager
from kimp.engine.books import BookStore
from kimp.exec.base import OrderResult
from kimp.exec.journal import ExecutionJournal
from kimp.exec.runner import LiveCycleRunner, SimDepositWatcher, SimOrderAdapter
from kimp.models import Book, D, Level, now_ms

RISK = {"cycle_cap_usd": 2000, "inflight_cap_usd": 6000, "per_coin_cap_usd": 3000,
        "hourly_entries": 10, "daily_loss_limit_usd": 300, "max_consecutive_failures": 3}


def _cfg(tmp_path, mode="dry_run", routes=None, max_cycles=1):
    return Config(raw={
        "staleness": {"book_ms": 60_000},
        "fees": {"taker": {"upbit": 0.0005, "okx": 0.001}},
        "storage": {"root": str(tmp_path)},
        "paper": {"entry_threshold": 0.005, "max_edge": 0.05, "transfer_minutes": {"default": 1}, "risk": RISK},
        "execution": {"mode": mode, "routes": routes if routes is not None else
                      [{"coin": "PROM", "dom": "upbit", "ovs": "okx", "direction": "in"}],
                      "max_notional_usd": 100, "max_cycles": max_cycles, "order_timeout_sec": 2,
                      "arm_ttl_sec": 600},
    })


class FakeGateway:
    def __init__(self, approve=True):
        self.approve, self.calls = approve, []

    async def request(self, coin, from_ex, to_ex, amount, usd, reason):
        self.calls.append((coin, from_ex, to_ex, amount))
        return "WD-1" if self.approve else None


class Ledger:
    def __init__(self): self.rows = []
    def add(self, r): self.rows.append(r)


def _book(ex, base, quote, bid, ask):
    return Book(ex, base, quote, (Level(D(bid), D(10**6)),), (Level(D(ask), D(10**6)),), None, now_ms())


def _mk(tmp_path, mode="dry_run", approve=True, routes=None, max_cycles=1, adapters=None):
    cfg = _cfg(tmp_path, mode, routes, max_cycles)
    books = BookStore()
    books.update(_book("okx", "PROM", "USDT", "9.99", "10"))
    books.update(_book("upbit", "PROM", "KRW", "14300", "14310"))   # 김프 ~2%
    books.update(_book("upbit", "USDT", "KRW", "1399", "1400"))
    journal = ExecutionJournal(tmp_path / "exec.db")
    risk, ledger, gw = RiskManager(RISK), Ledger(), FakeGateway(approve)
    adapters = adapters or {"okx": SimOrderAdapter("okx"), "upbit": SimOrderAdapter("upbit", D("0.0005"))}
    r = LiveCycleRunner(cfg, Bus(), books, journal, risk, Alerter("", ""), None, gw,
                        SimDepositWatcher(delay_sec=0.01), adapters,
                        {("upbit", "PROM"): (True, True), ("okx", "PROM"): (True, True)},
                        set(), {"PROM": {"UPBIT>OKX"}}, ledger)
    return r, journal, risk, ledger, gw, adapters


ROW = {"coin": "PROM", "dom_ex": "upbit", "ovs_ex": "okx", "in_net": 0.015, "out_net": None, "in_capacity_usd": 5000.0}


def test_lock_stack_blocks_until_armed_and_routed(tmp_path, monkeypatch):
    monkeypatch.delenv("LIVE_TRADING_ALLOWED", raising=False)
    r, *_ = _mk(tmp_path, mode="off")
    assert r.can_fire("PROM", "upbit", "okx", "in") == "execution.mode off"
    r, *_ = _mk(tmp_path, mode="live")
    assert "환경 잠금" in r.can_fire("PROM", "upbit", "okx", "in")          # live는 env 없이 절대 불가
    r, *_ = _mk(tmp_path)                                                    # dry_run
    assert "arm" in r.can_fire("PROM", "upbit", "okx", "in")
    assert "없는 코인" in r.arm("ONG")                                       # 라우트 밖 코인은 arm 자체 거부
    assert "ARMED" in r.arm("prom 5")
    assert r.can_fire("PROM", "upbit", "okx", "in") is None
    assert r.can_fire("PROM", "upbit", "okx", "out") == "라우트 allowlist 외"
    assert r.can_fire("PROM", "bithumb", "okx", "in") == "라우트 allowlist 외"
    r.disarm()
    assert "arm" in r.can_fire("PROM", "upbit", "okx", "in")
    r.arm("PROM"); r._armed_until = 0                                        # TTL 만료
    assert "TTL" in r.can_fire("PROM", "upbit", "okx", "in")


def test_dry_run_happy_path_settles_and_journals(tmp_path):
    r, journal, risk, ledger, gw, adapters = _mk(tmp_path)
    r.arm("PROM")

    async def go():
        r.consider(dict(ROW))
        await asyncio.gather(*r._tasks)
    asyncio.run(go())
    j = journal.get(1)
    assert j["state"] == "SETTLED" and j["buy"]["client_id"] == "kimpj1b" and j["sell"]["client_id"] == "kimpj1s0"
    assert gw.calls and gw.calls[0][1:3] == ("okx", "upbit")               # 출금은 게이트웨이 경유만
    assert D(j["withdraw"]["amount"]) < D(j["buy_result"]["filled"])       # 수수료·버퍼 차감 후 출금
    assert len(ledger.rows) == 1 and ledger.rows[0]["state"] == "SETTLED" and ledger.rows[0]["mode"] == "dry_run"
    assert ledger.rows[0]["pnl_usd"] > 0                                    # 2% 김프 − 수수료
    assert risk.open_notional == {} and r.cycles_run == 1
    assert r.can_fire("PROM", "upbit", "okx", "in") == "max_cycles 도달"    # 첫 라이브 = 1사이클 후 정지


def test_gateway_denial_reconciles_and_disarms(tmp_path):
    r, journal, risk, ledger, gw, _ = _mk(tmp_path, approve=False)
    r.arm("PROM")

    async def go():
        r.consider(dict(ROW))
        await asyncio.gather(*r._tasks)
    asyncio.run(go())
    j = journal.get(1)
    assert j["state"] == "RECONCILE_REQUIRED" and "출금 미승인" in j["why"]
    assert r._armed_coin is None                                             # 자동화 정지
    assert ledger.rows == [] and risk.consecutive_failures == 1


def test_buy_canceled_aborts_without_money_moving(tmp_path):
    class CancelAdapter(SimOrderAdapter):
        async def place_ioc(self, sess, side, base, quote, price, qty, client_id):
            r = OrderResult(self.exchange, "x", client_id, "canceled")
            self._orders[client_id] = r
            return r
    r, journal, risk, ledger, gw, _ = _mk(tmp_path, adapters={"okx": CancelAdapter("okx"), "upbit": SimOrderAdapter("upbit")})
    r.arm("PROM")

    async def go():
        r.consider(dict(ROW))
        await asyncio.gather(*r._tasks)
    asyncio.run(go())
    assert journal.get(1)["state"] == "ABORTED" and gw.calls == [] and ledger.rows == []


def test_restart_recovery_continues_without_new_buy_order(tmp_path):
    """중복 주문 0: BUY_SUBMITTED에서 죽었다 살아나면 같은 client_id를 '조회'해 이어간다 (재발사 없음)."""
    r, journal, risk, ledger, gw, adapters = _mk(tmp_path)
    okx = adapters["okx"]
    # 재시작 전 상태 재현: 저널 BUY_SUBMITTED + 거래소에는 체결 기록 존재
    jid = journal.begin("PROM:upbit>okx:in", {"coin": "PROM", "dom_ex": "upbit", "ovs_ex": "okx", "direction": "in",
                                              "notional_usd": 100.0, "edge": 0.015, "mode": "dry_run"})
    journal.advance(jid, "BUY_SUBMITTED", {"buy": {"ex": "okx", "client_id": "kimpj1b", "price": "10.02", "qty": "9.98"}})
    okx._orders["kimpj1b"] = OrderResult("okx", "o1", "kimpj1b", "filled", D("9.98"), D("10.02"), D("0.00998"), "PROM")
    places = []
    orig = okx.place_ioc
    async def counting_place(*a, **k):
        places.append(a); return await orig(*a, **k)
    okx.place_ioc = counting_place

    asyncio.run(r.recover())
    j = journal.get(jid)
    assert j["state"] == "SETTLED" and places == []                         # okx에 새 매수 주문 없음
    assert gw.calls and len(ledger.rows) == 1
