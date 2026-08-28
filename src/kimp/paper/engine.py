"""P2 페이퍼 트레이딩 엔진 — 주문·출금 없이 전체 사이클을 실전 조건으로 시뮬레이션.

실전성의 핵심 3가지:
  1. 체결은 '그 순간의 실제 호가창' VWAP (엔진 계산 재사용) — 슬리피지 실현
  2. 전송은 실제 벽시계 타이머 — 도착 시점의 호가로 매도하므로 **전송 중 드리프트가 실현됨**
     (IN naked의 김프 변동 리스크가 여기서 처음으로 실측됨)
  3. T13 한도·T4 지갑 게이트(3거래소)·capacity 사이징이 실거래와 동일하게 집행

단순화 (코드 주석에 명시, P2.1에서 정밀화):
  - 체결은 VWAP 전량 체결 가정 (부분체결 없음 — 깊이 부족이면 진입 안 함)
  - OUT은 헤지 락 가정: 해외 매도가를 진입 시점에 고정. 복귀 레그(USDT→KRW) 드리프트는
    O2 리밸런서 영역이라 미모델 — 진입 시점 환산으로 정산
  - 전송 시간은 코인별 설정값 (P3에서 실측 분포로 대체)
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

from ..alerts.telegram import CRIT, INFO, WARN, Alerter
from ..bus import Bus
from ..config import Config
from ..cycle.model import ARRIVED, ENTERED, IN_FLIGHT, SETTLED, SETTLED_STUCK, VOID, Cycle
from ..cycle.risk import RiskManager
from ..cycle.store import CycleStore
from ..engine.books import BookStore
from ..engine.premium import vwap_buy, vwap_sell
from ..models import D, now_ms

log = logging.getLogger("paper")


class PaperEngine:
    def __init__(
        self,
        bus: Bus,
        books: BookStore,
        cycle_store: CycleStore,
        risk: RiskManager,
        alerter: Alerter,
        wallet_state: dict,
        cfg: Config,
        ledger_writer,
    ) -> None:
        self.bus = bus
        self.books = books
        self.store = cycle_store
        self.risk = risk
        self.alerter = alerter
        self.wallet_state = wallet_state
        self.cfg = cfg
        self.ledger = ledger_writer
        p = cfg.raw.get("paper", {})
        self.enabled = bool(p.get("enabled", True))
        self.entry_thr = float(p.get("entry_threshold", 0.005))
        self.max_edge = float(p.get("max_edge", 0.05))
        self.hedge_out = bool(p.get("hedge_out", True))
        self.transfer_min = p.get("transfer_minutes", {}) or {}
        # V7 미검증 심볼 — 자산 동일성 확인 전까지 거래 금지 (수집·관측은 계속)
        self.blocklist: set[str] = {s.upper() for s in cfg.raw.get("universe", {}).get("trade_blocklist", [])}
        self._open_keys: set[tuple] = set()  # (coin, dom_ex, kind) — 같은 기회 중복 진입 방지
        self._tasks: set[asyncio.Task] = set()

    # ---------- 게이트 ----------

    def _wallet_ok(self, kind: str, coin: str, dom_ex: str, ovs_ex: str) -> str | None:
        """None=통과, str=차단 사유. 국내 미확인=차단(보수), 해외 미확인=통과(수집 랙 허용)."""
        dom = self.wallet_state.get((dom_ex, coin))
        if dom is None:
            return f"{dom_ex} 지갑 상태 미확인"
        dep, wd = dom
        if kind == "in" and not dep:
            return f"{dom_ex} 입금 정지"
        if kind == "out" and not wd:
            return f"{dom_ex} 출금 정지"
        ovs = self.wallet_state.get((ovs_ex, coin))
        if ovs is not None:
            ovs_dep, ovs_wd = ovs
            if kind == "in" and not ovs_wd:
                return f"{ovs_ex} 출금 정지"
            if kind == "out" and not ovs_dep:
                return f"{ovs_ex} 입금 정지"
        return None

    def _transfer_sec(self, coin: str) -> float:
        return float(self.transfer_min.get(coin, self.transfer_min.get("default", 15))) * 60

    def _fees(self, kind: str, coin: str, dom_ex: str, notional: Decimal) -> Decimal:
        fee_ratio = self.cfg.taker_fee(self.cfg.overseas_ref) + self.cfg.taker_fee(dom_ex) * 2
        wd = self.cfg.withdraw_fee_usd(coin) + self.cfg.withdraw_fee_usd("USDT")
        if kind == "out":  # 국내 코인 출금 정률 수수료 (빗썸 알트 ~1% — 운용자 실측 2026-08-28)
            wd += notional * self.cfg.withdraw_fee_pct(dom_ex, coin)
        return notional * fee_ratio + wd

    # ---------- 진입 ----------

    def consider(self, row: dict) -> None:
        """premium 행 1건 평가 — 0-지연 게이트 통과 시 페이퍼 사이클 진입."""
        if not self.enabled:
            return
        coin, dom_ex, ovs_ex = row["coin"], row["dom_ex"], row["ovs_ex"]
        if coin in self.blocklist:
            return  # V7 미검증 — 엣지가 아무리 좋아도 거래 금지
        for kind in ("in", "out"):
            net = row.get(f"{kind}_net")
            if net is None or net < self.entry_thr:
                continue
            if net > self.max_edge:
                continue  # 요주의(V7/V8 의심) — 페이퍼도 진입 금지
            key = (coin, dom_ex, kind)
            if key in self._open_keys:
                continue
            if self._wallet_ok(kind, coin, dom_ex, ovs_ex) is not None:
                continue  # 함정 — 알림 계층이 이미 WARN 처리
            cap = row.get(f"{kind}_capacity_usd")
            if not cap:
                continue
            notional = min(D(cap), self.risk.cycle_cap)
            reason = self.risk.check_entry(coin, notional)
            if reason is not None:
                log.debug("entry blocked %s %s: %s", coin, kind, reason)
                continue
            self._enter(kind, coin, dom_ex, ovs_ex, notional, float(net))

    def _enter(self, kind: str, coin: str, dom_ex: str, ovs_ex: str, notional: Decimal, net: float) -> None:
        stale = self.cfg.book_stale_ms
        dom = self.books.fresh(dom_ex, coin, "KRW", stale)
        ovs = self.books.fresh(ovs_ex, coin, "USDT", stale)
        usdt = self.books.fresh(dom_ex, "USDT", "KRW", stale)
        if dom is None or ovs is None or usdt is None:
            return

        c = Cycle(kind=kind, coin=coin, dom_ex=dom_ex, ovs_ex=ovs_ex,
                  notional_usd=notional, entry_edge=net, hedged=(kind == "out" and self.hedge_out))
        if kind == "in":
            r = vwap_buy(ovs.asks, notional)  # 해외에서 USDT로 코인 매수
            if r is None:
                return
            c.qty, _ = r
        else:
            usdt_mid = usdt.mid
            if usdt_mid is None:
                return
            r = vwap_buy(dom.asks, notional * usdt_mid)  # 국내에서 원화로 코인 매수
            if r is None:
                return
            c.qty, _ = r
            if c.hedged:
                s = vwap_sell(ovs.bids, c.qty)  # 헤지 락: 해외 매도가를 지금 고정
                if s is None:
                    return
                c.locked_usdt, _ = s

        c.stamp(ENTERED)
        c.arrival_at_ms = now_ms() + int(self._transfer_sec(coin) * 1000)
        c.stamp(IN_FLIGHT)
        self.store.save(c)                       # write-ahead: 부수효과 전에 기록
        self.risk.on_entry(c.id, coin, notional)
        self._open_keys.add((coin, dom_ex, kind))
        self._spawn_arrival(c)
        self.alerter.alert(
            INFO, f"paper:enter:{c.id}",
            f"[PAPER] {coin} {kind.upper()} 진입 ${notional:,.0f} @엣지 {net*100:.2f}% "
            f"({dom_ex}↔{ovs_ex}, 도착예정 {self._transfer_sec(coin)/60:.0f}분{', 헤지락' if c.hedged else ', naked'})",
            cooldown=0,
        )

    # ---------- 도착·정산 ----------

    def _spawn_arrival(self, c: Cycle) -> None:
        t = asyncio.create_task(self._arrival_timer(c), name=f"paper.{c.id}")
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _arrival_timer(self, c: Cycle) -> None:
        delay = max((c.arrival_at_ms or 0) - now_ms(), 0) / 1000
        await asyncio.sleep(delay)
        for attempt in range(6):
            if self._try_settle(c):
                return
            await asyncio.sleep(60)  # 도착 시 호가 스테일/깊이 부족 → 재시도 (T4 플레이북: 투매 금지)
        self._finalize(c, pnl=-float(self._fees(c.kind, c.coin, c.dom_ex, c.notional_usd)),
                       state=SETTLED_STUCK, note="도착 후 6회 재시도에도 정산 불가 — 수수료만 손실 처리")

    def _try_settle(self, c: Cycle) -> bool:
        stale = self.cfg.book_stale_ms
        dom = self.books.fresh(c.dom_ex, c.coin, "KRW", stale)
        usdt = self.books.fresh(c.dom_ex, "USDT", "KRW", stale)
        if c.kind == "in":
            if dom is None or usdt is None or c.qty is None:
                return False
            s = vwap_sell(dom.bids, c.qty)           # 도착 시점 국내 매도 — 드리프트 실현
            if s is None:
                return False
            krw, _ = s
            r = vwap_buy(usdt.asks, krw)             # USDT 복귀 레그
            if r is None:
                return False
            usdt_out, _ = r
            gross = usdt_out - c.notional_usd
        else:
            if c.hedged and c.locked_usdt is not None:
                gross = c.locked_usdt - c.notional_usd   # 헤지 락 — 진입 시점 확정 스프레드
            else:
                ovs = self.books.fresh(c.ovs_ex, c.coin, "USDT", stale)
                if ovs is None or c.qty is None:
                    return False
                s = vwap_sell(ovs.bids, c.qty)       # naked OUT: 도착 시점 해외 매도
                if s is None:
                    return False
                usdt_got, _ = s
                gross = usdt_got - c.notional_usd
        pnl = float(gross - self._fees(c.kind, c.coin, c.dom_ex, c.notional_usd))
        c.stamp(ARRIVED)
        self._finalize(c, pnl=pnl, state=SETTLED)
        return True

    def _finalize(self, c: Cycle, pnl: float, state: str, note: str = "") -> None:
        c.pnl_usd = pnl
        c.note = note
        c.stamp(state)
        self.store.save(c)
        self._open_keys.discard((c.coin, c.dom_ex, c.kind))
        halt = self.risk.on_close(c.id, c.coin, pnl, failed=(state == SETTLED_STUCK))
        self.ledger.add(
            {
                "ts": now_ms(), "cycle_id": c.id, "kind": c.kind, "coin": c.coin,
                "dom_ex": c.dom_ex, "ovs_ex": c.ovs_ex, "hedged": c.hedged,
                "notional_usd": float(c.notional_usd), "entry_edge": c.entry_edge,
                "pnl_usd": pnl, "state": state,
                "stamps": str(c.stamps), "note": note,
            }
        )
        sev = INFO if state == SETTLED and pnl >= 0 else WARN
        realized = pnl / float(c.notional_usd) * 100
        drift = "" if c.entry_edge is None else f" (기대 {c.entry_edge*100:.2f}% → 드리프트 {realized - c.entry_edge*100:+.2f}%p)"
        self.alerter.alert(
            sev, f"paper:settle:{c.id}",
            f"[PAPER] {c.coin} {c.kind.upper()} 정산 {'' if pnl < 0 else '+'}"
            f"${pnl:,.2f} ({realized:+.2f}%){drift} · 오늘 누적 ${self.risk.daily_pnl:,.2f}"
            + (f" · {note}" if note else ""),
            cooldown=0,
        )
        if halt:
            self.alerter.alert(CRIT, "paper:halt", f"[PAPER] 자동 L1 발동 — {halt}. 신규 진입 차단 (진행 중 사이클은 완주)", cooldown=0)

    # ---------- 루프·복구 ----------

    def resume(self) -> int:
        """재기동 시 열린 사이클 이어가기 (T6) — 도착 타이머 재장전.
        blocklist에 소급 등재된 코인의 사이클은 무효 처리 (손익 미집계 — 오염 방지)."""
        n = 0
        for c in self.store.load_open():
            if c.coin in self.blocklist:
                c.pnl_usd = 0.0
                c.note = "trade_blocklist 소급 무효 (V7 미검증 심볼)"
                c.stamp(VOID)
                self.store.save(c)
                self.ledger.add(
                    {"ts": now_ms(), "cycle_id": c.id, "kind": c.kind, "coin": c.coin,
                     "dom_ex": c.dom_ex, "ovs_ex": c.ovs_ex, "hedged": c.hedged,
                     "notional_usd": float(c.notional_usd), "entry_edge": c.entry_edge,
                     "pnl_usd": 0.0, "state": VOID, "stamps": str(c.stamps), "note": c.note}
                )
                self.alerter.alert(WARN, f"paper:void:{c.id}",
                                   f"[PAPER] {c.coin} {c.kind.upper()} 사이클 무효 처리 — {c.note}", cooldown=0)
                continue
            self.risk.on_entry(c.id, c.coin, c.notional_usd)
            self._open_keys.add((c.coin, c.dom_ex, c.kind))
            self._spawn_arrival(c)
            n += 1
        return n

    async def run(self, stop: asyncio.Event) -> None:
        if not self.enabled:
            await stop.wait()
            return
        n = self.resume()
        if n:
            self.alerter.alert(INFO, "paper:resume", f"[PAPER] 재기동 — 진행 중 사이클 {n}건 이어감", cooldown=0)
        q = self.bus.subscribe("premium")
        while not stop.is_set():
            try:
                rows = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            for r in rows:
                try:
                    self.consider(r)
                except Exception:
                    log.exception("consider failed: %s", r.get("coin"))
        for t in list(self._tasks):
            t.cancel()
