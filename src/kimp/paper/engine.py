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
from ..symbols import leg_blocked, leg_key

log = logging.getLogger("paper")


def wallet_block_reason(wallet_state: dict, kind: str, coin: str, dom_ex: str, ovs_ex: str) -> str | None:
    """T4 지갑 게이트 — None=통과, str=차단 사유. 국내 미확인=차단(보수), 해외 미확인=통과(수집 랙 허용).
    페이퍼·라이브 러너 공용 (판정 드리프트 방지)."""
    dom = wallet_state.get((dom_ex, coin))
    if dom is None:
        return f"{dom_ex} 지갑 상태 미확인"
    dep, wd = dom
    if kind == "in" and not dep:
        return f"{dom_ex} 입금 정지"
    if kind == "out" and not wd:
        return f"{dom_ex} 출금 정지"
    ovs = wallet_state.get((ovs_ex, coin))
    if ovs is not None:
        ovs_dep, ovs_wd = ovs
        if kind == "in" and not ovs_wd:
            return f"{ovs_ex} 출금 정지"
        if kind == "out" and not ovs_dep:
            return f"{ovs_ex} 입금 정지"
    return None


def leg_allowed(blocklist: set[str], verified_ok: dict[str, set[str]] | None, coin: str, dom_ex: str, ovs_ex: str) -> bool:
    """V7 게이트 — blocklist 레그 문법 + allowlist(검증 레그) 모드. 페이퍼·라이브 공용."""
    if leg_blocked(blocklist, coin, dom_ex, ovs_ex):
        return False
    if verified_ok is not None and leg_key(dom_ex, ovs_ex) not in verified_ok.get(coin.upper(), ()):
        return False
    return True


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
        # V7 미검증 심볼 — 자산 동일성 확인 전까지 거래 금지 (수집·관측은 계속).
        # 레그 문법은 symbols.leg_blocked 참조 ("COIN" / "COIN@dom" / "COIN>ovs" / "COIN@dom>ovs")
        self.blocklist: set[str] = {s.upper() for s in cfg.raw.get("universe", {}).get("trade_blocklist", [])}
        # allowlist 모드: verify_universe.py가 생성한 검증 통과 목록이 있으면 그 레그만 거래.
        # 신규 상장은 verify 재실행 전까지 자동 차단 — "미검증 = 거래 불가"의 구조화.
        # M2: {코인: {"DOM>OVS", ...}} 레그 단위 — 구 포맷(코인 목록)은 바이낸스 레그만 검증된 것으로 해석
        self.verified_ok: dict[str, set[str]] | None = self._load_verified(cfg)
        self._open_keys: set[tuple] = set()  # (coin, dom_ex, kind) — 같은 기회 중복 진입 방지
        self._tasks: set[asyncio.Task] = set()

    @staticmethod
    def _load_verified(cfg: Config):
        import json
        from pathlib import Path

        path = Path(cfg.storage.get("root", "data")) / "verified_ok.json"
        try:
            d = json.loads(path.read_text())
            legs_raw = d.get("legs")
            if isinstance(legs_raw, dict):  # M2 포맷: {coin: ["upbit>okx", ...]}
                legs = {c.upper(): {str(l).upper() for l in ls} for c, ls in legs_raw.items()}
            else:
                # 구 포맷(코인 목록) = 바이낸스 기준 검증만 수행된 시절 — 다른 해외 레그는 미검증으로 차단
                legs = {s.upper(): {"UPBIT>BINANCE", "BITHUMB>BINANCE"} for s in d.get("ok", [])}
            age_days = (now_ms() - int(d.get("generated_ms", 0))) / 86_400_000
            log.info("verified allowlist: %d coins (생성 %.1f일 전%s%s)", len(legs), age_days,
                     "" if isinstance(legs_raw, dict) else " · 구 포맷 — 바낸 레그만 허용, verify 재실행 권장",
                     " — 오래됨, verify_universe 재실행 권장" if age_days > 7 else "")
            if not legs:
                log.error("verified allowlist가 비어 있음 — fail-closed: 전 레그 거래 차단 (verify_universe 재실행 필요)")
            return legs  # 빈 dict = 전 레그 차단. "검증 0건 → allowlist 해제"는 fail-open이라 금지 (2026-08-30 리뷰)
        except FileNotFoundError:
            log.info("verified allowlist 없음 — blocklist 모드로 동작 (verify_universe 실행 시 allowlist 모드 전환)")
            return None
        except Exception:
            # 파일이 존재하는데 읽을 수 없음(손상 등) = 운용 중 이상 — 열어주는 쪽(None)이 아니라 닫는 쪽으로
            log.exception("verified_ok.json 파싱 실패 — fail-closed: 전 레그 거래 차단 (파일 복구/verify 재실행 필요)")
            return {}

    # ---------- 게이트 ----------

    def _wallet_ok(self, kind: str, coin: str, dom_ex: str, ovs_ex: str) -> str | None:
        return wallet_block_reason(self.wallet_state, kind, coin, dom_ex, ovs_ex)

    def _transfer_sec(self, coin: str) -> float:
        return float(self.transfer_min.get(coin, self.transfer_min.get("default", 15))) * 60

    def _fees(self, kind: str, coin: str, dom_ex: str, ovs_ex: str, notional: Decimal) -> Decimal:
        fee_ratio = self.cfg.taker_fee(ovs_ex) + self.cfg.taker_fee(dom_ex) * 2
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
        if not leg_allowed(self.blocklist, self.verified_ok, coin, dom_ex, ovs_ex):
            return  # V7: blocklist 레그 차단 / allowlist 모드 미검증 레그 — 엣지가 아무리 좋아도 거래 금지
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
        self._finalize(c, pnl=-float(self._fees(c.kind, c.coin, c.dom_ex, c.ovs_ex, c.notional_usd)),
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
        pnl = float(gross - self._fees(c.kind, c.coin, c.dom_ex, c.ovs_ex, c.notional_usd))
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
        blocklist 소급 등재 또는 allowlist에서 빠진(미검증) 레그의 사이클은 무효 처리
        (손익 미집계 — 오염 방지. 예: M1이 코인 단위 allowlist로 열었던 okx 레그가 M2 레그 검증에서 탈락)."""
        n = 0
        for c in self.store.load_open():
            unverified = self.verified_ok is not None and leg_key(c.dom_ex, c.ovs_ex) not in self.verified_ok.get(
                c.coin.upper(), ()
            )
            if leg_blocked(self.blocklist, c.coin, c.dom_ex, c.ovs_ex) or unverified:
                c.pnl_usd = 0.0
                c.note = "trade_blocklist/미검증 레그 소급 무효 (V7)"
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
            # 같은 틱 배치 안에서 최고 엣지 레그가 먼저 슬롯을 잡게 정렬 (M1 — 해외 3곳이
            # 같은 김프를 동시에 보이므로, config 나열 순서가 아니라 엣지가 거래소를 고른다)
            for r in sorted(rows, key=_best_edge, reverse=True):
                try:
                    self.consider(r)
                except Exception:
                    log.exception("consider failed: %s", r.get("coin"))
        for t in list(self._tasks):
            t.cancel()


def _best_edge(r: dict) -> float:
    vals = [v for v in (r.get("in_net"), r.get("out_net")) if v is not None]
    return max(vals) if vals else float("-inf")
