"""§1.5 재고 선배치형(INVENTORY) 페이퍼 엔진 — M3ⓐ.

기회 순간 양방 동시 체결을 시뮬레이션한다: IN 김프 = 국내 보유 코인 매도 ∥ 해외 매수.
전송이 사이클에 없으므로 (T18 확정: 거래소당 $5k, 재고 가격 노출 감수):
  - 사이클 손익은 즉시 확정 (전송 드리프트 0) — 슬라이스 원장에 바로 기록
  - 출금비는 사이클이 아니라 리밸런싱 배치에 붙는다 (별도 kind="rebalance" 행으로 분리 회계)
  - 지갑(입출금) 상태는 실행을 막지 않는다 — 리밸런싱 가능 여부만 가른다 (거래 가능 ≠ 리밸런싱 가능)
TRANSFER 모드(PaperEngine)와 병행 측정 — 같은 기회를 두 방식으로 각각 집계해 비교한다.

단순화 (v1 — 측정 목적):
  - base 초기 재고는 첫 신선 호가 시점의 mid로 배치 (실제 매집 슬리피지 미모델)
  - 재고 보유분의 시세 변동 손익은 미집계 (T18: 보유 정책상 감수 — 실행 손익과 분리)
  - 리밸런싱은 밴드 이탈 시 목표 복원 전송 1건으로 근사 (기회 주도 복귀(O2)는 IN/OUT 상쇄로 자연 반영)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from decimal import Decimal
from pathlib import Path

from ..alerts.telegram import INFO, WARN, Alerter
from ..bus import Bus
from ..config import Config
from ..cycle.store import CycleStore
from ..engine.books import BookStore
from ..engine.premium import vwap_buy, vwap_sell
from ..models import D, now_ms
from ..symbols import DOMESTIC, leg_blocked, leg_key

log = logging.getLogger("paper.inv")

DUST_USD = D(10)  # 이하 잔량/슬라이스는 무시


class InventoryEngine:
    def __init__(
        self,
        bus: Bus,
        books: BookStore,
        store: CycleStore,
        alerter: Alerter,
        wallet_state: dict,
        cfg: Config,
        ledger_writer,
        blocklist: set[str],
        verified_ok: dict[str, set[str]] | None,
    ) -> None:
        self.bus = bus
        self.books = books
        self.store = store
        self.alerter = alerter
        self.wallet_state = wallet_state
        self.cfg = cfg
        self.ledger = ledger_writer
        self.blocklist = blocklist
        self.verified_ok = verified_ok

        p = cfg.raw.get("paper", {})
        icfg = p.get("inventory", {}) or {}
        self.coins = [str(c).upper() for c in icfg.get("coins", [])]
        self.enabled = bool(icfg.get("enabled", False)) and bool(self.coins)
        self.venues_usd = {str(k): D(v) for k, v in (icfg.get("venues_usd") or {}).items()}
        self.dom_base_pct = D(icfg.get("dom_base_pct", 0.5))
        self.ovs_base_pct = D(icfg.get("ovs_base_pct", 0.2))
        self.band_pct = D(icfg.get("band_pct", 0.5))
        self.slice_usd = D(icfg.get("slice_usd", 500))
        self.slice_interval = float(icfg.get("slice_interval_sec", 5))
        self.entry_thr = D(icfg.get("entry_threshold", p.get("entry_threshold", 0.005)))
        self.max_edge = D(p.get("max_edge", 0.05))
        self.transfer_min = p.get("transfer_minutes", {}) or {}
        self.stale_ms = cfg.book_stale_ms

        # 재고 상태 — (venue, coin) 코인 수량 / venue별 quote(USD 등가). 목표는 초기 배치 시점 고정
        self.base_qty: dict[tuple[str, str], Decimal] = {}
        self.base_target: dict[tuple[str, str], Decimal] = {}
        self.quote_usd: dict[str, Decimal] = {}
        self.quote_target: dict[str, Decimal] = {}
        self.pending_rebalance: list[dict] = []  # [{to, from, coin|"USDT", qty, arrive_ms}]
        self._state_path = Path(cfg.storage.get("root", "data")) / "inventory_state.json"
        self._last_slice: dict[tuple, float] = {}
        self._tasks: set[asyncio.Task] = set()
        self._load_state()

    # ---------- 상태 영속화 (재시작 시 재고 드리프트 방지) ----------

    def _load_state(self) -> None:
        try:
            d = json.loads(self._state_path.read_text())
        except FileNotFoundError:
            return
        except Exception:
            log.exception("inventory_state.json 파싱 실패 — 초기 배치부터 다시 시작")
            return
        self.base_qty = {tuple(k.split("|", 1)): D(v) for k, v in d.get("base_qty", {}).items()}
        self.base_target = {tuple(k.split("|", 1)): D(v) for k, v in d.get("base_target", {}).items()}
        self.quote_usd = {k: D(v) for k, v in d.get("quote_usd", {}).items()}
        self.quote_target = {k: D(v) for k, v in d.get("quote_target", {}).items()}
        self.pending_rebalance = list(d.get("pending_rebalance", []))

    def _save_state(self) -> None:
        tmp = self._state_path.with_suffix(".json.tmp")
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps({
            "base_qty": {f"{v}|{c}": str(q) for (v, c), q in self.base_qty.items()},
            "base_target": {f"{v}|{c}": str(q) for (v, c), q in self.base_target.items()},
            "quote_usd": {k: str(v) for k, v in self.quote_usd.items()},
            "quote_target": {k: str(v) for k, v in self.quote_target.items()},
            "pending_rebalance": self.pending_rebalance,
        }))
        os.replace(tmp, self._state_path)

    # ---------- 초기 배치 (lazy — 첫 신선 호가 시점의 가격으로) ----------

    def _ensure_quote(self, venue: str) -> None:
        if venue in self.quote_usd or venue not in self.venues_usd:
            return
        base_pct = self.dom_base_pct if venue in DOMESTIC else self.ovs_base_pct
        q = self.venues_usd[venue] * (1 - base_pct)
        self.quote_usd[venue] = q
        self.quote_target[venue] = q

    def _ensure_base(self, venue: str, coin: str, price_usd: Decimal) -> None:
        key = (venue, coin)
        if key in self.base_qty or venue not in self.venues_usd or price_usd <= 0:
            return
        base_pct = self.dom_base_pct if venue in DOMESTIC else self.ovs_base_pct
        budget = self.venues_usd[venue] * base_pct / len(self.coins)
        qty = budget / price_usd
        self.base_qty[key] = qty
        self.base_target[key] = qty
        self._save_state()
        log.info("재고 초기 배치: %s %s %.6f개 (~$%.0f @ $%.4f)", venue, coin, qty, budget, price_usd)

    # ---------- 게이트 ----------

    def _leg_ok(self, coin: str, dom_ex: str, ovs_ex: str) -> bool:
        if leg_blocked(self.blocklist, coin, dom_ex, ovs_ex):
            return False
        if self.verified_ok is not None and leg_key(dom_ex, ovs_ex) not in self.verified_ok.get(coin.upper(), ()):
            return False
        return True

    def _band_room_qty(self, venue: str, coin: str) -> Decimal:
        """이 venue에서 코인을 더 '내보낼(매도)' 수 있는 수량 — 밴드 하한까지의 여유."""
        key = (venue, coin)
        tgt = self.base_target.get(key)
        if tgt is None:
            return D(0)
        floor = tgt * (1 - self.band_pct)
        return max(self.base_qty.get(key, D(0)) - floor, D(0))

    # ---------- 슬라이스 실행 ----------

    def consider(self, row: dict) -> None:
        if not self.enabled:
            return
        coin = str(row["coin"]).upper()
        if coin not in self.coins:
            return
        dom_ex, ovs_ex = row["dom_ex"], row["ovs_ex"]
        if not self._leg_ok(coin, dom_ex, ovs_ex):
            return
        dom = self.books.fresh(dom_ex, coin, "KRW", self.stale_ms)
        ovs = self.books.fresh(ovs_ex, coin, "USDT", self.stale_ms)
        usdt = self.books.fresh(dom_ex, "USDT", "KRW", self.stale_ms)
        if dom is None or ovs is None or usdt is None or usdt.mid is None:
            return
        self._ensure_quote(dom_ex)
        self._ensure_quote(ovs_ex)
        if dom.mid is not None:
            self._ensure_base(dom_ex, coin, dom.mid / usdt.mid)
        if ovs.mid is not None:
            self._ensure_base(ovs_ex, coin, ovs.mid)

        for direction in ("in", "out"):
            net = row.get(f"{direction}_net")
            if net is None:
                continue
            sk = (coin, dom_ex, ovs_ex, direction)
            now = time.monotonic()
            if now - self._last_slice.get(sk, 0.0) < self.slice_interval:
                continue
            executed = (
                self._slice_in(coin, dom_ex, ovs_ex, dom, ovs, usdt)
                if direction == "in"
                else self._slice_out(coin, dom_ex, ovs_ex, dom, ovs, usdt)
            )
            if executed:
                self._last_slice[sk] = now
                self._check_rebalance(coin)

    def _slice_in(self, coin, dom_ex, ovs_ex, dom, ovs, usdt) -> bool:
        """IN 김프: 해외 매수 ∥ 국내 매도 (+ 국내 매도대금 KRW→USDT 즉시 환전)."""
        avail_quote = self.quote_usd.get(ovs_ex, D(0))
        room_qty = self._band_room_qty(dom_ex, coin)
        n = min(self.slice_usd, avail_quote)
        if n < DUST_USD or room_qty <= 0:
            return False
        r1 = vwap_buy(ovs.asks, n)
        if r1 is None:
            return False
        qty, _ = r1
        if qty > room_qty:  # 국내 매도 가능 수량으로 슬라이스 축소
            n = n * room_qty / qty
            if n < DUST_USD:
                return False
            r1 = vwap_buy(ovs.asks, n)
            if r1 is None:
                return False
            qty, _ = r1
        r2 = vwap_sell(dom.bids, qty)
        if r2 is None:
            return False
        krw, _ = r2
        r3 = vwap_buy(usdt.asks, krw)  # 매도대금을 즉시 USDT로 (방향별 실행가 — asks)
        if r3 is None:
            return False
        usdt_out, _ = r3
        fees = n * (self.cfg.taker_fee(ovs_ex) + self.cfg.taker_fee(dom_ex) * 2)
        pnl = usdt_out - n - fees
        edge = pnl / n
        gross_edge = usdt_out / n - 1
        if edge < self.entry_thr or gross_edge > self.max_edge:
            return False
        # 재고 이동: 코인 총량 불변 (국내 −q / 해외 +q) — 가격 노출 없음이 이 모드의 본질
        self.base_qty[(dom_ex, coin)] -= qty
        self.base_qty[(ovs_ex, coin)] = self.base_qty.get((ovs_ex, coin), D(0)) + qty
        self.quote_usd[ovs_ex] -= n
        self.quote_usd[dom_ex] = self.quote_usd.get(dom_ex, D(0)) + usdt_out
        self._record("inv_in", coin, dom_ex, ovs_ex, n, float(edge), float(pnl))
        return True

    def _slice_out(self, coin, dom_ex, ovs_ex, dom, ovs, usdt) -> bool:
        """OUT 역프: 국내 매수 ∥ 해외 매도 — IN이 만든 재고 편차를 반대로 되돌린다 (O2 자연 상쇄)."""
        avail_quote = self.quote_usd.get(dom_ex, D(0))
        room_qty = self._band_room_qty(ovs_ex, coin)
        n = min(self.slice_usd, avail_quote)
        if n < DUST_USD or room_qty <= 0:
            return False
        krw_budget = n * usdt.mid
        r1 = vwap_buy(dom.asks, krw_budget)
        if r1 is None:
            return False
        qty, _ = r1
        if qty > room_qty:
            krw_budget = krw_budget * room_qty / qty
            r1 = vwap_buy(dom.asks, krw_budget)
            if r1 is None:
                return False
            qty, _ = r1
        r2 = vwap_sell(ovs.bids, qty)
        if r2 is None:
            return False
        usdt_recv, _ = r2
        r_cost = vwap_buy(usdt.asks, krw_budget)  # 쓴 KRW의 USD 등가 (기회비용 — 그 돈으로 살 수 있던 USDT)
        if r_cost is None:
            return False
        cost_usd, _ = r_cost
        if cost_usd < DUST_USD:
            return False
        fees = cost_usd * (self.cfg.taker_fee(dom_ex) * 2 + self.cfg.taker_fee(ovs_ex))
        pnl = usdt_recv - cost_usd - fees
        edge = pnl / cost_usd
        if edge < self.entry_thr or (usdt_recv / cost_usd - 1) > self.max_edge:
            return False
        self.base_qty[(dom_ex, coin)] = self.base_qty.get((dom_ex, coin), D(0)) + qty
        self.base_qty[(ovs_ex, coin)] -= qty
        self.quote_usd[dom_ex] -= cost_usd
        self.quote_usd[ovs_ex] = self.quote_usd.get(ovs_ex, D(0)) + usdt_recv
        self._record("inv_out", coin, dom_ex, ovs_ex, cost_usd, float(edge), float(pnl))
        return True

    def _record(self, kind: str, coin: str, dom_ex: str, ovs_ex: str, notional: Decimal, edge: float, pnl: float) -> None:
        row = {
            "ts": now_ms(), "mode": "inventory", "kind": kind, "coin": coin,
            "dom_ex": dom_ex, "ovs_ex": ovs_ex, "notional_usd": float(notional),
            "entry_edge": edge, "pnl_usd": pnl, "state": "SETTLED", "hedged": False,
            "stamps": "", "note": "",
        }
        self.store.save_inv(row)
        self.ledger.add(row)
        self._save_state()
        self.alerter.alert(
            INFO, f"inv:{kind}:{coin}:{dom_ex}:{ovs_ex}",
            f"[INV] {coin} {kind[4:].upper()} 슬라이스 ${float(notional):,.0f} @엣지 {edge*100:.2f}% "
            f"({dom_ex}↔{ovs_ex}, 손익 ${pnl:+,.2f} 즉시 확정)",
            cooldown=300,
        )

    # ---------- 리밸런싱 시뮬 (밴드 이탈 → 전송 — 여기서만 출금비·전송시간이 등장) ----------

    def _check_rebalance(self, coin: str) -> None:
        for (venue, c), qty in list(self.base_qty.items()):
            if c != coin:
                continue
            tgt = self.base_target.get((venue, c))
            # 트리거는 밴드 절반 지점 — 진입 차단(밴드 하한)보다 먼저 발동해야 경계에서 교착이 없다
            if tgt is None or tgt <= 0 or qty >= tgt * (1 - self.band_pct / 2):
                continue
            if any(p["to"] == venue and p["coin"] == c for p in self.pending_rebalance):
                continue  # 이미 이 목적지로 전송 중 — 이중 발사 방지
            need = tgt - qty
            src = max(
                (k for k in self.base_qty if k[1] == c and k[0] != venue),
                key=lambda k: self.base_qty[k] - self.base_target.get(k, D(0)),
                default=None,
            )
            if src is None:
                continue
            surplus = self.base_qty[src] - self.base_target.get(src, D(0))
            amount = min(need, surplus)
            if amount <= 0:
                continue
            # 지갑 게이트는 여기서만 — 실행이 아니라 리밸런싱을 막는다 (§1.5 거래가능≠리밸런싱가능)
            from_ws = self.wallet_state.get((src[0], c))
            to_ws = self.wallet_state.get((venue, c))
            if (from_ws is not None and not from_ws[1]) or (to_ws is not None and not to_ws[0]):
                self.alerter.alert(
                    WARN, f"inv:rebal_blocked:{c}:{venue}",
                    f"[INV] {c} 리밸런싱 불가 ({src[0]}→{venue} 입출금 정지) — 잔여 런웨이 내에서만 실행 계속",
                    cooldown=3600,
                )
                continue
            self._start_transfer(src[0], venue, c, amount)

    def _start_transfer(self, from_v: str, to_v: str, coin: str, qty: Decimal) -> None:
        price = self._price_usd(from_v, coin)
        pct = self.cfg.withdraw_fee_pct(from_v, coin)
        cost = self.cfg.withdraw_fee_usd(coin) + (qty * price * pct if price else D(0))
        minutes = float(self.transfer_min.get(coin, self.transfer_min.get("default", 15)))
        arrive_ms = now_ms() + int(minutes * 60_000)
        self.base_qty[(from_v, coin)] -= qty
        self.pending_rebalance.append(
            {"from": from_v, "to": to_v, "coin": coin, "qty": str(qty), "arrive_ms": arrive_ms}
        )
        self._record_rebalance(from_v, to_v, coin, qty, cost)
        self._spawn_arrival(arrive_ms)

    def _record_rebalance(self, from_v: str, to_v: str, coin: str, qty: Decimal, cost: Decimal) -> None:
        row = {
            "ts": now_ms(), "mode": "inventory", "kind": "rebalance", "coin": coin,
            "dom_ex": to_v, "ovs_ex": from_v, "notional_usd": float(qty * self._price_usd(from_v, coin)),
            "entry_edge": 0.0, "pnl_usd": -float(cost), "state": "SETTLED", "hedged": False,
            "stamps": "", "note": f"{from_v}→{to_v} {qty} {coin}",
        }
        self.store.save_inv(row)
        self.ledger.add(row)
        self._save_state()
        self.alerter.alert(
            INFO, f"inv:rebal:{coin}:{to_v}",
            f"[INV] {coin} 리밸런싱 {from_v}→{to_v} {float(qty):.4f}개 (비용 ${float(cost):.2f} — 슬라이스 수익에서 상각)",
            cooldown=0,
        )

    def _price_usd(self, venue: str, coin: str) -> Decimal:
        if venue in DOMESTIC:
            dom = self.books.fresh(venue, coin, "KRW", self.stale_ms)
            usdt = self.books.fresh(venue, "USDT", "KRW", self.stale_ms)
            if dom is not None and dom.mid is not None and usdt is not None and usdt.mid:
                return dom.mid / usdt.mid
        else:
            b = self.books.fresh(venue, coin, "USDT", self.stale_ms)
            if b is not None and b.mid is not None:
                return b.mid
        return D(0)

    def _spawn_arrival(self, arrive_ms: int) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return  # 루프 밖(동기 테스트 경로) — 도착분은 run()/재시작 시 _settle_arrivals가 처리

        async def waiter():
            delay = max((arrive_ms - now_ms()) / 1000, 0)
            await asyncio.sleep(delay)
            self._settle_arrivals()

        t = asyncio.create_task(waiter(), name="inv.rebalance")
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def _settle_arrivals(self) -> None:
        now = now_ms()
        remaining = []
        for p in self.pending_rebalance:
            if int(p["arrive_ms"]) <= now:
                self.base_qty[(p["to"], p["coin"])] = self.base_qty.get((p["to"], p["coin"]), D(0)) + D(p["qty"])
            else:
                remaining.append(p)
        self.pending_rebalance = remaining
        self._save_state()

    # ---------- 루프 ----------

    def summary_line(self, today_start_ms: int) -> str:
        s = self.store.inv_summary(today_start_ms)
        return (f"INV: 오늘 ${s['pnl_today']:+,.2f} · 슬라이스 {s['slices']}건 · "
                f"리밸 {s['rebalances']}건(−${s['rebalance_cost']:,.2f})")

    async def run(self, stop: asyncio.Event) -> None:
        if not self.enabled:
            await stop.wait()
            return
        # 재시작 복구: 지나간 도착분 즉시 크레딧, 남은 전송은 타이머 재장전
        self._settle_arrivals()
        for p in self.pending_rebalance:
            self._spawn_arrival(int(p["arrive_ms"]))
        q = self.bus.subscribe("premium")
        log.info("inventory paper: ON — 코인 %s, 거래소당 배분 %s", self.coins,
                 {k: float(v) for k, v in self.venues_usd.items()})
        while not stop.is_set():
            try:
                rows = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            for r in rows:
                try:
                    self.consider(r)
                except Exception:
                    log.exception("inventory consider failed: %s", r.get("coin"))
        for t in list(self._tasks):
            t.cancel()
