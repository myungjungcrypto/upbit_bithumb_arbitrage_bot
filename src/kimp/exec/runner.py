"""전송형 라이브 사이클 러너 (M3ⓔⓕ) — 부품 결선: 게이트 → 저널 → 주문 어댑터 → 출금 게이트웨이 → 입금 감시 → 매도.

안전 잠금 스택 (§4.1 방어선 6 — 전부 통과해야 발사, can_fire()):
  ① execution.mode == live (config)         ② LIVE_TRADING_ALLOWED=1 (환경 잠금, live만)
  ③ /arm <코인> [분] 텔레그램 수동 arm + TTL   ④ execution.routes 라우트 allowlist
  ⑤ V7 레그 allowlist·blocklist + T4 지갑 게이트 (페이퍼와 동일 함수)   ⑥ T13 한도
  ⑦ execution.max_cycles 총 실행 상한 (최소 금액 테스트 = 1)
dry_run 모드는 ②를 제외한 전 잠금과 저널·승인 UX를 그대로 밟되, 시뮬 어댑터(즉시 체결)·
페이퍼 출금·시뮬 입금으로 돈이 움직이지 않는 리허설이다 — 서버에서 라이브 전 마지막 점검용.

원칙 (인계서 §9): 의도 선기록 → 발사 / timeout이면 client ID 조회, 재주문 금지 / 첫 레그 체결량만큼만
다음 레그 / 상태 불명·불일치 = RECONCILE_REQUIRED로 자동화 중단 + CRIT / 재시작 시 저널부터 복구.
"""
from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

import aiohttp

from ..alerts.telegram import CRIT, INFO, WARN
from ..config import Config
from ..engine.premium import vwap_buy
from ..models import D, now_ms
from ..paper.engine import leg_allowed, wallet_block_reason
from ..symbols import DOMESTIC
from .base import OrderAdapter, OrderResult, live_allowed
from .journal import ExecutionJournal, buy_client_id, next_action, sell_client_id
from .rules import (
    BITHUMB_KRW_TICKS, MIN_NOTIONAL_KRW, QTY_STEP_KRW, UPBIT_KRW_TICKS,
    check_min_notional, ioc_buy_price, ioc_sell_price, krw_tick, quantize_qty,
)

log = logging.getLogger("exec.runner")


# ---------- dry_run 시뮬 부품 (돈이 움직이지 않는 리허설) ----------

class SimOrderAdapter(OrderAdapter):
    """요청 가격·수량에 즉시 전량 체결. get_order는 client_id로 마지막 결과 재현 (복구 경로 리허설)."""

    def __init__(self, exchange: str, fee_rate: Decimal = D("0.001")) -> None:
        super().__init__(allow_live=False)
        self.exchange = exchange
        self.fee_rate = fee_rate
        self._orders: dict[str, OrderResult] = {}

    async def instrument_rules(self, sess, base, quote="USDT"):
        return D("0.0001"), D("0.0001"), D("1"), D("5")

    async def place_ioc(self, sess, side, base, quote, price, qty, client_id) -> OrderResult:
        fee = qty * self.fee_rate if side == "buy" else qty * price * self.fee_rate
        r = OrderResult(self.exchange, f"sim-{client_id}", client_id, "filled", qty, price,
                        fee, base if side == "buy" else quote, {"sim": True})
        self._orders[client_id] = r
        return r

    async def get_order(self, sess, base, quote, order_id="", client_id="") -> OrderResult:
        return self._orders.get(client_id) or OrderResult(self.exchange, "", client_id, "unknown")


class SimDepositWatcher:
    def __init__(self, delay_sec: float = 3.0) -> None:
        self.delay_sec = delay_sec

    async def wait_for(self, exchange, coin, expected, timeout_sec, stop=None, min_ratio=D("0.9")):
        await asyncio.sleep(min(self.delay_sec, timeout_sec))
        return {"id": "sim-dep", "amount": expected, "txid": "sim", "raw": {}}  # 시뮬 = 전량 도착


# ---------- 러너 ----------

class LiveCycleRunner:
    def __init__(self, cfg: Config, bus, books, journal: ExecutionJournal, risk, alerter, control,
                 gateway, watcher, adapters: dict[str, OrderAdapter], wallet_state: dict,
                 blocklist: set[str], verified_ok, ledger) -> None:
        self.cfg, self.bus, self.books = cfg, bus, books
        self.journal, self.risk, self.alerter, self.control = journal, risk, alerter, control
        self.gateway, self.watcher, self.adapters = gateway, watcher, adapters
        self.wallet_state, self.blocklist, self.verified_ok, self.ledger = wallet_state, blocklist, verified_ok, ledger
        e = cfg.raw.get("execution", {}) or {}
        m = e.get("mode", "off")                                   # off | dry_run | live
        self.mode = "off" if m in (False, None, "False", "false") else str(m)  # YAML의 off → False 정규화
        self.routes = {(str(r["coin"]).upper(), str(r["dom"]), str(r["ovs"]), str(r.get("direction", "in")))
                       for r in e.get("routes", [])}
        self.max_notional = D(e.get("max_notional_usd", 100))
        self.max_cycles = int(e.get("max_cycles", 1))
        self.entry_thr = D(e.get("entry_threshold", cfg.raw.get("paper", {}).get("entry_threshold", 0.005)))
        self.max_edge = D(cfg.raw.get("paper", {}).get("max_edge", 0.05))
        self.margin = D(e.get("ioc_margin_pct", 0.002))
        self.min_fill_pct = D(e.get("min_fill_pct", 0.10))       # T6 확정: 목표의 10% 미만 체결 = ABORT
        self.order_timeout = float(e.get("order_timeout_sec", 10))
        self.withdraw_buffer_pct = D(e.get("withdraw_buffer_pct", 0.005))
        self.deposit_timeout_mult = float(e.get("deposit_timeout_mult", 4))
        self.arm_ttl = float(e.get("arm_ttl_sec", 900))
        self.transfer_min = cfg.raw.get("paper", {}).get("transfer_minutes", {}) or {}
        self.stale_ms = cfg.book_stale_ms
        self._armed_coin: str | None = None
        self._armed_until = 0.0
        self.cycles_run = 0
        self._busy = False
        self._tasks: set[asyncio.Task] = set()
        self._sess: aiohttp.ClientSession | None = None  # 인증 REST keep-alive (§1.4 — 임계 경로에서 TLS 제거)

    # ---------- 잠금 스택 ----------

    def arm(self, args: str) -> str:
        """/arm <코인> [분] — 확인 문자열 = 코인명. TTL 만료 시 자동 해제 (인계서 §2.13 승인 TTL)."""
        parts = args.split()
        if not parts:
            return "사용법: /arm <코인> [분] — 예: /arm PROM 15"
        coin = parts[0].upper()
        minutes = float(parts[1]) if len(parts) > 1 else self.arm_ttl / 60
        if not any(r[0] == coin for r in self.routes):
            return f"❌ {coin}: execution.routes에 없는 코인 (허용: {sorted({r[0] for r in self.routes}) or '-'})"
        self._armed_coin, self._armed_until = coin, time.monotonic() + minutes * 60
        return (f"🔫 ARMED {coin} — {minutes:.0f}분간 (모드 {self.mode}, 환경잠금 "
                f"{'OPEN' if live_allowed() else 'LOCKED'}, 상한 ${self.max_notional}, 남은 사이클 {self.max_cycles - self.cycles_run})")

    def disarm(self) -> str:
        self._armed_coin, self._armed_until = None, 0.0
        return "🔒 DISARMED"

    def status(self) -> str:
        armed = self._armed_coin and time.monotonic() < self._armed_until
        left = max(self._armed_until - time.monotonic(), 0) / 60 if armed else 0
        act = self.journal.active()
        return (f"EXEC 모드 {self.mode} · 환경잠금 {'OPEN' if live_allowed() else 'LOCKED'} · "
                f"{'ARMED ' + self._armed_coin + f' ({left:.0f}분)' if armed else 'DISARMED'} · "
                f"실행 {self.cycles_run}/{self.max_cycles} · 진행 중 저널 {len(act)}건"
                + (f" ({', '.join(str(j['id']) + ':' + j['state'] for j in act)})" if act else ""))

    def can_fire(self, coin: str, dom_ex: str, ovs_ex: str, direction: str) -> str | None:
        """None=발사 가능, str=차단 사유 (0-지연 — 전부 인메모리)."""
        if self.mode not in ("dry_run", "live"):
            return "execution.mode off"
        if self.mode == "live" and not live_allowed():
            return "환경 잠금 (LIVE_TRADING_ALLOWED)"
        if not (self._armed_coin == coin.upper() and time.monotonic() < self._armed_until):
            return "미arm 또는 TTL 만료"
        if (coin.upper(), dom_ex, ovs_ex, direction) not in self.routes:
            return "라우트 allowlist 외"
        if self.cycles_run >= self.max_cycles:
            return "max_cycles 도달"
        if self._busy:
            return "사이클 진행 중"
        if not leg_allowed(self.blocklist, self.verified_ok, coin, dom_ex, ovs_ex):
            return "V7 레그 미검증/차단"
        w = wallet_block_reason(self.wallet_state, direction, coin, dom_ex, ovs_ex)
        if w:
            return w
        return None

    # ---------- 진입 (premium 행 → 사이클) ----------

    def consider(self, row: dict) -> None:
        coin, dom_ex, ovs_ex = str(row["coin"]).upper(), row["dom_ex"], row["ovs_ex"]
        for direction in ("in", "out"):
            net = row.get(f"{direction}_net")
            if net is None or D(net) < self.entry_thr or D(net) > self.max_edge:
                continue
            if self.can_fire(coin, dom_ex, ovs_ex, direction) is not None:
                continue
            cap = row.get(f"{direction}_capacity_usd") or 0
            notional = min(self.max_notional, self.risk.cycle_cap, D(cap) if cap else self.max_notional)
            if notional <= 0:
                continue
            reason = self.risk.check_entry(coin, notional)
            if reason:
                log.info("exec 진입 차단 %s: %s", coin, reason)
                continue
            self._spawn(self.run_cycle(coin, dom_ex, ovs_ex, direction, notional, float(net)))
            return

    def _spawn(self, coro) -> None:
        t = asyncio.create_task(coro, name="exec.cycle")
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    # ---------- 사이클 본체 ----------

    def _legs(self, coin, dom_ex, ovs_ex, direction):
        """(매수 거래소, 매수 quote, 매도 거래소, 매도 quote)."""
        return (ovs_ex, "USDT", dom_ex, "KRW") if direction == "in" else (dom_ex, "KRW", ovs_ex, "USDT")

    async def _rules(self, ex: str, coin: str, quote: str, price: Decimal):
        if ex in DOMESTIC:
            table = UPBIT_KRW_TICKS if ex == "upbit" else BITHUMB_KRW_TICKS
            return krw_tick(price, table), QTY_STEP_KRW, MIN_NOTIONAL_KRW
        tick, lot, min_qty, min_notional = await self.adapters[ex].instrument_rules(self._sess, coin, quote)
        return tick, lot, max(min_notional, min_qty * price)  # 해외: 수량 하한·명목 하한 중 큰 쪽

    async def _place_with_recovery(self, ex, side, coin, quote, price, qty, client_id) -> OrderResult:
        """발사 → timeout이면 재주문 없이 client_id로 조회 (최대 3회). 그래도 불명이면 unknown 반환."""
        adapter = self.adapters[ex]
        try:
            return await asyncio.wait_for(adapter.place_ioc(self._sess, side, coin, quote, price, qty, client_id),
                                          timeout=self.order_timeout)
        except asyncio.TimeoutError:
            self.alerter.alert(WARN, f"exec:timeout:{client_id}", f"주문 timeout — {ex} {side} {coin} 재조회 (재주문 없음)", cooldown=0)
        except Exception as e:
            self.alerter.alert(WARN, f"exec:err:{client_id}", f"주문 오류 — {ex} {side} {coin}: {e!r} → 재조회", cooldown=0)
        for _ in range(3):
            await asyncio.sleep(1.0)
            try:
                r = await adapter.get_order(self._sess, coin, quote, client_id=client_id)
                if r.status != "unknown":
                    return r
            except Exception:
                pass
        return OrderResult(ex, "", client_id, "unknown")

    def _usd_of_krw(self, dom_ex: str, krw: Decimal) -> Decimal | None:
        usdt = self.books.fresh(dom_ex, "USDT", "KRW", self.stale_ms)
        if usdt is None:
            return None
        r = vwap_buy(usdt.asks, krw)
        return r[0] if r else None

    async def run_cycle(self, coin, dom_ex, ovs_ex, direction, notional: Decimal, edge: float) -> None:
        route = f"{coin}:{dom_ex}>{ovs_ex}:{direction}"
        jid = self.journal.begin(route, {"coin": coin, "dom_ex": dom_ex, "ovs_ex": ovs_ex, "direction": direction,
                                         "notional_usd": float(notional), "edge": edge, "mode": self.mode})
        if jid is None:
            return
        self._busy = True
        self.cycles_run += 1
        self.risk.on_entry(f"live-{jid}", coin, notional)
        buy_ex, buy_q, sell_ex, sell_q = self._legs(coin, dom_ex, ovs_ex, direction)
        self.alerter.alert(INFO, f"exec:start:{jid}",
                           f"[{self.mode.upper()}] #{jid} {coin} {direction.upper()} 시작 ${notional} @엣지 {edge*100:.2f}% ({buy_ex} 매수 → {sell_ex} 매도)",
                           cooldown=0)
        try:
            await self._run_from_buy(jid, coin, buy_ex, buy_q, sell_ex, sell_q, notional)
        except Exception as e:
            log.exception("cycle %s failed", jid)
            self._reconcile(jid, coin, f"예외: {e!r}")
        finally:
            self._busy = False

    async def _run_from_buy(self, jid, coin, buy_ex, buy_q, sell_ex, sell_q, notional: Decimal) -> None:
        book = self.books.fresh(buy_ex, coin, buy_q, self.stale_ms)
        if book is None or not book.asks:
            return self._abort(jid, coin, "매수 호가 stale/없음 (발사 전)")
        quote_budget = notional if buy_q == "USDT" else (notional * self.books.fresh(buy_ex, "USDT", "KRW", self.stale_ms).mid)
        tick, step, min_notional = await self._rules(buy_ex, coin, buy_q, book.asks[0].price)
        price = ioc_buy_price(book.asks[0].price, self.margin, tick)
        qty = quantize_qty(quote_budget / price, step)
        if not check_min_notional(price, qty, min_notional):
            return self._abort(jid, coin, f"최소 주문 미달 ({price}×{qty} < {min_notional})")
        cid = buy_client_id(jid)
        self.journal.advance(jid, "BUY_SUBMITTED", {"buy": {"ex": buy_ex, "client_id": cid, "price": str(price), "qty": str(qty)}})
        res = await self._place_with_recovery(buy_ex, "buy", coin, buy_q, price, qty, cid)
        await self._after_buy(jid, coin, buy_ex, buy_q, sell_ex, sell_q, qty, res)

    async def _after_buy(self, jid, coin, buy_ex, buy_q, sell_ex, sell_q, target_qty: Decimal, res: OrderResult) -> None:
        act = next_action("BUY_SUBMITTED", res, None)
        if act == "reconcile":
            return self._reconcile(jid, coin, "매수 상태 불명 — 거래소에서 직접 확인 (재주문 금지)")
        if act == "abort":
            return self._abort(jid, coin, "매수 전량 미체결 (자금 이동 없음)")
        filled = res.filled_qty
        if filled < target_qty * self.min_fill_pct:
            # T6 확정: 10% 미만 체결 → 즉시 반대매매 후 ABORT
            sb = self.books.fresh(buy_ex, coin, buy_q, self.stale_ms)
            if sb and sb.bids:
                tick, step, _ = await self._rules(buy_ex, coin, buy_q, sb.bids[0].price)
                await self._place_with_recovery(buy_ex, "sell", coin, buy_q, ioc_sell_price(sb.bids[0].price, self.margin, tick),
                                                quantize_qty(filled, step), sell_client_id(jid, 9))
            return self._abort(jid, coin, f"부분체결 {filled}/{target_qty} < {self.min_fill_pct*100:.0f}% — 반대매매 후 중단")
        # 매수 수수료가 코인으로 징수되면 가용 수량에서 차감 (OKX 기본)
        avail = filled - (res.fee if res.fee_currency.upper() == coin else D(0))
        self.journal.advance(jid, "BUY_DONE", {"buy_result": {"filled": str(filled), "avg": str(res.avg_price), "fee": str(res.fee),
                                                              "fee_ccy": res.fee_currency, "order_id": res.order_id, "avail": str(avail)}})
        await self._run_from_withdraw(jid, coin, buy_ex, sell_ex, sell_q, avail, res)

    async def _run_from_withdraw(self, jid, coin, buy_ex, sell_ex, sell_q, avail: Decimal, buy_res: OrderResult) -> None:
        amount = quantize_qty(avail * (1 - self.withdraw_buffer_pct), D("0.00000001"))
        usd = amount * (buy_res.avg_price or D(0)) if buy_ex not in DOMESTIC else (self._usd_of_krw(buy_ex, amount * (buy_res.avg_price or D(0))) or D(0))
        self.journal.advance(jid, "WITHDRAW_REQUESTED", {"withdraw": {"from": buy_ex, "to": sell_ex, "amount": str(amount), "usd": str(usd)}})
        wd = await self.gateway.request(coin, buy_ex, sell_ex, amount, usd, f"사이클 #{jid} 전송 레그")
        if wd is None:
            return self._reconcile(jid, coin, f"출금 미승인/실패 — {coin} {amount}개가 {buy_ex}에 남아 있음 (수동 처리)")
        self.journal.advance(jid, "WITHDRAW_SENT", {"withdraw_id": wd, "withdraw_ms": now_ms()})
        await self._run_from_deposit(jid, coin, sell_ex, sell_q, amount)

    async def _run_from_deposit(self, jid, coin, sell_ex, sell_q, amount: Decimal) -> None:
        minutes = float(self.transfer_min.get(coin, self.transfer_min.get("default", 15)))
        timeout = minutes * 60 * self.deposit_timeout_mult
        hit = await self.watcher.wait_for(sell_ex, coin, amount, timeout)  # 기대 수량의 90% 이상이면 도착
        if hit is None:
            return self._reconcile(jid, coin, f"입금 미감지 {timeout/60:.0f}분 (STUCK_DEPOSIT) — {sell_ex} 입금 내역 확인")
        self.journal.advance(jid, "DEPOSIT_CREDITED", {"deposit": {"id": hit["id"], "amount": str(hit["amount"]), "txid": hit.get("txid", "")}})
        await self._run_from_sell(jid, coin, sell_ex, sell_q, D(str(hit["amount"])))

    async def _run_from_sell(self, jid, coin, sell_ex, sell_q, qty: Decimal, attempt: int = 0) -> None:
        book = self.books.fresh(sell_ex, coin, sell_q, self.stale_ms)
        if book is None or not book.bids:
            return self._reconcile(jid, coin, f"매도 호가 stale — {coin} {qty}개 {sell_ex}에 도착해 있음 (수동 매도)")
        tick, step, _ = await self._rules(sell_ex, coin, sell_q, book.bids[0].price)
        price = ioc_sell_price(book.bids[0].price, self.margin, tick)
        cid = sell_client_id(jid, attempt)
        self.journal.advance(jid, "SELL_SUBMITTED", {"sell": {"ex": sell_ex, "client_id": cid, "price": str(price), "qty": str(qty), "attempt": attempt}})
        res = await self._place_with_recovery(sell_ex, "sell", coin, sell_q, price, quantize_qty(qty, step), cid)
        await self._after_sell(jid, coin, sell_ex, sell_q, qty, res, attempt)

    async def _after_sell(self, jid, coin, sell_ex, sell_q, qty: Decimal, res: OrderResult, attempt: int) -> None:
        act = next_action("SELL_SUBMITTED", None, res)
        if act == "reconcile":
            return self._reconcile(jid, coin, "매도 상태 불명 — 거래소 확인 (재주문 금지)")
        if act == "submit_sell":
            if attempt >= 3:
                return self._reconcile(jid, coin, f"매도 IOC {attempt+1}회 전량 미체결 — {coin} {qty}개 {sell_ex} 보유 중 (수동 매도)")
            return await self._run_from_sell(jid, coin, sell_ex, sell_q, qty, attempt + 1)  # SELL_SUBMITTED→SELL_SUBMITTED 허용
        proceeds = res.filled_qty * (res.avg_price or D(0))
        self.journal.advance(jid, "SELL_DONE", {"sell_result": {"filled": str(res.filled_qty), "avg": str(res.avg_price),
                                                               "fee": str(res.fee), "fee_ccy": res.fee_currency, "order_id": res.order_id}})
        remainder = qty - res.filled_qty
        self._settle(jid, coin, sell_ex, sell_q, proceeds, res, remainder)

    # ---------- 종결 ----------

    def _settle(self, jid, coin, sell_ex, sell_q, proceeds: Decimal, sell_res: OrderResult, remainder: Decimal) -> None:
        j = self.journal.get(jid)
        buy = j.get("buy_result", {})
        buy_cost = D(buy.get("filled", 0)) * D(buy.get("avg") or 0)
        # USD 공통 회계: KRW 레그는 그 시점 USDT 실행가(asks)로 환산 (인계서 §2.5 방향별 FX)
        if sell_q == "KRW":
            proceeds_usd = self._usd_of_krw(sell_ex, proceeds) or D(0)
            cost_usd = buy_cost
        else:
            proceeds_usd = proceeds
            cost_usd = self._usd_of_krw(j["buy"]["ex"], buy_cost) or D(0)
        fees_usd = D(0)
        if sell_res.fee_currency.upper() == "KRW":
            fees_usd += self._usd_of_krw(sell_ex, sell_res.fee) or D(0)
        elif sell_res.fee_currency.upper() == "USDT":
            fees_usd += sell_res.fee
        pnl = float(proceeds_usd - cost_usd - fees_usd)
        self.journal.advance(jid, "SETTLED", {"pnl_usd": pnl, "remainder": str(remainder)})
        note = f" · 잔량 {remainder} {coin} 미매도" if remainder > 0 else ""
        self.ledger.add({"ts": now_ms(), "mode": self.mode, "kind": j["meta"]["direction"], "coin": coin,
                         "dom_ex": j["meta"]["dom_ex"], "ovs_ex": j["meta"]["ovs_ex"],
                         "notional_usd": float(cost_usd), "entry_edge": j["meta"]["edge"], "pnl_usd": pnl,
                         "state": "SETTLED", "hedged": False, "stamps": "", "note": note.strip()})
        halted = self.risk.on_close(f"live-{jid}", coin, pnl)
        self.alerter.alert(INFO if pnl >= 0 else WARN, f"exec:settled:{jid}",
                           f"[{self.mode.upper()}] #{jid} {coin} 정산 손익 ${pnl:+,.2f}{note}", cooldown=0)
        if halted:
            self.alerter.alert(CRIT, "exec:L1", f"🛑 L1 자동 발동 — {halted}", cooldown=0)

    def _abort(self, jid, coin, why: str) -> None:
        self.journal.advance(jid, "ABORTED", {"why": why})
        self.risk.on_close(f"live-{jid}", coin, 0.0, failed=True)
        self.alerter.alert(WARN, f"exec:abort:{jid}", f"[{self.mode.upper()}] #{jid} {coin} 중단 — {why}", cooldown=0)

    def _reconcile(self, jid, coin, why: str) -> None:
        try:
            self.journal.advance(jid, "RECONCILE_REQUIRED", {"why": why})
        except Exception:
            log.exception("reconcile mark failed")
        self.risk.on_close(f"live-{jid}", coin, 0.0, failed=True)
        self.disarm()
        self.alerter.alert(CRIT, f"exec:reconcile:{jid}",
                           f"🚨 RECONCILE #{jid} {coin} — {why}\n자동화 정지(DISARM). 거래소 확인 후 저널 정리 필요", cooldown=0)

    # ---------- 재시작 복구 ----------

    async def recover(self) -> None:
        """저널의 active 사이클을 거래소 상태와 대조해 재개/중단 (재주문 없음)."""
        for j in self.journal.active():
            jid, st, coin = j["id"], j["state"], j["meta"]["coin"]
            dom_ex, ovs_ex, direction = j["meta"]["dom_ex"], j["meta"]["ovs_ex"], j["meta"]["direction"]
            buy_ex, buy_q, sell_ex, sell_q = self._legs(coin, dom_ex, ovs_ex, direction)
            self.risk.on_entry(f"live-{jid}", coin, D(str(j["meta"]["notional_usd"])))
            self.alerter.alert(WARN, f"exec:recover:{jid}", f"재시작 복구 — 저널 #{jid} {coin} 상태 {st}", cooldown=0)
            if st == "INTENT_RECORDED":
                self._abort(jid, coin, "재시작 — 발사 전 저널")
            elif st == "BUY_SUBMITTED":
                res = await self.adapters[buy_ex].get_order(self._sess, coin, buy_q, client_id=j["buy"]["client_id"])
                await self._after_buy(jid, coin, buy_ex, buy_q, sell_ex, sell_q, D(j["buy"]["qty"]), res)
            elif st in ("BUY_DONE", "WITHDRAW_REQUESTED"):
                self._reconcile(jid, coin, "재시작 — 출금 승인 단계에서 중단됨 (코인이 매수 거래소에 있음)")
            elif st == "WITHDRAW_SENT":
                await self._run_from_deposit(jid, coin, sell_ex, sell_q, D(j["withdraw"]["amount"]))
            elif st == "DEPOSIT_CREDITED":
                await self._run_from_sell(jid, coin, sell_ex, sell_q, D(j["deposit"]["amount"]))
            elif st == "SELL_SUBMITTED":
                s = j["sell"]
                res = await self.adapters[sell_ex].get_order(self._sess, coin, sell_q, client_id=s["client_id"])
                await self._after_sell(jid, coin, sell_ex, sell_q, D(s["qty"]), res, int(s.get("attempt", 0)))
            elif st == "SELL_DONE":
                self._reconcile(jid, coin, "재시작 — 정산 직전 중단 (손익 수동 확인)")

    async def run(self, stop: asyncio.Event) -> None:
        if self.mode not in ("dry_run", "live"):
            await stop.wait()
            return
        self._sess = aiohttp.ClientSession(trust_env=True, headers={"Accept": "application/json"})
        await self.recover()
        q = self.bus.subscribe("premium")
        log.info("exec runner: %s — routes=%s max_notional=$%s max_cycles=%d", self.mode, sorted(self.routes), self.max_notional, self.max_cycles)
        while not stop.is_set():
            try:
                rows = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            for r in rows:
                try:
                    self.consider(r)
                except Exception:
                    log.exception("exec consider failed")
        for t in list(self._tasks):
            t.cancel()
        if self._sess is not None:
            await self._sess.close()
