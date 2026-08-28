"""PremiumEngine — 호가/환율 이벤트를 받아 코인별 김프 틱을 산출해 bus로 발행.

발행 형식: 행 딕셔너리 리스트 (topic "premium"). 한 행 = (코인, 국내거래소, 금액대).
저장·알림 소비자가 그대로 사용한다.
"""
from __future__ import annotations

import time
from decimal import Decimal

from ..bus import Bus
from ..config import Config
from ..models import Book, D, Fx, now_ms
from .books import BookStore
from .premium import (
    capacity_at_threshold,
    exec_premium,
    inbound_gross_edge,
    outbound_gross_edge,
    theo_premium,
)


class PremiumEngine:
    def __init__(self, bus: Bus, store: BookStore, cfg: Config, coins: list[str] | None = None) -> None:
        self.bus = bus
        self.store = store
        self.cfg = cfg
        self.coins = list(coins) if coins is not None else cfg.coins
        self._coin_set = set(self.coins)
        self.fx_rate: Decimal | None = None
        self.fx_ts: int = 0
        self._last_calc: dict[str, float] = {}
        self._edge_threshold = D(cfg.alerts.get("net_edge_threshold", 0.005))
        # KRW-USDT 마켓은 매우 활발해서 전 코인 재계산을 상시 유발함 — 팬아웃은 별도(느린) 스로틀.
        # 코인 자신의 호가 이벤트는 여전히 min_interval_ms(기본 200ms)로 즉시 반응 (§1.4 속도 원칙 유지)
        self._usdt_fanout_ms = int(cfg.raw.get("engine", {}).get("usdt_fanout_interval_ms", 1000))
        self._last_fanout: dict[str, float] = {}

    def on_fx(self, fx: Fx) -> None:
        self.fx_rate = fx.rate
        self.fx_ts = fx.ts_local

    def on_book(self, book: Book) -> None:
        if book.base == "USDT":
            now = time.monotonic()
            for coin in self.coins:
                last = self._last_fanout.get(coin)
                if last is not None and (now - last) * 1000 < self._usdt_fanout_ms:
                    continue
                self._last_fanout[coin] = now
                if self._throttled(coin):
                    continue
                rows = self.compute(coin)
                if rows:
                    self.bus.publish("premium", rows)
            return
        coin = book.base
        if coin not in self._coin_set or self._throttled(coin):
            return
        rows = self.compute(coin)
        if rows:
            self.bus.publish("premium", rows)

    def _throttled(self, coin: str) -> bool:
        now = time.monotonic()
        last = self._last_calc.get(coin, 0.0)
        if (now - last) * 1000 < self.cfg.engine_min_interval_ms:
            return True
        self._last_calc[coin] = now
        return False

    def compute(self, coin: str) -> list[dict]:
        stale_ms = self.cfg.book_stale_ms
        ovs = self.store.fresh(self.cfg.overseas_ref, coin, "USDT", stale_ms)
        if ovs is None:
            return []

        fx_ok = self.fx_rate is not None and now_ms() - self.fx_ts <= self.cfg.fx_stale_ms

        rows: list[dict] = []
        ts = now_ms()
        for dom_ex in self.cfg.domestic:
            dom = self.store.fresh(dom_ex, coin, "KRW", stale_ms)
            usdt = self.store.fresh(dom_ex, "USDT", "KRW", stale_ms)
            if dom is None or usdt is None:
                continue

            theo = theo_premium(dom, ovs, self.fx_rate) if fx_ok else None
            execp = exec_premium(dom, ovs, usdt)
            usdt_mid = usdt.mid

            fee_dom = self.cfg.taker_fee(dom_ex)
            fee_ovs = self.cfg.taker_fee(self.cfg.overseas_ref)
            # 왕복 3-레그: 해외 1회 + 국내 2회 (코인 레그 + USDT 복귀 레그)
            fee_total = fee_ovs + fee_dom * 2
            wd_cost_usd = self.cfg.withdraw_fee_usd(coin) + self.cfg.withdraw_fee_usd("USDT")
            # OUT은 국내 코인 출금에 정률 수수료 추가 (빗썸 알트 ~1% — 운용자 실측)
            out_wd_pct = self.cfg.withdraw_fee_pct(dom_ex, coin)

            # capacity: 임계 순엣지를 유지하는 최대 명목 (§1.4 사이징 자동화)
            thr = self._edge_threshold
            usdt_mid_now = usdt.mid

            def in_net(n: Decimal) -> Decimal | None:
                g = inbound_gross_edge(ovs, dom, usdt, n)
                return None if g is None else g - fee_total - wd_cost_usd / n

            def out_net(n_usd: Decimal) -> Decimal | None:
                if usdt_mid_now is None or usdt_mid_now <= 0:
                    return None
                g = outbound_gross_edge(dom, ovs, usdt, n_usd * usdt_mid_now)
                return None if g is None else g - fee_total - out_wd_pct - wd_cost_usd / n_usd

            in_cap = capacity_at_threshold(in_net, thr)
            out_cap = capacity_at_threshold(out_net, thr)

            for notional_usd in self.cfg.ladder_usd:
                if usdt_mid is None or usdt_mid <= 0:
                    continue
                notional_krw = notional_usd * usdt_mid
                in_gross = inbound_gross_edge(ovs, dom, usdt, notional_usd)
                out_gross = outbound_gross_edge(dom, ovs, usdt, notional_krw)
                wd_ratio = wd_cost_usd / notional_usd
                rows.append(
                    {
                        "ts": ts,
                        "coin": coin,
                        "dom_ex": dom_ex,
                        "ovs_ex": self.cfg.overseas_ref,
                        "notional_usd": float(notional_usd),
                        "theo_mid": _f(theo),
                        "exec_mid": _f(execp),
                        "usdtkrw_mid": _f(usdt_mid),
                        "fx_usdkrw": _f(self.fx_rate) if fx_ok else None,
                        "in_gross": _f(in_gross),
                        "in_net": _f(in_gross - fee_total - wd_ratio) if in_gross is not None else None,
                        "out_gross": _f(out_gross),
                        "out_net": _f(out_gross - fee_total - out_wd_pct - wd_ratio) if out_gross is not None else None,
                        "dom_ts_lag_ms": ts - dom.ts_local,
                        "ovs_ts_lag_ms": ts - ovs.ts_local,
                        "in_capacity_usd": _f(in_cap),
                        "out_capacity_usd": _f(out_cap),
                    }
                )
        return rows


def _f(x) -> float | None:
    """파생 비율값은 저장·비교용으로 float 변환 (원본 가격은 Decimal 유지)."""
    return float(x) if x is not None else None
