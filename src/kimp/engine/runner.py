"""PremiumEngine — 호가/환율 이벤트를 받아 코인별 김프 틱을 산출해 bus로 발행.

발행 형식: 행 딕셔너리 리스트 (topic "premium"). 한 행 = (코인, 국내거래소, 금액대).
저장·알림 소비자가 그대로 사용한다.
"""
from __future__ import annotations

import time
from decimal import Decimal

from ..bus import Bus
from ..config import Config
from ..models import Book, Fx, now_ms
from .books import BookStore
from .premium import exec_premium, inbound_gross_edge, outbound_gross_edge, theo_premium


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

    def on_fx(self, fx: Fx) -> None:
        self.fx_rate = fx.rate
        self.fx_ts = fx.ts_local

    def on_book(self, book: Book) -> None:
        # KRW-USDT 마켓 갱신은 전 코인에 영향을 주지만, 코인별 스로틀이 폭주를 막는다
        coins = self.coins if book.base == "USDT" else [book.base]
        for coin in coins:
            if coin not in self._coin_set:
                continue
            if self._throttled(coin):
                continue
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
                        "out_net": _f(out_gross - fee_total - wd_ratio) if out_gross is not None else None,
                        "dom_ts_lag_ms": ts - dom.ts_local,
                        "ovs_ts_lag_ms": ts - ovs.ts_local,
                    }
                )
        return rows


def _f(x) -> float | None:
    """파생 비율값은 저장·비교용으로 float 변환 (원본 가격은 Decimal 유지)."""
    return float(x) if x is not None else None
