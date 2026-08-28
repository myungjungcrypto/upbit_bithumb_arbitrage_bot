"""T13 리스크 한도 집행 (운용자 승인 2026-08-28).

사이클당 $2,000 / 동시 in-flight 총 $6,000 / 코인당 $3,000 / 시간당 신규 10건 /
일 실현손실 −$300 → 자동 L1 / 연속 실패 3회 → 자동 L1.
전부 config로 조정 가능. L1 = 신규 진입 차단 (진행 중 사이클은 완주 — §1.3).
"""
from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal


class RiskManager:
    def __init__(self, cfg: dict) -> None:
        self.cycle_cap = Decimal(str(cfg.get("cycle_cap_usd", 2000)))
        self.inflight_cap = Decimal(str(cfg.get("inflight_cap_usd", 6000)))
        self.per_coin_cap = Decimal(str(cfg.get("per_coin_cap_usd", 3000)))
        self.hourly_entries = int(cfg.get("hourly_entries", 10))
        self.daily_loss_limit = float(cfg.get("daily_loss_limit_usd", 300))
        self.max_consecutive_failures = int(cfg.get("max_consecutive_failures", 3))

        self.open_notional: dict[str, Decimal] = {}   # cycle_id -> notional
        self.open_by_coin: dict[str, Decimal] = {}
        self._entry_times: deque[float] = deque()
        self.daily_pnl = 0.0
        self._day = self._today()
        self.consecutive_failures = 0
        self.halted: str | None = None                # None=정상, str=L1 사유

    @staticmethod
    def _today() -> str:
        return datetime.now(timezone.utc).date().isoformat()

    def _roll_day(self) -> None:
        d = self._today()
        if d != self._day:
            self._day = d
            self.daily_pnl = 0.0
            if self.halted and self.halted.startswith("일손실"):
                self.halted = None  # 일손실 L1은 UTC 새 날에 자동 해제

    def check_entry(self, coin: str, notional: Decimal) -> str | None:
        """진입 가능하면 None, 불가면 사유 문자열 (0-지연 — 전부 인메모리)."""
        self._roll_day()
        if self.halted:
            return f"L1 정지 중: {self.halted}"
        if notional > self.cycle_cap:
            return f"사이클 상한 초과 (${notional} > ${self.cycle_cap})"
        total = sum(self.open_notional.values(), Decimal(0))
        if total + notional > self.inflight_cap:
            return f"in-flight 총액 상한 (${total}+${notional} > ${self.inflight_cap})"
        coin_open = self.open_by_coin.get(coin, Decimal(0))
        if coin_open + notional > self.per_coin_cap:
            return f"{coin} 코인당 상한 (${coin_open}+${notional} > ${self.per_coin_cap})"
        now = time.monotonic()
        while self._entry_times and now - self._entry_times[0] > 3600:
            self._entry_times.popleft()
        if len(self._entry_times) >= self.hourly_entries:
            return f"시간당 신규 한도 ({self.hourly_entries}건)"
        return None

    def on_entry(self, cycle_id: str, coin: str, notional: Decimal) -> None:
        self.open_notional[cycle_id] = notional
        self.open_by_coin[coin] = self.open_by_coin.get(coin, Decimal(0)) + notional
        self._entry_times.append(time.monotonic())

    def on_close(self, cycle_id: str, coin: str, pnl_usd: float, failed: bool = False) -> str | None:
        """정산/중단 반영. L1 발동 시 사유 반환 (호출측이 CRIT 알림)."""
        self._roll_day()
        notional = self.open_notional.pop(cycle_id, Decimal(0))
        if coin in self.open_by_coin:
            self.open_by_coin[coin] = max(self.open_by_coin[coin] - notional, Decimal(0))
        self.daily_pnl += pnl_usd
        self.consecutive_failures = self.consecutive_failures + 1 if failed else 0
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.halted = f"연속 실패 {self.consecutive_failures}회"
            return self.halted
        if self.daily_pnl <= -self.daily_loss_limit:
            self.halted = f"일손실 한도 도달 (${self.daily_pnl:,.0f})"
            return self.halted
        return None
