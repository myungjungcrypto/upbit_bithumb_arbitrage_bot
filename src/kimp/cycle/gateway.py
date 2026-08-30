"""출금 게이트웨이 — §4.1 방어선 3~5의 정책 엔진.

전략 코드는 출금 API를 직접 호출하지 못한다. 유일한 경로는 이 게이트웨이의 request()이며,
게이트웨이가 검증한다:
  ① 내부 allowlist 대조 — (코인, 출발 거래소, 도착 거래소)가 사전 등록 조합인지.
     거래소단 주소 화이트리스트(방어선 1)와 이중 잠금
  ② 건당·일일 한도
  ③ 감독 모드: 텔레그램 인라인 버튼 승인 (P3 초기 전 건 — 무응답/미가동 = 거부)
  ④ 전 건 감사 로그 + CRIT 통지

backend 인터페이스로 실제 출금을 분리: PaperWithdrawBackend(시뮬) → P3에서 거래소별
Live 백엔드 교체. 출금 가능 API 키는 Live 백엔드(격리 프로세스)만 갖는다.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal

log = logging.getLogger("gateway")


class PaperWithdrawBackend:
    """시뮬 백엔드 — 실제 출금 없음. 승인 UX·정책 검증용."""

    async def withdraw(self, coin: str, from_ex: str, to_ex: str, amount: Decimal, address: str) -> str:
        log.info("[PAPER-WD] %s %s→%s %s (addr=…%s)", coin, from_ex, to_ex, amount, address[-6:])
        return "PAPER-TX"


class WithdrawalGateway:
    def __init__(self, cfg: dict, control, alerter, backend) -> None:
        self.control = control          # TelegramControl (승인용) — None이면 전 건 거부
        self.alerter = alerter
        self.backend = backend
        self.mode = cfg.get("mode", "supervised")
        self.per_tx_cap = Decimal(str(cfg.get("per_tx_usd_cap", 3000)))
        self.daily_cap = Decimal(str(cfg.get("daily_usd_cap", 10000)))
        self.approval_timeout = float(cfg.get("approval_timeout_sec", 600))
        # allowlist: [{coin, from, to, address}] — 공개 주소라 시크릿 아님. 거래소 등록분과 1:1
        self.allowlist = {
            (str(e["coin"]).upper(), str(e["from"]).lower(), str(e["to"]).lower()): str(e["address"])
            for e in cfg.get("allowlist", [])
        }
        self._daily_used = Decimal(0)
        self._day = self._today()

    @staticmethod
    def _today() -> str:
        return datetime.now(timezone.utc).date().isoformat()

    def _roll_day(self) -> None:
        d = self._today()
        if d != self._day:
            self._day, self._daily_used = d, Decimal(0)

    async def request(
        self, coin: str, from_ex: str, to_ex: str, amount: Decimal, usd_value: Decimal, reason: str
    ) -> str | None:
        """출금 요청. 성공 시 txid, 거부/실패 시 None. 모든 결정은 알림으로 남는다."""
        from ..alerts.telegram import CRIT, INFO, WARN

        self._roll_day()
        key = (coin.upper(), from_ex.lower(), to_ex.lower())
        address = self.allowlist.get(key)
        if address is None:
            self.alerter.alert(CRIT, f"gw:deny:{coin}",
                               f"🚫 출금 거부 — 미등록 조합 {coin} {from_ex}→{to_ex} (${usd_value:,.0f}) · {reason}",
                               cooldown=0)
            return None
        if usd_value > self.per_tx_cap:
            self.alerter.alert(CRIT, f"gw:cap:{coin}",
                               f"🚫 출금 거부 — 건당 한도 초과 ${usd_value:,.0f} > ${self.per_tx_cap:,.0f} · {reason}",
                               cooldown=0)
            return None
        if self._daily_used + usd_value > self.daily_cap:
            self.alerter.alert(CRIT, "gw:daily",
                               f"🚫 출금 거부 — 일일 한도 (사용 ${self._daily_used:,.0f} + ${usd_value:,.0f} > ${self.daily_cap:,.0f})",
                               cooldown=0)
            return None

        if self.mode == "supervised":
            if self.control is None:
                self.alerter.alert(CRIT, "gw:noctl", "🚫 출금 거부 — 감독 모드인데 관제탑 미가동", cooldown=0)
                return None
            ok = await self.control.request_approval(
                f"출금: {coin} {amount} ({from_ex} → {to_ex}, ≈${usd_value:,.0f})\n사유: {reason}\n주소: …{address[-8:]}",
                self.approval_timeout,
            )
            if not ok:
                self.alerter.alert(WARN, f"gw:denied:{coin}",
                                   f"출금 미승인 — {coin} {from_ex}→{to_ex} ${usd_value:,.0f} (거부/타임아웃)",
                                   cooldown=0)
                return None

        try:
            txid = await self.backend.withdraw(coin, from_ex, to_ex, amount, address)
        except Exception as e:
            self.alerter.alert(CRIT, f"gw:fail:{coin}",
                               f"🚨 출금 실행 실패 — {coin} {from_ex}→{to_ex}: {e!r}", cooldown=0)
            return None
        self._daily_used += usd_value
        self.alerter.alert(INFO, f"gw:ok:{coin}",
                           f"출금 실행 — {coin} {amount} {from_ex}→{to_ex} (tx={txid}, 오늘 누적 ${self._daily_used:,.0f})",
                           cooldown=0)
        return txid
