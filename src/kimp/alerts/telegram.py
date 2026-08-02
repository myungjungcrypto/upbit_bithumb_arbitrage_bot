"""텔레그램 알림 — 심각도 계층 + 키별 쿨다운 + INFO 홍수 방지 (PLAN §1.3).

정책:
- WARN/CRIT: 즉시 전송 (쿨다운만 적용)
- INFO: 분당 예산(토큰 버킷) 내에서 즉시 전송, 초과분은 60초 다이제스트로 묶음
  → 평시 소수 기회는 즉시 도착(속도 원칙), 시장 전체가 움직일 때는 요약 1건
- 토큰 미설정 시 로그 전용. 전송은 백그라운드 큐 — 핫패스를 절대 블록하지 않음
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

log = logging.getLogger("alerts.telegram")

INFO, WARN, CRIT = "INFO", "WARN", "CRIT"
_EMOJI = {INFO: "ℹ️", WARN: "⚠️", CRIT: "\U0001f6a8"}


class Alerter:
    def __init__(
        self,
        token: str,
        chat_id: str,
        cooldown_sec: float = 300.0,
        max_immediate_per_min: int = 6,
        digest_flush_sec: float = 60.0,
    ) -> None:
        self.token = token
        self.chat_id = chat_id
        self.cooldown_sec = cooldown_sec
        self.digest_flush_sec = digest_flush_sec
        self._last_sent: dict[str, float] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=200)
        self._digest: dict[str, str] = {}
        self._cap = float(max_immediate_per_min)
        self._tokens = float(max_immediate_per_min)
        self._rate = max_immediate_per_min / 60.0
        self._last_refill = time.monotonic()

    @property
    def live(self) -> bool:
        return bool(self.token and self.chat_id)

    def _take_token(self) -> bool:
        now = time.monotonic()
        self._tokens = min(self._cap, self._tokens + (now - self._last_refill) * self._rate)
        self._last_refill = now
        if self._tokens >= 1:
            self._tokens -= 1
            return True
        return False

    def alert(self, severity: str, key: str, text: str, cooldown: float | None = None) -> None:
        """key별 쿨다운 → INFO는 예산 검사 → 전송 큐. 핫패스에서 호출해도 안전 (논블로킹)."""
        now = time.monotonic()
        cd = self.cooldown_sec if cooldown is None else cooldown
        if now - self._last_sent.get(key, 0.0) < cd:
            return
        self._last_sent[key] = now
        msg = f"{_EMOJI.get(severity, '')} [{severity}] {text}"
        log.info("alert %s: %s", key, msg)
        if not self.live:
            return
        if severity == INFO and not self._take_token():
            self._digest[key] = text  # 예산 초과 → 다이제스트로
            return
        try:
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            log.warning("alert queue full, dropped: %s", key)

    async def digest_loop(self, stop: asyncio.Event) -> None:
        """예산 초과 INFO를 주기적으로 1건으로 묶어 전송."""
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.digest_flush_sec)
            except asyncio.TimeoutError:
                pass
            if not self._digest:
                continue
            items = list(self._digest.values())
            self._digest.clear()
            body = "\n".join(f"· {t}" for t in items[:20])
            if len(items) > 20:
                body += f"\n… 외 {len(items) - 20}건"
            try:
                self._queue.put_nowait(f"ℹ️ [INFO 요약 {int(self.digest_flush_sec)}s] {len(items)}건\n{body}")
            except asyncio.QueueFull:
                pass

    async def sender(self, stop: asyncio.Event) -> None:
        if not self.live:
            await stop.wait()
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        async with aiohttp.ClientSession(trust_env=True) as sess:
            while not (stop.is_set() and self._queue.empty()):
                try:
                    msg = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                try:
                    async with sess.post(
                        url,
                        json={"chat_id": self.chat_id, "text": msg},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            log.warning("telegram send failed: %s %s", resp.status, await resp.text())
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("telegram send error: %r", e)
