"""텔레그램 알림 — 심각도 계층 + (키별) 쿨다운 (PLAN §1.3).

P0 범위: 발신만 (INFO/WARN/CRIT). 수신 명령(킬스위치·승인 버튼)은 P2 관제탑에서.
토큰 미설정 시 로그 전용으로 동작 — 파이프라인은 텔레그램 없이도 완결.
전송은 백그라운드 태스크로, 핫패스를 절대 블록하지 않는다.
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
    def __init__(self, token: str, chat_id: str, cooldown_sec: float = 300.0) -> None:
        self.token = token
        self.chat_id = chat_id
        self.cooldown_sec = cooldown_sec
        self._last_sent: dict[str, float] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=200)

    @property
    def live(self) -> bool:
        return bool(self.token and self.chat_id)

    def alert(self, severity: str, key: str, text: str, cooldown: float | None = None) -> None:
        """key별 쿨다운 적용해 전송 큐에 적재. 핫패스에서 호출해도 안전 (논블로킹)."""
        now = time.monotonic()
        cd = self.cooldown_sec if cooldown is None else cooldown
        if now - self._last_sent.get(key, 0.0) < cd:
            return
        self._last_sent[key] = now
        msg = f"{_EMOJI.get(severity, '')} [{severity}] {text}"
        log.info("alert %s: %s", key, msg)
        if not self.live:
            return
        try:
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            log.warning("alert queue full, dropped: %s", key)

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
