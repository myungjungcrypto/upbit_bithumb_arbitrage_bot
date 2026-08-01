"""USD/KRW 환율 폴러 — 이론 김프의 분모 (PLAN §2, 소스 확정은 T8).

NOTE: 기본 소스는 던어무 비공식 엔드포인트 (업비트 UI가 쓰는 것과 동일 소스).
실패해도 시스템은 계속 돈다 — 이론 김프만 결측되고, 트리거인 실행 김프는
USDT/KRW 실거래가 기반이라 영향 없음.
"""
from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..bus import Bus
from ..models import D, Fx, Health, now_ms


class FxCollector:
    name = "fx"

    def __init__(self, bus: Bus, url: str, poll_sec: float = 5.0) -> None:
        self.bus = bus
        self.url = url
        self.poll_sec = poll_sec
        self.log = logging.getLogger("collector.fx")

    async def run(self, stop: asyncio.Event) -> None:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        fail_streak = 0
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as sess:
            while not stop.is_set():
                try:
                    async with sess.get(
                        self.url, timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json(content_type=None)
                        rate = D(data[0]["basePrice"])
                        self.bus.publish("fx", Fx("USD/KRW", rate, "dunamu", now_ms()))
                        fail_streak = 0
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    fail_streak += 1
                    if fail_streak in (3, 20) or fail_streak % 100 == 0:
                        self.log.warning("fx fetch failing (streak=%d): %r", fail_streak, e)
                        self.bus.publish(
                            "health",
                            Health("collector.fx", "error", f"streak={fail_streak}: {e!r}", now_ms()),
                        )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_sec)
                except asyncio.TimeoutError:
                    pass
