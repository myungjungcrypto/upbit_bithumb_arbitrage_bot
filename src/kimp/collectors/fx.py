"""USD/KRW 환율 폴러 — 이론 김프의 분모 (PLAN §2, 정식 소스 확정은 T8).

소스 체인 (순서대로 시도, 첫 성공 채택):
1. 던어무 (업비트 UI와 동일 소스, 실시간) — 일부 네트워크에서 DNS 실패 실측됨 (2026-08-02 EC2)
2. open.er-api.com (무료, 일 1회 갱신) — 이론 김프는 분석용이라 일간 정밀도로도 유의미

실패해도 시스템은 계속 돈다 — 이론 김프만 결측되고, 트리거인 실행 김프는
USDT/KRW 실거래가 기반이라 영향 없음.
"""
from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..bus import Bus
from ..models import D, Fx, Health, now_ms


def _parse_dunamu(data) -> "D":
    return D(data[0]["basePrice"])


def _parse_erapi(data) -> "D":
    return D(data["rates"]["KRW"])


DEFAULT_SOURCES = [
    ("dunamu", "https://quotation-api-cdn.dunamu.com/v1/forex/recent?codes=FRX.KRWUSD", _parse_dunamu),
    ("er-api", "https://open.er-api.com/v6/latest/USD", _parse_erapi),
]


class FxCollector:
    name = "fx"

    def __init__(self, bus: Bus, url: str = "", poll_sec: float = 5.0) -> None:
        self.bus = bus
        self.poll_sec = poll_sec
        self.sources = list(DEFAULT_SOURCES)
        if url:  # config가 1순위 소스를 덮어쓸 수 있음
            self.sources[0] = ("config", url, _parse_dunamu)
        self.log = logging.getLogger("collector.fx")

    async def run(self, stop: asyncio.Event) -> None:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        fail_streak = 0
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as sess:
            while not stop.is_set():
                got = False
                for name, url, parse in self.sources:
                    try:
                        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            resp.raise_for_status()
                            data = await resp.json(content_type=None)
                        rate = parse(data)
                        if rate <= 0:
                            raise ValueError(f"bad rate {rate}")
                        self.bus.publish("fx", Fx("USD/KRW", rate, name, now_ms()))
                        got = True
                        fail_streak = 0
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self.log.debug("fx source %s failed: %r", name, e)
                if not got:
                    fail_streak += 1
                    if fail_streak in (3, 20) or fail_streak % 100 == 0:
                        self.log.warning("all fx sources failing (streak=%d)", fail_streak)
                        self.bus.publish(
                            "health",
                            Health("collector.fx", "error", f"전 소스 실패 streak={fail_streak}", now_ms()),
                        )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_sec)
                except asyncio.TimeoutError:
                    pass
