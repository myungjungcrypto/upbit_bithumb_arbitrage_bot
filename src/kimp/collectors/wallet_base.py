"""입출금 상태 수집기 공통 베이스 — diff·initial 처리와 폴링 루프 (T4 게이트 ①).

하위 클래스는 fetch(sess) -> [(코인, 입금가능, 출금가능)] 만 구현한다.
첫 스냅샷은 initial=True로 발행(개별 알림 억제용)하고, 정지 중 코인은 요약 1건으로 보고한다.
"""
from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..bus import Bus
from ..models import Health, WalletStatus, now_ms


class WalletStatusCollector:
    exchange = "base"

    def __init__(self, bus: Bus, poll_sec: float = 60.0) -> None:
        self.bus = bus
        self.poll_sec = poll_sec
        self._state: dict[str, tuple[bool, bool]] = {}
        self.log = logging.getLogger(f"collector.wallet.{self.exchange}")

    async def fetch(self, sess: aiohttp.ClientSession) -> list[tuple[str, bool, bool]]:
        raise NotImplementedError

    def _process(self, parsed: list[tuple[str, bool, bool]], ts: int) -> tuple[list[WalletStatus], list[str]]:
        """diff → 이벤트 목록 + (첫 스냅샷일 때만) 정지 코인 요약. 순수 로직 (테스트 대상)."""
        initial = not self._state
        events: list[WalletStatus] = []
        suspended: list[str] = []
        for coin, dep, wd in parsed:
            if self._state.get(coin) == (dep, wd):
                continue
            self._state[coin] = (dep, wd)
            events.append(WalletStatus(self.exchange, coin, dep, wd, ts, initial))
            if initial and not (dep and wd):
                parts = [] if dep else ["입금"]
                if not wd:
                    parts.append("출금")
                suspended.append(f"{coin}({'·'.join(parts)} 정지)")
        return events, suspended

    async def run(self, stop: asyncio.Event) -> None:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        fail_streak = 0
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as sess:
            while not stop.is_set():
                try:
                    parsed = await self.fetch(sess)
                    events, suspended = self._process(parsed, now_ms())
                    for ev in events:
                        self.bus.publish("wallet", ev)
                    if suspended:
                        head = suspended[:20]
                        more = f" 외 {len(suspended)-20}종" if len(suspended) > 20 else ""
                        self.bus.publish(
                            "health",
                            Health(
                                f"collector.wallet.{self.exchange}",
                                "wallet_snapshot",
                                f"{self.exchange} 입출금 정지 중 {len(suspended)}종: {', '.join(head)}{more}",
                                now_ms(),
                            ),
                        )
                    fail_streak = 0
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    fail_streak += 1
                    if fail_streak in (3, 20) or fail_streak % 100 == 0:
                        self.log.warning("wallet status fetch failing (streak=%d): %r", fail_streak, e)
                        self.bus.publish(
                            "health",
                            Health(
                                f"collector.wallet.{self.exchange}",
                                "error",
                                f"streak={fail_streak}: {e!r}",
                                now_ms(),
                            ),
                        )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_sec)
                except asyncio.TimeoutError:
                    pass
