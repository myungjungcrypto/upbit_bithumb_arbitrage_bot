"""빗썸 입출금 상태 폴러 — T4 게이트 ①의 첫 조각 (무인증 public API).

김프 급등의 최다 원인은 입금 정지다. 이 수집기는 전 코인의 입출금 가능 여부를
주기 폴링해서 '변화'만 발행한다 (상태 플립 = WARN/INFO 알림 + 저장).

VERIFY(실배포): 구 public API 응답 유지 여부. 업비트(/v1/status/wallet)·해외 3사는
읽기 전용 키 필요 → P0.5에서 추가.
"""
from __future__ import annotations

import asyncio
import logging

import aiohttp

from ..bus import Bus
from ..models import Health, WalletStatus, now_ms

log = logging.getLogger("collector.wallet.bithumb")

URL = "https://api.bithumb.com/public/assetsstatus/ALL"


def parse_assetsstatus(data: dict) -> list[tuple[str, bool, bool]]:
    """응답 → [(코인, 입금가능, 출금가능)]. 형식 이상 항목은 건너뜀 (순수 함수, 테스트 대상)."""
    out: list[tuple[str, bool, bool]] = []
    body = data.get("data")
    if not isinstance(body, dict):
        return out
    for coin, st in body.items():
        if not isinstance(st, dict):
            continue
        try:
            dep = int(st.get("deposit_status", 0)) == 1
            wd = int(st.get("withdrawal_status", 0)) == 1
        except (TypeError, ValueError):
            continue
        out.append((coin.upper(), dep, wd))
    return out


class BithumbWalletStatusCollector:
    name = "wallet.bithumb"

    def __init__(self, bus: Bus, poll_sec: float = 60.0) -> None:
        self.bus = bus
        self.poll_sec = poll_sec
        self._state: dict[str, tuple[bool, bool]] = {}

    async def run(self, stop: asyncio.Event) -> None:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        fail_streak = 0
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as sess:
            while not stop.is_set():
                try:
                    async with sess.get(URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        resp.raise_for_status()
                        data = await resp.json(content_type=None)
                    ts = now_ms()
                    for coin, dep, wd in parse_assetsstatus(data):
                        prev = self._state.get(coin)
                        if prev == (dep, wd):
                            continue  # 변화만 발행
                        self._state[coin] = (dep, wd)
                        self.bus.publish("wallet", WalletStatus("bithumb", coin, dep, wd, ts))
                    fail_streak = 0
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    fail_streak += 1
                    if fail_streak in (3, 20) or fail_streak % 100 == 0:
                        log.warning("assetsstatus fetch failing (streak=%d): %r", fail_streak, e)
                        self.bus.publish(
                            "health",
                            Health(f"collector.{self.name}", "error", f"streak={fail_streak}: {e!r}", now_ms()),
                        )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_sec)
                except asyncio.TimeoutError:
                    pass
