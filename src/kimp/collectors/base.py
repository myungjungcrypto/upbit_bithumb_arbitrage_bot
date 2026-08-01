"""WS 수집기 공통 베이스 — 재연결(지수 백오프+지터), 스테일 감시, 앱레벨 핑.

설계 원칙 (PLAN §5): 수집기는 거래소별 독립 태스크로 돌고, 죽으면 스스로 재연결한다.
파싱 실패는 해당 메시지만 버리고 연결은 유지한다 (거래소가 필드를 추가해도 죽지 않게).
"""
from __future__ import annotations

import asyncio
import logging
import random
import time

import aiohttp

from ..bus import Bus
from ..models import Health, now_ms


class WSCollector:
    name = "base"
    url = ""
    stale_after_sec = 30.0
    ping_interval_sec: float | None = None  # 프로토콜이 앱레벨 핑을 요구하면 설정 (bybit/okx)

    def __init__(self, bus: Bus) -> None:
        self.bus = bus
        self.log = logging.getLogger(f"collector.{self.name}")
        self._last_msg = 0.0
        self.msg_count = 0

    # --- 하위 클래스 구현 지점 ---
    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """연결 직후 구독 메시지 전송. 연결별 상태 초기화도 여기서."""

    async def handle(self, raw: str | bytes) -> None:
        """수신 메시지 1건 파싱·발행."""

    def app_ping(self):
        """주기 핑 페이로드 (dict → JSON, str → 텍스트). None이면 미사용."""
        return None

    # --- 공통 루프 ---
    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with aiohttp.ClientSession(trust_env=True) as sess:
                    async with sess.ws_connect(
                        self.url, heartbeat=20, max_msg_size=8 * 2**20
                    ) as ws:
                        self.log.info("connected: %s", self.url)
                        self._health("up", "connected")
                        await self.subscribe(ws)
                        backoff = 1.0
                        self._last_msg = time.monotonic()
                        aux = [asyncio.create_task(self._watchdog(ws))]
                        if self.ping_interval_sec:
                            aux.append(asyncio.create_task(self._pinger(ws)))
                        try:
                            async for msg in ws:
                                if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                    self._last_msg = time.monotonic()
                                    self.msg_count += 1
                                    try:
                                        await self.handle(msg.data)
                                    except Exception:
                                        self.log.exception("handle error: %.200s", msg.data)
                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.CLOSING,
                                    aiohttp.WSMsgType.ERROR,
                                ):
                                    break
                        finally:
                            for t in aux:
                                t.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.warning("connection error: %r", e)
            if stop.is_set():
                break
            delay = backoff + random.uniform(0, backoff / 2)
            self._health("down", f"reconnect in {delay:.1f}s")
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)

    async def _watchdog(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """스트림 조용한 죽음 감지 (PLAN §8) — 일정 시간 무수신이면 강제 재연결."""
        while True:
            await asyncio.sleep(5)
            silent = time.monotonic() - self._last_msg
            if silent > self.stale_after_sec:
                self._health("stale", f"no message for {silent:.0f}s, force reconnect")
                await ws.close()
                return

    async def _pinger(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            await asyncio.sleep(self.ping_interval_sec)
            payload = self.app_ping()
            if payload is None:
                continue
            if isinstance(payload, str):
                await ws.send_str(payload)
            else:
                await ws.send_json(payload)

    def _health(self, status: str, detail: str) -> None:
        self.bus.publish("health", Health(f"collector.{self.name}", status, detail, now_ms()))
