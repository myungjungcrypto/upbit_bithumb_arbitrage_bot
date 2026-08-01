"""OKX v5 현물 WebSocket 수집기 — books5 (5단 스냅샷, 변동 시 ~100ms 주기 푸시).

프로토콜: wss://ws.okx.com:8443/ws/v5/public
- books5는 매 푸시가 완전한 5단 스냅샷 → 로컬 북 유지 불필요
- 25초마다 텍스트 "ping" 필요, 서버는 "pong" 응답
"""
from __future__ import annotations

import json

import aiohttp

from ..bus import Bus
from ..models import Book, D, Level, now_ms
from ..symbols import okx_inst, parse_okx_inst
from .base import WSCollector


class OkxCollector(WSCollector):
    name = "okx"
    url = "wss://ws.okx.com:8443/ws/v5/public"
    ping_interval_sec = 25.0

    def __init__(self, bus: Bus, coins: list[str]) -> None:
        super().__init__(bus)
        self.insts = [okx_inst(c) for c in coins]

    def app_ping(self):
        return "ping"

    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        await ws.send_json(
            {"op": "subscribe", "args": [{"channel": "books5", "instId": i} for i in self.insts]}
        )

    async def handle(self, raw: str | bytes) -> None:
        if raw == "pong":
            return
        d = json.loads(raw)
        if "event" in d:  # 구독 ack / error
            if d.get("event") == "error":
                self.log.warning("okx error: %s", d)
            return
        arg = d.get("arg") or {}
        if arg.get("channel") != "books5":
            return
        inst = arg.get("instId", "")
        base, quote = parse_okx_inst(inst)
        for row in d.get("data") or []:
            bids = tuple(Level(D(x[0]), D(x[1])) for x in row.get("bids", []))
            asks = tuple(Level(D(x[0]), D(x[1])) for x in row.get("asks", []))
            if not bids and not asks:
                continue
            ts = int(row["ts"]) if row.get("ts") else None
            self.bus.publish("book", Book(self.name, base, quote, bids, asks, ts, now_ms()))
