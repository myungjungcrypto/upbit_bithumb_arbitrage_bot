"""바이낸스 현물 WebSocket 수집기 — depth20@100ms 부분 호가 스냅샷.

diff 스트림 대신 20단 부분 스냅샷을 쓴다: 로컬 북 유지가 필요 없어 P0에 적합하고,
100ms 주기가 탐지 요건(<100ms 푸시 기반)과 일치한다.

대형 유니버스(100+ 심볼) 대응: 스트림을 URL에 다 싣지 않고 연결 후 SUBSCRIBE
메시지를 청크로 나눠 보낸다 (바이낸스 WS는 초당 메시지 수 제한이 있음).
NOTE: 부분 스냅샷에는 이벤트 타임스탬프가 없어 ts_exchange=None → 로컬 시각 기준.
"""
from __future__ import annotations

import asyncio
import json

import aiohttp

from ..bus import Bus
from ..models import Book, D, Level, now_ms
from ..symbols import binance_symbol
from .base import WSCollector

SUBSCRIBE_CHUNK = 50
SUBSCRIBE_GAP_SEC = 0.3


class BinanceCollector(WSCollector):
    name = "binance"
    url = "wss://stream.binance.com:9443/stream"

    def __init__(self, bus: Bus, coins: list[str]) -> None:
        super().__init__(bus)
        self._stream_to_base = {
            f"{binance_symbol(c).lower()}@depth20@100ms": c for c in coins
        }

    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        params = list(self._stream_to_base)
        for i in range(0, len(params), SUBSCRIBE_CHUNK):
            await ws.send_json(
                {"method": "SUBSCRIBE", "params": params[i : i + SUBSCRIBE_CHUNK], "id": i // SUBSCRIBE_CHUNK + 1}
            )
            if i + SUBSCRIBE_CHUNK < len(params):
                await asyncio.sleep(SUBSCRIBE_GAP_SEC)

    async def handle(self, raw: str | bytes) -> None:
        d = json.loads(raw)
        stream = d.get("stream")  # 구독 ack({"result":null,"id":N})는 stream 없음 → 무시
        base = self._stream_to_base.get(stream)
        if base is None:
            return
        data = d.get("data") or {}
        bids = tuple(Level(D(p), D(q)) for p, q in data.get("bids", []))
        asks = tuple(Level(D(p), D(q)) for p, q in data.get("asks", []))
        if not bids and not asks:
            return
        self.bus.publish("book", Book(self.name, base, "USDT", bids, asks, None, now_ms()))
