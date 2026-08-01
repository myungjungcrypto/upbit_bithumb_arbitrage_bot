"""바이낸스 현물 WebSocket 수집기 — depth20@100ms 부분 호가 스냅샷.

diff 스트림 대신 20단 부분 스냅샷을 쓴다: 로컬 북 유지가 필요 없어 P0에 적합하고,
100ms 주기가 탐지 요건(<100ms 푸시 기반)과 일치한다.
NOTE: 부분 스냅샷에는 이벤트 타임스탬프가 없어 ts_exchange=None → 로컬 시각 기준.
"""
from __future__ import annotations

import json

import aiohttp

from ..bus import Bus
from ..models import Book, D, Level, now_ms
from ..symbols import binance_symbol
from .base import WSCollector


class BinanceCollector(WSCollector):
    name = "binance"

    def __init__(self, bus: Bus, coins: list[str]) -> None:
        super().__init__(bus)
        self._stream_to_base = {
            f"{binance_symbol(c).lower()}@depth20@100ms": c for c in coins
        }
        streams = "/".join(self._stream_to_base)
        self.url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    async def handle(self, raw: str | bytes) -> None:
        d = json.loads(raw)
        stream = d.get("stream")
        base = self._stream_to_base.get(stream)
        if base is None:
            return
        data = d.get("data") or {}
        bids = tuple(Level(D(p), D(q)) for p, q in data.get("bids", []))
        asks = tuple(Level(D(p), D(q)) for p, q in data.get("asks", []))
        if not bids and not asks:
            return
        self.bus.publish("book", Book(self.name, base, "USDT", bids, asks, None, now_ms()))
