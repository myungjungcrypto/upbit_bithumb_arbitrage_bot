"""바이비트 v5 현물 WebSocket 수집기 — orderbook.50 (snapshot + delta).

프로토콜: wss://stream.bybit.com/v5/public/spot
- snapshot: 전체 교체 / delta: 수량 0이면 레벨 삭제, 아니면 갱신
- 20초마다 {"op":"ping"} 앱레벨 핑 필요
- 재연결 시 북 상태 초기화 (subscribe에서 수행)
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import aiohttp

from ..bus import Bus
from ..models import Book, D, Level, now_ms
from ..symbols import bybit_symbol, parse_concat_symbol
from .base import WSCollector

DEPTH_PUBLISH = 15  # 발행 시 상위 15단으로 절단 (국내 15단과 정합)


class BybitCollector(WSCollector):
    name = "bybit"
    url = "wss://stream.bybit.com/v5/public/spot"
    ping_interval_sec = 20.0

    def __init__(self, bus: Bus, coins: list[str]) -> None:
        super().__init__(bus)
        self.symbols = [bybit_symbol(c) for c in coins]
        self._books: dict[str, dict[str, dict[str, str]]] = {}

    def app_ping(self):
        return {"op": "ping"}

    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._books = {}  # 재연결마다 스냅샷부터 다시 시작
        args = [f"orderbook.50.{s}" for s in self.symbols]
        for i in range(0, len(args), 10):  # v5는 구독 메시지당 args 수 제한
            await ws.send_json({"op": "subscribe", "args": args[i : i + 10]})
            if i + 10 < len(args):
                await asyncio.sleep(0.2)

    async def handle(self, raw: str | bytes) -> None:
        d = json.loads(raw)
        topic = d.get("topic", "")
        if not topic.startswith("orderbook."):
            return  # 구독 ack / pong
        data = d.get("data") or {}
        sym = data.get("s")
        if not sym:
            return
        book = self._books.setdefault(sym, {"b": {}, "a": {}})
        if d.get("type") == "snapshot":
            book["b"] = {p: q for p, q in data.get("b", [])}
            book["a"] = {p: q for p, q in data.get("a", [])}
        else:
            for p, q in data.get("b", []):
                if q == "0":
                    book["b"].pop(p, None)
                else:
                    book["b"][p] = q
            for p, q in data.get("a", []):
                if q == "0":
                    book["a"].pop(p, None)
                else:
                    book["a"][p] = q

        bids = tuple(
            Level(D(p), D(q))
            for p, q in sorted(book["b"].items(), key=lambda kv: Decimal(kv[0]), reverse=True)[
                :DEPTH_PUBLISH
            ]
        )
        asks = tuple(
            Level(D(p), D(q))
            for p, q in sorted(book["a"].items(), key=lambda kv: Decimal(kv[0]))[:DEPTH_PUBLISH]
        )
        if not bids and not asks:
            return
        base, quote = parse_concat_symbol(sym)
        self.bus.publish("book", Book(self.name, base, quote, bids, asks, d.get("ts"), now_ms()))
