"""업비트 WebSocket 수집기 — orderbook(15단) + trade, 변동 즉시 푸시.

프로토콜: wss://api.upbit.com/websocket/v1
구독: [{"ticket": ...}, {"type": "orderbook", "codes": [...]}, {"type": "trade", "codes": [...]}]
KRW-USDT 마켓을 항상 포함한다 — 실행 김프의 분모 (PLAN §2).
"""
from __future__ import annotations

import json
import uuid

import aiohttp

from ..bus import Bus
from ..models import Book, D, Level, Trade, now_ms
from ..symbols import parse_dash_code, upbit_code
from .base import WSCollector


class UpbitCollector(WSCollector):
    name = "upbit"
    url = "wss://api.upbit.com/websocket/v1"

    def __init__(self, bus: Bus, coins: list[str]) -> None:
        super().__init__(bus)
        self.codes = [upbit_code(c) for c in coins] + [upbit_code("USDT")]

    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        await ws.send_json(
            [
                {"ticket": f"kimp-{uuid.uuid4().hex[:8]}"},
                {"type": "orderbook", "codes": self.codes},
                {"type": "trade", "codes": self.codes},
                {"format": "DEFAULT"},
            ]
        )

    async def handle(self, raw: str | bytes) -> None:
        d = json.loads(raw)
        t = d.get("type")
        if t == "orderbook":
            base, quote = parse_dash_code(d["code"])
            units = d.get("orderbook_units") or []
            bids = tuple(Level(D(u["bid_price"]), D(u["bid_size"])) for u in units)
            asks = tuple(Level(D(u["ask_price"]), D(u["ask_size"])) for u in units)
            self.bus.publish(
                "book",
                Book(self.name, base, quote, bids, asks, d.get("timestamp"), now_ms()),
            )
        elif t == "trade":
            base, quote = parse_dash_code(d["code"])
            side = "buy" if d.get("ask_bid") == "BID" else "sell"
            self.bus.publish(
                "trade",
                Trade(
                    self.name,
                    base,
                    quote,
                    D(d["trade_price"]),
                    D(d["trade_volume"]),
                    side,
                    d.get("trade_timestamp") or d.get("timestamp"),
                    now_ms(),
                ),
            )
