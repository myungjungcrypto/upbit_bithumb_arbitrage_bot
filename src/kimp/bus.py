"""인프로세스 pub/sub. 수집기(생산자)는 절대 블록되지 않는다 — 느린 소비자의 큐가 차면
가장 오래된 항목을 버리고 카운트한다 (핫패스 보호)."""
from __future__ import annotations

import asyncio
from collections import Counter


class Bus:
    def __init__(self) -> None:
        self._subs: dict[str, list[asyncio.Queue]] = {}
        self.dropped: Counter[str] = Counter()

    def subscribe(self, topic: str, maxsize: int = 5000) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._subs.setdefault(topic, []).append(q)
        return q

    def publish(self, topic: str, item) -> None:
        for q in self._subs.get(topic, ()):
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                q.put_nowait(item)
                self.dropped[topic] += 1
