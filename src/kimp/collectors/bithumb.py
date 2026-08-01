"""빗썸 WebSocket 수집기.

빗썸 v1 WS API(wss://ws-api.bithumb.com/websocket/v1)는 업비트와 동일한 프로토콜·
메시지 체계를 사용하므로 업비트 수집기를 상속한다.
VERIFY(P2): 실수신 데이터로 필드 완전 일치 확인 — 불일치 필드는 handle에서 로그로 드러난다.
"""
from __future__ import annotations

from ..bus import Bus
from .upbit import UpbitCollector


class BithumbCollector(UpbitCollector):
    name = "bithumb"
    url = "wss://ws-api.bithumb.com/websocket/v1"

    def __init__(self, bus: Bus, coins: list[str]) -> None:
        super().__init__(bus, coins)
        # 로거·헬스 이름이 상속 시점의 클래스 속성을 따르도록 재초기화
        import logging

        self.log = logging.getLogger(f"collector.{self.name}")
