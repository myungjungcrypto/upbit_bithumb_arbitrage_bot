"""주문 어댑터 공통 — 정규화된 주문 결과, 이중 실거래 잠금, client order ID 규약."""
from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass, field
from decimal import Decimal

import aiohttp


def live_allowed() -> bool:
    """환경 잠금 (§4.1 방어선 6 스택의 1단) — .env의 LIVE_TRADING_ALLOWED=1 없이는 어떤 주문도 불가."""
    return os.environ.get("LIVE_TRADING_ALLOWED", "") == "1"


class LiveLockError(RuntimeError):
    """실거래 잠금이 풀리지 않은 상태에서 주문 시도 — 상위 계층 버그의 마지막 방어선."""


class OrderError(RuntimeError):
    """거래소가 주문을 거부했거나 응답이 주문 실패를 명시함 (재시도 판단은 저널 몫)."""


_CLIENT_ID_RE = re.compile(r"[^a-zA-Z0-9]")


def make_client_id(prefix: str = "kimp") -> str:
    """멱등키 — 영숫자만, 32자 이내 (OKX clOrdId 제약이 가장 엄격해 그 기준으로 통일)."""
    cid = _CLIENT_ID_RE.sub("", prefix) + uuid.uuid4().hex
    return cid[:32]


@dataclass(slots=True)
class OrderResult:
    """거래소별 응답을 정규화한 주문 상태 — 저널이 이것만 보고 다음 레그를 결정한다."""
    exchange: str
    order_id: str                 # 거래소 주문 ID ("" = 미확인)
    client_id: str
    status: str                   # filled | partial | canceled | open | unknown
    filled_qty: Decimal = Decimal(0)
    avg_price: Decimal | None = None
    fee: Decimal = Decimal(0)     # 지불 수수료 (fee_currency 단위, 양수)
    fee_currency: str = ""
    raw: dict = field(default_factory=dict)  # 원 응답 — 저널 감사 보존용

    @property
    def done(self) -> bool:
        """IOC 관점의 종결 여부 — open/unknown만 미종결."""
        return self.status in ("filled", "partial", "canceled")


class OrderAdapter:
    """하위 클래스가 구현: place_ioc / get_order (+심볼 규칙). 세션은 호출자가 관리."""

    exchange = "base"

    def __init__(self, allow_live: bool = False) -> None:
        # 이중 잠금: 코드가 allow_live=True를 줘도 환경 잠금 없이는 열리지 않는다
        self.allow_live = bool(allow_live) and live_allowed()

    def _guard(self) -> None:
        if not self.allow_live:
            raise LiveLockError(
                f"{self.exchange} 실주문 잠김 — LIVE_TRADING_ALLOWED=1 환경 잠금과 allow_live=True가 모두 필요"
            )

    async def place_ioc(
        self, sess: aiohttp.ClientSession, side: str, base: str, quote: str,
        price: Decimal, qty: Decimal, client_id: str,
    ) -> OrderResult:
        raise NotImplementedError

    async def get_order(
        self, sess: aiohttp.ClientSession, base: str, quote: str,
        order_id: str = "", client_id: str = "",
    ) -> OrderResult:
        raise NotImplementedError
