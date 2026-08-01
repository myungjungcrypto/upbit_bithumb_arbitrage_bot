"""공용 데이터 모델. 금액·수량은 전부 Decimal (PLAN §1.4 — float 금지)."""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal


def D(x) -> Decimal:
    """float 오염 없이 Decimal 변환 (str 경유)."""
    return Decimal(str(x))


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True, slots=True)
class Level:
    price: Decimal
    size: Decimal


@dataclass(slots=True)
class Book:
    exchange: str
    base: str
    quote: str
    bids: tuple[Level, ...]  # 가격 내림차순
    asks: tuple[Level, ...]  # 가격 오름차순
    ts_exchange: int | None  # 거래소 타임스탬프(ms), 없는 거래소는 None
    ts_local: int            # 로컬 수신 시각(ms) — 레이턴시 분석용 이중 기록

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.exchange, self.base, self.quote)

    @property
    def mid(self) -> Decimal | None:
        if self.bids and self.asks:
            return (self.bids[0].price + self.asks[0].price) / 2
        return None


@dataclass(slots=True)
class Trade:
    exchange: str
    base: str
    quote: str
    price: Decimal
    size: Decimal
    side: str  # "buy" | "sell" (taker 기준)
    ts_exchange: int | None
    ts_local: int


@dataclass(slots=True)
class Fx:
    pair: str        # "USD/KRW"
    rate: Decimal
    source: str
    ts_local: int


@dataclass(slots=True)
class Health:
    component: str
    status: str      # "up" | "down" | "stale" | "error"
    detail: str
    ts_local: int
