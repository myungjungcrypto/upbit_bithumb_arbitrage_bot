"""YAML 설정 로드 + 환경변수 오버레이. 시크릿은 환경변수로만 (저장소 커밋 금지)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import yaml

from .models import D


@dataclass(slots=True)
class Config:
    raw: dict = field(default_factory=dict)

    # 자주 쓰는 값들의 타입 안전 접근자
    @property
    def coins(self) -> list[str]:
        return list(self.raw.get("coins", []))

    @property
    def domestic(self) -> list[str]:
        return list(self.raw.get("domestic", ["upbit", "bithumb"]))

    @property
    def overseas(self) -> list[str]:
        return list(self.raw.get("overseas", ["binance"]))

    @property
    def overseas_ref(self) -> str:
        return self.raw.get("overseas_ref", "binance")

    @property
    def ladder_usd(self) -> list[Decimal]:
        return [D(x) for x in self.raw.get("ladder_usd", [5000])]

    @property
    def book_stale_ms(self) -> int:
        return int(self.raw.get("staleness", {}).get("book_ms", 5000))

    @property
    def fx_stale_ms(self) -> int:
        return int(self.raw.get("staleness", {}).get("fx_ms", 120000))

    @property
    def engine_min_interval_ms(self) -> int:
        return int(self.raw.get("engine", {}).get("min_interval_ms", 200))

    def taker_fee(self, exchange: str) -> Decimal:
        return D(self.raw.get("fees", {}).get("taker", {}).get(exchange, 0.001))

    def withdraw_fee_usd(self, coin: str) -> Decimal:
        table = self.raw.get("withdraw_fee_usd_est", {})
        return D(table.get(coin, table.get("default", 2.0)))

    @property
    def alerts(self) -> dict:
        return self.raw.get("alerts", {})

    @property
    def storage(self) -> dict:
        return self.raw.get("storage", {})

    @property
    def fx(self) -> dict:
        return self.raw.get("fx", {})

    @property
    def telegram_token(self) -> str:
        return os.environ.get("TELEGRAM_BOT_TOKEN", "")

    @property
    def telegram_chat_id(self) -> str:
        return os.environ.get("TELEGRAM_CHAT_ID", "")

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.raw.get("telegram", {}).get("enabled", True)) and bool(self.telegram_token)


def load_config(path: str | Path) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        return Config(raw=yaml.safe_load(f) or {})
