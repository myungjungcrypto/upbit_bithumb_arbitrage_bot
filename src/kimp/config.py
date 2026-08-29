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

    def withdraw_fee_pct(self, exchange: str, coin: str) -> Decimal:
        """정률 출금 수수료 (운용자 실측 2026-08-28: 빗썸 알트 다수 ~명목의 1%).
        OUT 사이클의 국내 코인 출금에 적용 — 정액 모델만으로는 빗썸 비용을 크게 과소평가."""
        table = self.raw.get("withdraw_fee_pct", {}).get(exchange, {})
        return D(table.get(coin, table.get("default", 0)))

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
    def upbit_access_key(self) -> str:
        return os.environ.get("UPBIT_ACCESS_KEY", "")

    @property
    def upbit_secret_key(self) -> str:
        return os.environ.get("UPBIT_SECRET_KEY", "")

    @property
    def bithumb_api_key(self) -> str:
        return os.environ.get("BITHUMB_API_KEY", "")

    @property
    def bithumb_api_secret(self) -> str:
        return os.environ.get("BITHUMB_API_SECRET", "")

    @property
    def binance_api_key(self) -> str:
        return os.environ.get("BINANCE_API_KEY", "")

    @property
    def binance_api_secret(self) -> str:
        return os.environ.get("BINANCE_API_SECRET", "")

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
