"""바이낸스 자산별 입출금 상태 수집기 — GET /sapi/v1/capital/config/getall (읽기 전용 키).

V8 사각 해소: IN 방향의 출발지(바이낸스 출금)·OUT 방향의 도착지(바이낸스 입금) 게이트.
코인 단위 판정은 "어느 한 네트워크라도 가능하면 가능"으로 근사 (네트워크별 정밀 매핑은 V4/T3).

키는 환경변수 BINANCE_API_KEY / BINANCE_API_SECRET — 조회 권한만 부여된 키를 사용할 것 (PLAN §4.1).
"""
from __future__ import annotations

import hashlib
import hmac
import time
import urllib.parse

import aiohttp

from ..bus import Bus
from .wallet_base import WalletStatusCollector

URL = "https://api.binance.com/sapi/v1/capital/config/getall"


def sign_query(secret: str, params: dict) -> str:
    """바이낸스 서명 쿼리스트링 생성 (HMAC-SHA256, stdlib만 사용)."""
    query = urllib.parse.urlencode(params)
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={sig}"


def parse_capital_config(data: list) -> list[tuple[str, bool, bool]]:
    """응답 → [(코인, 입금가능, 출금가능)] — 네트워크 중 하나라도 열려 있으면 가능 (순수 함수)."""
    out: list[tuple[str, bool, bool]] = []
    if not isinstance(data, list):
        return out
    for row in data:
        if not isinstance(row, dict):
            continue
        coin = row.get("coin")
        nets = row.get("networkList")
        if not coin or not isinstance(nets, list) or not nets:
            continue
        dep = any(bool(n.get("depositEnable")) for n in nets if isinstance(n, dict))
        wd = any(bool(n.get("withdrawEnable")) for n in nets if isinstance(n, dict))
        out.append((str(coin).upper(), dep, wd))
    return out


class BinanceWalletStatusCollector(WalletStatusCollector):
    exchange = "binance"

    def __init__(self, bus: Bus, api_key: str, api_secret: str, poll_sec: float = 60.0) -> None:
        super().__init__(bus, poll_sec)
        self.api_key = api_key
        self.api_secret = api_secret

    async def fetch(self, sess: aiohttp.ClientSession) -> list[tuple[str, bool, bool]]:
        qs = sign_query(self.api_secret, {"timestamp": int(time.time() * 1000), "recvWindow": 10000})
        async with sess.get(
            f"{URL}?{qs}",
            headers={"X-MBX-APIKEY": self.api_key},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            resp.raise_for_status()
            return parse_capital_config(await resp.json(content_type=None))
