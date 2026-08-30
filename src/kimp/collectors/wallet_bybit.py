"""바이비트 자산별 입출금 상태 수집기 — GET /v5/asset/coin/query-info (읽기 전용 키).

M1 해외 다변화: 바이비트 레그의 V8 사각 해소. 응답의 chains[] 배열에서
chainDeposit/chainWithdraw("0"/"1" 문자열)를 "어느 한 체인이라도 열려 있으면 가능"으로 OR 집계.

키는 환경변수 BYBIT_API_KEY / BYBIT_API_SECRET — 조회 권한만 (PLAN §4.1).
"""
from __future__ import annotations

import hashlib
import hmac
import time

import aiohttp

from ..bus import Bus
from .wallet_base import WalletStatusCollector

URL = "https://api.bybit.com/v5/asset/coin/query-info"
RECV_WINDOW = "10000"


def sign_bybit(secret: str, timestamp: str, api_key: str, recv_window: str, query: str) -> str:
    """바이비트 v5 서명: hex(HMAC-SHA256(ts + key + recvWindow + queryString))."""
    msg = f"{timestamp}{api_key}{recv_window}{query}"
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()


def parse_coin_info(data: dict) -> list[tuple[str, bool, bool]]:
    """응답 → [(코인, 입금가능, 출금가능)] — 체인 중 하나라도 열려 있으면 가능 (순수 함수)."""
    out: list[tuple[str, bool, bool]] = []
    rows = (((data or {}).get("result") or {}).get("rows")) if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    for row in rows:
        if not isinstance(row, dict):
            continue
        coin = row.get("coin")
        chains = row.get("chains")
        if not coin or not isinstance(chains, list) or not chains:
            continue
        dep = any(str(c.get("chainDeposit")) == "1" for c in chains if isinstance(c, dict))
        wd = any(str(c.get("chainWithdraw")) == "1" for c in chains if isinstance(c, dict))
        out.append((str(coin).upper(), dep, wd))
    return out


class BybitWalletStatusCollector(WalletStatusCollector):
    exchange = "bybit"

    def __init__(self, bus: Bus, api_key: str, api_secret: str, poll_sec: float = 60.0) -> None:
        super().__init__(bus, poll_sec)
        self.api_key = api_key
        self.api_secret = api_secret

    async def fetch(self, sess: aiohttp.ClientSession) -> list[tuple[str, bool, bool]]:
        ts = str(int(time.time() * 1000))
        query = ""  # 전 코인 조회 — 쿼리스트링 없음
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW,
            "X-BAPI-SIGN": sign_bybit(self.api_secret, ts, self.api_key, RECV_WINDOW, query),
        }
        async with sess.get(URL, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        if isinstance(data, dict) and data.get("retCode") not in (0, "0", None):
            raise RuntimeError(f"bybit coin-info error retCode={data.get('retCode')} msg={data.get('retMsg')}")
        return parse_coin_info(data)
