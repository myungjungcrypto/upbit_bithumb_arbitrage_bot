"""OKX 자산별 입출금 상태 수집기 — GET /api/v5/asset/currencies (읽기 전용 키).

M1 해외 다변화: OKX가 P3 라이브 1순위(소액 격리 운용)이므로 지갑 게이트도 우선 확보.
응답은 (ccy, chain) 행 단위 — 코인 판정은 "어느 한 체인이라도 열려 있으면 가능"으로 OR 집계.

키는 환경변수 OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE — 조회 권한만 (PLAN §4.1).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timezone

import aiohttp

from ..bus import Bus
from .wallet_base import WalletStatusCollector

BASE = "https://www.okx.com"
PATH = "/api/v5/asset/currencies"


def okx_timestamp(now: datetime | None = None) -> str:
    """OKX 서명용 ISO8601 타임스탬프 (밀리초 + 'Z')."""
    dt = now or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def sign_okx(secret: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """OKX 서명: Base64(HMAC-SHA256(ts + METHOD + path + body))."""
    msg = f"{timestamp}{method.upper()}{request_path}{body}"
    return base64.b64encode(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()).decode()


def parse_currencies(data: dict) -> list[tuple[str, bool, bool]]:
    """응답 → [(코인, 입금가능, 출금가능)] — 체인 중 하나라도 열려 있으면 가능 (순수 함수)."""
    agg: dict[str, tuple[bool, bool]] = {}
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ccy = row.get("ccy")
        if not ccy:
            continue
        coin = str(ccy).upper()
        dep0, wd0 = agg.get(coin, (False, False))
        agg[coin] = (dep0 or bool(row.get("canDep")), wd0 or bool(row.get("canWd")))
    return [(c, d, w) for c, (d, w) in agg.items()]


class OkxWalletStatusCollector(WalletStatusCollector):
    exchange = "okx"

    def __init__(
        self, bus: Bus, api_key: str, api_secret: str, passphrase: str, poll_sec: float = 60.0
    ) -> None:
        super().__init__(bus, poll_sec)
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase

    async def fetch(self, sess: aiohttp.ClientSession) -> list[tuple[str, bool, bool]]:
        ts = okx_timestamp()
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign_okx(self.api_secret, ts, "GET", PATH),
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
        }
        async with sess.get(
            f"{BASE}{PATH}", headers=headers, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        if isinstance(data, dict) and data.get("code") not in ("0", 0, None):
            raise RuntimeError(f"okx currencies error code={data.get('code')} msg={data.get('msg')}")
        return parse_currencies(data)
