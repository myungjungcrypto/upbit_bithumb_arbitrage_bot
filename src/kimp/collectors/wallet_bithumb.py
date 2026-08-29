"""빗썸 입출금 상태 폴러 (T4 게이트 ①).

두 모드:
- 인증 모드 (BITHUMB_API_KEY/SECRET 설정 시): 신 API GET /v1/status/wallet —
  업비트 호환 스키마(currency, wallet_state, net_type). 신규 상장 커버리지 갭 해소 +
  네트워크 정보 확보(verify_universe의 빗썸 레그 검사에 사용). 실패 시 자동으로 public 폴백.
- public 모드: 구 assetsstatus/ALL (무인증) — 2026-08 실측 정상 동작, 단 신규 상장 일부 미포함.

VERIFY(P2): 빗썸 v1 JWT 페이로드(access_key/nonce/timestamp)와 응답 필드 실측 확인.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid

import aiohttp

from ..bus import Bus
from .wallet_base import WalletStatusCollector
from .wallet_upbit import parse_wallet_status

PUBLIC_URL = "https://api.bithumb.com/public/assetsstatus/ALL"
V1_URL = "https://api.bithumb.com/v1/status/wallet"


def make_bithumb_jwt(access_key: str, secret_key: str) -> str:
    """빗썸 2.0 인증 JWT (HS256) — 업비트와 동일 구조 + timestamp 필드."""
    def b64(b: bytes) -> bytes:
        return base64.urlsafe_b64encode(b).rstrip(b"=")

    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = b64(json.dumps({
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
    }).encode())
    signing = header + b"." + payload
    sig = b64(hmac.new(secret_key.encode(), signing, hashlib.sha256).digest())
    return (signing + b"." + sig).decode()


def parse_assetsstatus(data: dict) -> list[tuple[str, bool, bool]]:
    """구 public API 응답 → [(코인, 입금가능, 출금가능)] (순수 함수, 테스트 대상)."""
    out: list[tuple[str, bool, bool]] = []
    body = data.get("data")
    if not isinstance(body, dict):
        return out
    for coin, st in body.items():
        if not isinstance(st, dict):
            continue
        try:
            dep = int(st.get("deposit_status", 0)) == 1
            wd = int(st.get("withdrawal_status", 0)) == 1
        except (TypeError, ValueError):
            continue
        out.append((coin.upper(), dep, wd))
    return out


class BithumbWalletStatusCollector(WalletStatusCollector):
    exchange = "bithumb"

    def __init__(self, bus: Bus, poll_sec: float = 60.0, api_key: str = "", api_secret: str = "") -> None:
        super().__init__(bus, poll_sec)
        self.api_key = api_key
        self.api_secret = api_secret
        self._authed_ok = bool(api_key and api_secret)  # 실패 시 public 폴백으로 강등

    async def fetch(self, sess: aiohttp.ClientSession) -> list[tuple[str, bool, bool]]:
        if self._authed_ok:
            try:
                token = make_bithumb_jwt(self.api_key, self.api_secret)
                async with sess.get(
                    V1_URL, headers={"Authorization": f"Bearer {token}"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    resp.raise_for_status()
                    parsed = parse_wallet_status(await resp.json(content_type=None))
                    if parsed:
                        return parsed
                    raise ValueError("empty v1 wallet response")
            except Exception as e:
                self.log.warning("bithumb v1 wallet 실패 (%r) — public API로 폴백", e)
                self._authed_ok = False
        async with sess.get(PUBLIC_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            return parse_assetsstatus(await resp.json(content_type=None))
