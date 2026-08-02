"""업비트 입출금 현황 수집기 — GET /v1/status/wallet (인증: 조회 전용 키, JWT HS256).

wallet_state 매핑:
  working       → 입금 O / 출금 O
  deposit_only  → 입금 O / 출금 X
  withdraw_only → 입금 X / 출금 O
  paused, unsupported, 그 외 → 입금 X / 출금 X (보수적)

키는 환경변수 UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY — 조회 권한만 부여된 키를 사용할 것 (PLAN §4.1 키 3계층).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import uuid

import aiohttp

from ..bus import Bus
from .wallet_base import WalletStatusCollector

URL = "https://api.upbit.com/v1/status/wallet"


def _b64url(data: bytes) -> bytes:
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def make_jwt(access_key: str, secret_key: str) -> str:
    """업비트 인증용 JWT (HS256, 파라미터 없는 요청). 외부 의존성 없이 stdlib로 생성."""
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url(
        json.dumps({"access_key": access_key, "nonce": str(uuid.uuid4())}).encode()
    )
    signing_input = header + b"." + payload
    sig = _b64url(hmac.new(secret_key.encode(), signing_input, hashlib.sha256).digest())
    return (signing_input + b"." + sig).decode()


def parse_wallet_status(data: list) -> list[tuple[str, bool, bool]]:
    """응답 → [(코인, 입금가능, 출금가능)]. 미지의 상태는 보수적으로 정지 취급 (순수 함수, 테스트 대상)."""
    out: list[tuple[str, bool, bool]] = []
    if not isinstance(data, list):
        return out
    for row in data:
        if not isinstance(row, dict):
            continue
        coin = row.get("currency")
        state = row.get("wallet_state")
        if not coin or not isinstance(state, str):
            continue
        dep = state in ("working", "deposit_only")
        wd = state in ("working", "withdraw_only")
        out.append((coin.upper(), dep, wd))
    return out


class UpbitWalletStatusCollector(WalletStatusCollector):
    exchange = "upbit"

    def __init__(self, bus: Bus, access_key: str, secret_key: str, poll_sec: float = 60.0) -> None:
        super().__init__(bus, poll_sec)
        self.access_key = access_key
        self.secret_key = secret_key

    async def fetch(self, sess: aiohttp.ClientSession) -> list[tuple[str, bool, bool]]:
        token = make_jwt(self.access_key, self.secret_key)
        async with sess.get(
            URL,
            headers={"Authorization": f"Bearer {token}"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            return parse_wallet_status(await resp.json(content_type=None))
