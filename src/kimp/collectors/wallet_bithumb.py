"""빗썸 입출금 상태 폴러 — 무인증 public API (T4 게이트 ①).

VERIFY(실배포): 구 public API 응답 유지 여부 — 2026-08-02 EC2에서 정상 동작 확인됨.
"""
from __future__ import annotations

import aiohttp

from .wallet_base import WalletStatusCollector

URL = "https://api.bithumb.com/public/assetsstatus/ALL"


def parse_assetsstatus(data: dict) -> list[tuple[str, bool, bool]]:
    """응답 → [(코인, 입금가능, 출금가능)]. 형식 이상 항목은 건너뜀 (순수 함수, 테스트 대상)."""
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

    async def fetch(self, sess: aiohttp.ClientSession) -> list[tuple[str, bool, bool]]:
        async with sess.get(URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            return parse_assetsstatus(await resp.json(content_type=None))
