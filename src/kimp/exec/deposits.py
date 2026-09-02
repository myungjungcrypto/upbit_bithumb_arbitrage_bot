"""입금 감지 (M3ⓓ) — 도착 즉시 매도 발사(§1.4 속도 원칙)의 감지측.

원리: 대기 시작 시점의 입금 이력 ID를 스냅샷하고, 그 이후 나타난 **새 ID** 중 '입금 완료' 상태이고
수량이 기대치 이상인 첫 건을 도착으로 본다. 타임스탬프 포맷 차이에 의존하지 않는다.
조회 키는 읽기 전용 키로 충분 (입금 조회 권한) — 프라이빗 WS 전환은 M5.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

import aiohttp

import time

from ..collectors.wallet_binance import sign_query
from ..collectors.wallet_bithumb import make_bithumb_jwt
from ..collectors.wallet_okx import okx_timestamp, sign_okx
from ..collectors.wallet_upbit import make_jwt
from ..config import Config

log = logging.getLogger("deposits")

# 거래소별 '트레이딩 가능한 입금 완료' 상태 (OKX 1=credited, 2=successful / 국내 ACCEPTED)
# 바이낸스 deposit/hisrec status: 0 pending, 6 credited(출금 불가, 거래 가능), 1 success
ACCEPTED = {"okx": {"1", "2"}, "upbit": {"ACCEPTED"}, "bithumb": {"ACCEPTED"}, "binance": {"1", "6"}}


def deposit_id(row: dict) -> str:
    return str(row.get("depId") or row.get("uuid") or row.get("txId") or row.get("txid") or "")


def deposit_amount(row: dict) -> Decimal:
    v = row.get("amt", row.get("amount", 0))
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal(0)


def match_new_deposit(before_ids: set[str], rows: list[dict], exchange: str, coin: str, min_amount: Decimal) -> dict | None:
    """스냅샷 이후의 새 입금 중 완료 상태 ∧ 수량 ≥ min_amount ∧ 같은 코인 → 정규화 dict (순수 함수)."""
    ok_states = ACCEPTED.get(exchange, set())
    for r in rows:
        if not isinstance(r, dict):
            continue
        rid = deposit_id(r)
        if not rid or rid in before_ids:
            continue
        ccy = str(r.get("ccy") or r.get("currency") or coin).upper()
        if ccy != coin.upper():
            continue
        st = str(r.get("state", r.get("status", "")))  # OKX/국내=state, 바이낸스=status
        if st not in ok_states and st.upper() not in ok_states:
            continue
        amt = deposit_amount(r)
        if amt >= min_amount:
            return {"id": rid, "amount": amt, "txid": str(r.get("txId") or r.get("txid") or ""), "raw": r}
    return None


class DepositWatcher:
    def __init__(self, cfg: Config, poll_sec: float = 3.0) -> None:
        self.cfg = cfg
        self.poll_sec = poll_sec

    async def fetch(self, sess: aiohttp.ClientSession, exchange: str, coin: str) -> list[dict]:
        """최근 입금 이력 (거래소별 인증 GET). 실패 시 예외 — wait_for가 재시도."""
        if exchange == "okx":
            path = f"/api/v5/asset/deposit-history?ccy={coin}"
            ts = okx_timestamp()
            h = {"OK-ACCESS-KEY": self.cfg.okx_api_key,
                 "OK-ACCESS-SIGN": sign_okx(self.cfg.okx_api_secret, ts, "GET", path),
                 "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": self.cfg.okx_api_passphrase}
            async with sess.get(f"https://www.okx.com{path}", headers=h, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json(content_type=None)
            return list((data or {}).get("data") or [])
        if exchange == "binance":
            qs = sign_query(self.cfg.binance_api_secret, {"coin": coin, "timestamp": int(time.time() * 1000), "recvWindow": 10000})
            async with sess.get(f"https://api.binance.com/sapi/v1/capital/deposit/hisrec?{qs}",
                                headers={"X-MBX-APIKEY": self.cfg.binance_api_key}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json(content_type=None)
            return list(data) if isinstance(data, list) else []
        if exchange == "upbit":
            ak, sk, base, jwt = self.cfg.upbit_access_key, self.cfg.upbit_secret_key, "https://api.upbit.com", make_jwt
        elif exchange == "bithumb":
            ak, sk, base, jwt = self.cfg.bithumb_api_key, self.cfg.bithumb_api_secret, "https://api.bithumb.com", make_bithumb_jwt
        else:
            raise ValueError(f"입금 감지 미지원 거래소: {exchange}")
        params = {"currency": coin, "limit": "20"}
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        async with sess.get(f"{base}/v1/deposits?{qs}", headers={"Authorization": f"Bearer {jwt(ak, sk, params)}"},
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
        return list(data) if isinstance(data, list) else []

    async def wait_for(self, exchange: str, coin: str, expected: Decimal, timeout_sec: float,
                       stop: asyncio.Event | None = None, min_ratio: Decimal = Decimal("0.9")) -> dict | None:
        """새 입금 도착까지 폴링 — 기대 수량의 min_ratio 이상이면 도착. 반환 None = 타임아웃/중단
        (STUCK_DEPOSIT 판단은 호출자). 실제 크레딧 수량은 반환 dict의 amount."""
        min_amount = expected * min_ratio
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_sec
        async with aiohttp.ClientSession(trust_env=True, headers={"Accept": "application/json"}) as sess:
            before: set[str] | None = None
            while loop.time() < deadline and not (stop and stop.is_set()):
                try:
                    rows = await self.fetch(sess, exchange, coin)
                    if before is None:
                        before = {deposit_id(r) for r in rows if isinstance(r, dict)}
                    else:
                        hit = match_new_deposit(before, rows, exchange, coin, min_amount)
                        if hit is not None:
                            return hit
                except Exception as e:
                    log.warning("deposit poll 실패 (%s %s): %r", exchange, coin, e)
                await asyncio.sleep(self.poll_sec)
        return None
