"""출금 Live 백엔드 (M3ⓓ) — 게이트웨이(§4.1 방어선 4·5)가 allowlist·한도·승인을 통과시킨 요청만 도달한다.

  - 키: *_WITHDRAW_* (출금 전용 — 조회·거래 키와 분리, §4.1 키 3계층). 이중 잠금 뒤에 있다
  - from_ex로 라우팅: OKX / 업비트 / 빗썸. 각 거래소 출금 API의 파라미터 차이는 여기서 흡수
  - 네트워크·메모·거래소별 추가 파라미터(트래블룰 등)는 게이트웨이 allowlist 항목에서 온다 —
    코드 수정 없이 V1(빗썸 트래블룰 필드) 실측 결과를 config로 반영 가능
  - status(): 출금 진행 상태를 정규화 (pending / sent / done / failed / review) — 오케스트레이터가
    WITHDRAW_SENT 판정·STUCK 타임아웃(T6)에 사용
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal

import aiohttp

from ..collectors.wallet_binance import sign_query
from ..collectors.wallet_bithumb import make_bithumb_jwt
from ..collectors.wallet_okx import okx_timestamp, sign_okx
from ..collectors.wallet_upbit import make_jwt
from ..config import Config
from .base import LiveLockError, OrderError, live_allowed

log = logging.getLogger("gateway.live")

OKX_BASE = "https://www.okx.com"
UPBIT_BASE = "https://api.upbit.com"
BITHUMB_BASE = "https://api.bithumb.com"

# OKX withdrawal-history state → 정규화
_OKX_WD = {
    "-3": "pending", "-2": "failed", "-1": "failed", "0": "pending", "1": "sent", "2": "done",
    "7": "pending", "10": "pending", "15": "pending", "16": "pending", "17": "pending",
    "4": "review", "5": "review", "6": "review", "8": "review", "9": "review", "12": "review",
}
_UPBIT_WD = {"WAITING": "pending", "PROCESSING": "sent", "DONE": "done",
             "FAILED": "failed", "CANCELLED": "failed", "REJECTED": "failed"}
# 바이낸스 withdraw/history status: 0 Email Sent, 1 Cancelled, 2 Awaiting Approval, 3 Rejected, 4 Processing, 5 Failure, 6 Completed
_BINANCE_WD = {"0": "pending", "1": "failed", "2": "pending", "3": "failed", "4": "sent", "5": "failed", "6": "done"}
BINANCE_BASE = "https://api.binance.com"


def norm_binance_wd_state(state) -> str:
    return _BINANCE_WD.get(str(state), "pending")


def norm_okx_wd_state(state) -> str:
    return _OKX_WD.get(str(state), "pending")


def norm_upbit_wd_state(state) -> str:
    return _UPBIT_WD.get(str(state).upper(), "pending")


def okx_withdraw_body(coin: str, amount: Decimal, address: str, network: str, memo: str = "",
                      fee: Decimal | None = None) -> dict:
    """OKX POST /api/v5/asset/withdrawal — 메모 코인은 toAddr을 'addr:memo'로, chain은 OKX 표기('USDT-TRC20')."""
    body = {"ccy": coin, "amt": str(amount), "dest": "4", "toAddr": f"{address}:{memo}" if memo else address,
            "chain": network}
    if fee is not None:
        body["fee"] = str(fee)
    return body


def upbit_withdraw_params(coin: str, amount: Decimal, address: str, network: str, memo: str = "",
                          extra: dict | None = None) -> dict:
    """업비트/빗썸 POST /v1/withdraws/coin — extra로 거래소별 트래블룰 필드 주입 (allowlist 항목 출처)."""
    p = {"currency": coin, "net_type": network, "amount": str(amount), "address": address,
         "transaction_type": "default"}
    if memo:
        p["secondary_address"] = memo
    if extra:
        p.update({str(k): str(v) for k, v in extra.items()})
    return p


class LiveWithdrawBackend:
    def __init__(self, cfg: Config, allow_live: bool = False) -> None:
        self.cfg = cfg
        self.allow_live = bool(allow_live) and live_allowed()

    def _guard(self) -> None:
        if not self.allow_live:
            raise LiveLockError("출금 백엔드 잠김 — LIVE_TRADING_ALLOWED=1 ∧ allow_live 필요")

    # ---------- 발사 ----------

    async def withdraw(self, coin: str, from_ex: str, to_ex: str, amount: Decimal, address: str,
                       network: str = "", memo: str = "", extra: dict | None = None) -> str:
        self._guard()
        if not network:
            raise OrderError(f"allowlist 항목에 network 없음 — {coin} {from_ex}→{to_ex}")
        async with aiohttp.ClientSession(trust_env=True, headers={"Accept": "application/json"}) as sess:
            if from_ex == "okx":
                return await self._okx(sess, coin, amount, address, network, memo)
            if from_ex == "binance":
                return await self._binance(sess, coin, amount, address, network, memo)
            if from_ex in ("upbit", "bithumb"):
                return await self._krw(sess, from_ex, coin, amount, address, network, memo, extra)
        raise OrderError(f"출금 백엔드 미지원 거래소: {from_ex}")

    async def _okx(self, sess, coin, amount, address, network, memo) -> str:
        ak, sk, pp = self.cfg.okx_withdraw_keys
        if not (ak and sk and pp):
            raise OrderError("OKX 출금 키 미설정 (OKX_WITHDRAW_*)")

        def hdr(method: str, path: str, body: str = "") -> dict:
            ts = okx_timestamp()
            return {"OK-ACCESS-KEY": ak, "OK-ACCESS-SIGN": sign_okx(sk, ts, method, path, body),
                    "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": pp, "Content-Type": "application/json"}

        # 수수료: OKX는 체인별 minFee를 fee로 명시 — currencies에서 조회 (출금 정지 체인도 여기서 걸러짐)
        path = f"/api/v5/asset/currencies?ccy={coin}"
        async with sess.get(f"{OKX_BASE}{path}", headers=hdr("GET", path), timeout=aiohttp.ClientTimeout(total=10)) as r:
            cur = await r.json(content_type=None)
        fee = None
        for row in (cur or {}).get("data") or []:
            if row.get("chain") == network:
                if not row.get("canWd", True):
                    raise OrderError(f"OKX {coin} {network} 출금 정지 중")
                fee = Decimal(str(row.get("minFee") or 0))
                break
        if fee is None:
            raise OrderError(f"OKX 체인 표기 불일치: {coin} {network} (allowlist network는 OKX 표기여야 함)")
        path = "/api/v5/asset/withdrawal"
        body = json.dumps(okx_withdraw_body(coin, amount, address, network, memo, fee))
        async with sess.post(f"{OKX_BASE}{path}", data=body, headers=hdr("POST", path, body),
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
        rows = (data or {}).get("data") or []
        if data.get("code") != "0" or not rows:
            raise OrderError(f"OKX 출금 거부: {data.get('msg')} (code={data.get('code')})")
        return str(rows[0].get("wdId"))

    async def _binance(self, sess, coin, amount, address, network, memo) -> str:
        """POST /sapi/v1/capital/withdraw/apply — 마스터 계정 출금 전용 키. 주소 화이트리스트 ON이면 미등록 주소는
        거래소가 거부한다(방어선 1). network는 바낸 코드("ETH"/"BSC"…). 서브계좌 2단 출금(이체→출금)은 M4."""
        ak, sk = self.cfg.binance_withdraw_keys
        if not (ak and sk):
            raise OrderError("바이낸스 출금 키 미설정 (BINANCE_WITHDRAW_*)")
        params = {"coin": coin, "network": network, "address": address, "amount": str(amount),
                  "walletType": 0, "timestamp": int(__import__("time").time() * 1000), "recvWindow": 10000}
        if memo:
            params["addressTag"] = memo
        qs = sign_query(sk, params)
        async with sess.post(f"{BINANCE_BASE}/sapi/v1/capital/withdraw/apply?{qs}", headers={"X-MBX-APIKEY": ak},
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
            if r.status >= 400 or not isinstance(data, dict) or "id" not in data:
                raise OrderError(f"바이낸스 출금 거부: {data}")
        return str(data["id"])

    async def _krw(self, sess, ex, coin, amount, address, network, memo, extra) -> str:
        if ex == "upbit":
            ak, sk = self.cfg.upbit_withdraw_keys
            base, jwt = UPBIT_BASE, make_jwt
        else:
            ak, sk = self.cfg.bithumb_withdraw_keys
            base, jwt = BITHUMB_BASE, make_bithumb_jwt
        if not (ak and sk):
            raise OrderError(f"{ex} 출금 키 미설정 ({ex.upper()}_WITHDRAW_*)")
        params = upbit_withdraw_params(coin, amount, address, network, memo, extra)
        async with sess.post(f"{base}/v1/withdraws/coin", json=params,
                             headers={"Authorization": f"Bearer {jwt(ak, sk, params)}"},
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
            if r.status >= 400 or not isinstance(data, dict) or "uuid" not in data:
                raise OrderError(f"{ex} 출금 거부: {data}")
        return str(data["uuid"])

    # ---------- 상태 ----------

    async def status(self, from_ex: str, coin: str, wd_id: str) -> tuple[str, str]:
        """(정규화 상태, txid) — 조회 실패 시 ('pending', '') (재조회로 수렴, 판단은 오케스트레이터 타임아웃)."""
        try:
            async with aiohttp.ClientSession(trust_env=True, headers={"Accept": "application/json"}) as sess:
                if from_ex == "okx":
                    ak, sk, pp = self.cfg.okx_withdraw_keys
                    path = f"/api/v5/asset/withdrawal-history?ccy={coin}&wdId={wd_id}"
                    ts = okx_timestamp()
                    h = {"OK-ACCESS-KEY": ak, "OK-ACCESS-SIGN": sign_okx(sk, ts, "GET", path),
                         "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": pp}
                    async with sess.get(f"{OKX_BASE}{path}", headers=h, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        data = await r.json(content_type=None)
                    rows = (data or {}).get("data") or []
                    if not rows:
                        return "pending", ""
                    return norm_okx_wd_state(rows[0].get("state")), str(rows[0].get("txId") or "")
                if from_ex == "binance":
                    ak, sk = self.cfg.binance_withdraw_keys
                    qs = sign_query(sk, {"coin": coin, "timestamp": int(__import__("time").time() * 1000), "recvWindow": 10000})
                    async with sess.get(f"{BINANCE_BASE}/sapi/v1/capital/withdraw/history?{qs}", headers={"X-MBX-APIKEY": ak},
                                        timeout=aiohttp.ClientTimeout(total=10)) as r:
                        data = await r.json(content_type=None)
                    for row in data if isinstance(data, list) else []:
                        if str(row.get("id")) == str(wd_id):
                            return norm_binance_wd_state(row.get("status")), str(row.get("txId") or "")
                    return "pending", ""
                ak, sk = self.cfg.upbit_withdraw_keys if from_ex == "upbit" else self.cfg.bithumb_withdraw_keys
                base = UPBIT_BASE if from_ex == "upbit" else BITHUMB_BASE
                jwt = make_jwt if from_ex == "upbit" else make_bithumb_jwt
                params = {"uuid": wd_id, "currency": coin}
                qs = "&".join(f"{k}={v}" for k, v in params.items())
                async with sess.get(f"{base}/v1/withdraw?{qs}", headers={"Authorization": f"Bearer {jwt(ak, sk, params)}"},
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json(content_type=None)
                if not isinstance(data, dict) or "state" not in data:
                    return "pending", ""
                return norm_upbit_wd_state(data.get("state")), str(data.get("txid") or "")
        except Exception as e:
            log.warning("withdraw status 조회 실패 (%s %s %s): %r", from_ex, coin, wd_id, e)
            return "pending", ""
