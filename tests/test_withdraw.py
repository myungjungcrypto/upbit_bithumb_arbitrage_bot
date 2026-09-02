"""출금 Live 백엔드·입금 감지 검증 (M3ⓓ) — 파서·본문 빌더·잠금. 네트워크 호출 없음."""
import asyncio
from decimal import Decimal

import pytest

from kimp.config import Config
from kimp.exec.base import LiveLockError
from kimp.exec.deposits import ACCEPTED, deposit_id, match_new_deposit
from kimp.exec.withdraw import (
    LiveWithdrawBackend,
    norm_okx_wd_state,
    norm_upbit_wd_state,
    okx_withdraw_body,
    upbit_withdraw_params,
)


def test_withdraw_state_normalization():
    assert norm_okx_wd_state("2") == "done" and norm_okx_wd_state(1) == "sent"
    assert norm_okx_wd_state("-1") == "failed" and norm_okx_wd_state("4") == "review"
    assert norm_okx_wd_state("999") == "pending"                 # 미지 코드는 보수적으로 대기
    assert norm_upbit_wd_state("DONE") == "done" and norm_upbit_wd_state("processing") == "sent"
    assert norm_upbit_wd_state("REJECTED") == "failed" and norm_upbit_wd_state("?") == "pending"


def test_okx_withdraw_body_memo_and_fee():
    b = okx_withdraw_body("USDT", Decimal("100.5"), "TAddr", "USDT-TRC20", fee=Decimal("1"))
    assert b == {"ccy": "USDT", "amt": "100.5", "dest": "4", "toAddr": "TAddr", "chain": "USDT-TRC20", "fee": "1"}
    assert okx_withdraw_body("XRP", Decimal(5), "rAddr", "XRP-Ripple", memo="12345")["toAddr"] == "rAddr:12345"


def test_upbit_withdraw_params_extra_merge():
    p = upbit_withdraw_params("ONG", Decimal(10), "AXyz", "ONT", memo="", extra={"exchange_name": "okx"})
    assert p["currency"] == "ONG" and p["net_type"] == "ONT" and p["transaction_type"] == "default"
    assert "secondary_address" not in p and p["exchange_name"] == "okx"
    assert upbit_withdraw_params("XRP", Decimal(1), "r", "XRP", memo="77")["secondary_address"] == "77"


def test_match_new_deposit_snapshot_logic():
    before = {"d1", "u1"}
    okx_rows = [
        {"depId": "d1", "ccy": "PROM", "amt": "50", "state": "2"},                  # 스냅샷 이전 → 무시
        {"depId": "d2", "ccy": "PROM", "amt": "3", "state": "2"},                   # 수량 미달
        {"depId": "d3", "ccy": "PROM", "amt": "49.5", "state": "0"},                # 미확정
        {"depId": "d4", "ccy": "ONG", "amt": "49.5", "state": "2"},                 # 다른 코인
        {"depId": "d5", "ccy": "PROM", "amt": "49.5", "state": "1", "txId": "0xab"},  # credited → 매도 가능
    ]
    hit = match_new_deposit(before, okx_rows, "okx", "PROM", Decimal("49"))
    assert hit and hit["id"] == "d5" and hit["amount"] == Decimal("49.5") and hit["txid"] == "0xab"
    upbit_rows = [{"uuid": "u1", "currency": "PROM", "amount": "50", "state": "ACCEPTED"},
                  {"uuid": "u2", "currency": "PROM", "amount": "50", "state": "PROCESSING"},
                  {"uuid": "u3", "currency": "PROM", "amount": "50", "state": "ACCEPTED", "txid": "t3"}]
    assert match_new_deposit(before, upbit_rows, "upbit", "prom", Decimal(50))["id"] == "u3"
    assert match_new_deposit(before, upbit_rows, "upbit", "PROM", Decimal(51)) is None
    assert deposit_id({"txid": "x"}) == "x" and deposit_id({}) == ""
    assert ACCEPTED["bithumb"] == {"ACCEPTED"}


def test_live_withdraw_backend_locked(monkeypatch):
    monkeypatch.delenv("LIVE_TRADING_ALLOWED", raising=False)
    be = LiveWithdrawBackend(Config(raw={}), allow_live=True)   # 코드가 열어도 환경 잠금이 막는다
    with pytest.raises(LiveLockError):
        asyncio.run(be.withdraw("PROM", "okx", "upbit", Decimal(1), "addr", network="PROM-ERC20"))
    monkeypatch.setenv("LIVE_TRADING_ALLOWED", "1")
    be2 = LiveWithdrawBackend(Config(raw={}), allow_live=True)
    with pytest.raises(Exception) as ei:                        # 잠금은 열렸으나 network 없음 → 거부 (키 검사 전)
        asyncio.run(be2.withdraw("PROM", "okx", "upbit", Decimal(1), "addr"))
    assert "network" in str(ei.value)
