"""주문 어댑터 계층 검증 (M3ⓑ) — 규칙·서명·파서·실거래 잠금. 네트워크 호출 없음."""
import asyncio
import base64
import hashlib
import json
import urllib.parse

import pytest

from kimp.exec.base import LiveLockError, OrderResult, make_client_id
from kimp.exec.bithumb import BithumbOrderAdapter
from kimp.exec.okx import OkxOrderAdapter, parse_okx_order
from kimp.exec.rules import (
    BITHUMB_KRW_TICKS,
    UPBIT_KRW_TICKS,
    check_min_notional,
    ioc_buy_price,
    ioc_sell_price,
    krw_tick,
    quantize_qty,
)
from kimp.exec.upbit import UpbitOrderAdapter, parse_upbit_order
from kimp.models import D


# ---------- 규칙 ----------

def test_upbit_tick_bands():
    assert krw_tick(D(2_500_000), UPBIT_KRW_TICKS) == D(1000)
    assert krw_tick(D(150_000), UPBIT_KRW_TICKS) == D(50)
    assert krw_tick(D(5_000), UPBIT_KRW_TICKS) == D(1)
    assert krw_tick(D("0.005"), UPBIT_KRW_TICKS) == D("0.000001")
    assert krw_tick(D(7_000), BITHUMB_KRW_TICKS) == D(5)   # 빗썸 5k-10k 밴드 차이


def test_ioc_pricing_crosses_book():
    # 매수: margin 후 tick 올림 → 항상 ask 이상 (크로스 유지)
    p = ioc_buy_price(D(141_050), D("0.002"), D(50))
    assert p >= D(141_050) and p % D(50) == 0
    # 매도: tick 내림 → 항상 bid 이하
    s = ioc_sell_price(D(141_000), D("0.002"), D(50))
    assert s <= D(141_000) and s % D(50) == 0


def test_qty_quantize_and_min_notional():
    assert quantize_qty(D("1.23456789999"), D("0.00000001")) == D("1.23456789")
    assert quantize_qty(D("7.9"), D("0.1")) == D("7.9")
    assert check_min_notional(D(100), D(49), D(5000)) is False
    assert check_min_notional(D(100), D(50), D(5000)) is True


def test_client_id_constraints():
    cid = make_client_id("kimp-c1_")
    assert cid.isalnum() and len(cid) <= 32 and cid.startswith("kimpc1")
    assert make_client_id() != make_client_id()


# ---------- OKX 파서 ----------

def test_parse_okx_order_states():
    def resp(state, fill="0", fee="-0.1"):
        return {"code": "0", "data": [{
            "ordId": "123", "clOrdId": "kimpabc", "state": state,
            "accFillSz": fill, "avgPx": "100.5" if fill != "0" else "",
            "fee": fee, "feeCcy": "USDT",
        }]}
    r = parse_okx_order(resp("filled", "5"))
    assert r.status == "filled" and r.filled_qty == D(5) and r.avg_price == D("100.5")
    assert r.fee == D("0.1") and r.fee_currency == "USDT"   # 음수 수수료 → 지불액 양수화
    assert parse_okx_order(resp("canceled", "2")).status == "partial"
    assert parse_okx_order(resp("canceled", "0", "0")).status == "canceled"
    assert parse_okx_order(resp("live")).status == "open"
    assert parse_okx_order({"code": "51603", "data": []}, "cid").status == "unknown"


# ---------- 업비트/빗썸 파서 (공용 스키마) ----------

def test_parse_upbit_order_with_trades():
    d = {
        "uuid": "u-1", "identifier": "kimpxyz", "market": "KRW-PROM", "state": "done",
        "executed_volume": "10", "paid_fee": "70.5",
        "trades": [
            {"price": "1410", "volume": "6", "funds": "8460"},
            {"price": "1400", "volume": "4", "funds": "5600"},
        ],
    }
    r = parse_upbit_order(d)
    assert r.status == "filled" and r.filled_qty == D(10)
    assert r.avg_price == D(14060) / D(10)                  # Σfunds/Σvolume
    assert r.fee == D("70.5") and r.fee_currency == "KRW"
    assert parse_upbit_order({**d, "state": "cancel", "executed_volume": "3"}).status == "partial"
    assert parse_upbit_order({**d, "state": "cancel", "executed_volume": "0", "trades": []}).status == "canceled"
    assert parse_upbit_order({**d, "state": "wait"}).status == "open"
    assert parse_upbit_order({"error": {"name": "x"}}).status == "unknown"


# ---------- JWT query_hash ----------

def test_upbit_jwt_query_hash():
    from kimp.collectors.wallet_upbit import make_jwt

    params = {"market": "KRW-PROM", "side": "bid", "volume": "10", "price": "1410",
              "ord_type": "limit", "time_in_force": "ioc", "identifier": "kimpabc"}
    tok = make_jwt("ak", "sk", params)
    payload_b64 = tok.split(".")[1]
    payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)))
    expected = hashlib.sha512(urllib.parse.urlencode(params, doseq=True).encode()).hexdigest()
    assert payload["query_hash"] == expected and payload["query_hash_alg"] == "SHA512"
    # 파라미터 없으면 기존 형태 유지 (지갑 조회 하위호환)
    tok2 = make_jwt("ak", "sk")
    payload2_b64 = tok2.split(".")[1]
    payload2 = json.loads(base64.urlsafe_b64decode(payload2_b64 + "=" * (-len(payload2_b64) % 4)))
    assert "query_hash" not in payload2


def test_bithumb_jwt_has_timestamp_and_query_hash():
    from kimp.collectors.wallet_bithumb import make_bithumb_jwt

    tok = make_bithumb_jwt("ak", "sk", {"uuid": "u-1"})
    payload_b64 = tok.split(".")[1]
    payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)))
    assert "timestamp" in payload and payload["query_hash_alg"] == "SHA512"


# ---------- 실거래 이중 잠금 ----------

def test_live_lock_blocks_orders_without_env(monkeypatch):
    monkeypatch.delenv("LIVE_TRADING_ALLOWED", raising=False)
    for adapter in (
        OkxOrderAdapter("k", "s", "p", allow_live=True),     # 코드가 열어도 환경 잠금이 막는다
        UpbitOrderAdapter("k", "s", allow_live=True),
        BithumbOrderAdapter("k", "s", allow_live=False),
    ):
        with pytest.raises(LiveLockError):
            asyncio.run(adapter.place_ioc(None, "buy", "PROM", "USDT", D(1), D(1), "kimpx"))

    monkeypatch.setenv("LIVE_TRADING_ALLOWED", "1")
    assert OkxOrderAdapter("k", "s", "p", allow_live=False).allow_live is False  # 생성자 잠금도 독립
    assert OkxOrderAdapter("k", "s", "p", allow_live=True).allow_live is True


def test_order_result_done_semantics():
    assert OrderResult("okx", "1", "c", "partial").done
    assert not OrderResult("okx", "1", "c", "open").done
    assert not OrderResult("okx", "", "c", "unknown").done
