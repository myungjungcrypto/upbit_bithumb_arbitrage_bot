"""바이낸스 실행 부품 검증 — 주문 파서·규칙·출금 상태·입금 매칭. 네트워크 없음."""
import asyncio
from decimal import Decimal

import pytest

from kimp.exec.base import LiveLockError
from kimp.exec.binance import BinanceOrderAdapter, parse_binance_order, parse_exchange_info
from kimp.exec.deposits import match_new_deposit
from kimp.exec.withdraw import norm_binance_wd_state
from kimp.models import D


def test_parse_binance_order_full_response():
    d = {"symbol": "PROMUSDT", "orderId": 77, "clientOrderId": "kimpj1b", "status": "FILLED",
         "executedQty": "9.98000000", "cummulativeQuoteQty": "99.99960000",
         "fills": [{"price": "10.02", "qty": "5", "commission": "0.005", "commissionAsset": "PROM"},
                   {"price": "10.02", "qty": "4.98", "commission": "0.00498", "commissionAsset": "PROM"}]}
    r = parse_binance_order(d)
    assert r.status == "filled" and r.filled_qty == D("9.98") and r.client_id == "kimpj1b"
    assert abs(r.avg_price - D("10.02")) < D("0.0001")
    assert r.fee == D("0.00998") and r.fee_currency == "PROM"          # 매수 수수료는 코인으로 징수
    assert parse_binance_order({**d, "status": "EXPIRED", "executedQty": "3"}).status == "partial"
    assert parse_binance_order({**d, "status": "EXPIRED", "executedQty": "0", "fills": []}).status == "canceled"
    assert parse_binance_order({"status": "NEW", "executedQty": "0"}).status == "open"
    assert parse_binance_order({"code": -2013, "msg": "Order does not exist."}, "cid").status == "unknown"
    # 조회 응답(fills 없음) + myTrades로 수수료 보강
    q = {"orderId": 77, "origClientOrderId": "kimpj1b", "status": "FILLED", "executedQty": "2", "cummulativeQuoteQty": "20"}
    r2 = parse_binance_order(q, trades=[{"commission": "0.02", "commissionAsset": "USDT"}])
    assert r2.avg_price == D(10) and r2.fee == D("0.02") and r2.fee_currency == "USDT"


def test_parse_exchange_info_filters():
    info = {"symbols": [{"symbol": "PROMUSDT", "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.00100000"},
        {"filterType": "LOT_SIZE", "stepSize": "0.01000000", "minQty": "0.01000000"},
        {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
    ]}]}
    tick, step, min_qty, min_notional = parse_exchange_info(info, "PROMUSDT")
    assert (tick, step, min_qty, min_notional) == (D("0.001"), D("0.01"), D("0.01"), D("5"))
    with pytest.raises(Exception):
        parse_exchange_info(info, "NOPEUSDT")


def test_binance_withdraw_state_and_deposit_match():
    assert norm_binance_wd_state(6) == "done" and norm_binance_wd_state("4") == "sent"
    assert norm_binance_wd_state(3) == "failed" and norm_binance_wd_state("2") == "pending"
    rows = [{"id": "d1", "txId": "0xold", "coin": "PROM", "amount": "9.9", "status": 1},
            {"id": "d2", "txId": "0xnew", "coin": "PROM", "amount": "9.9", "status": 6}]   # credited(거래 가능)
    hit = match_new_deposit({"0xold"}, rows, "binance", "PROM", D("9"))
    assert hit and hit["id"] == "0xnew" and hit["amount"] == D("9.9")


def test_binance_adapter_locked(monkeypatch):
    monkeypatch.delenv("LIVE_TRADING_ALLOWED", raising=False)
    with pytest.raises(LiveLockError):
        asyncio.run(BinanceOrderAdapter("k", "s", allow_live=True).place_ioc(None, "buy", "PROM", "USDT", D(10), D(1), "kimpx"))
