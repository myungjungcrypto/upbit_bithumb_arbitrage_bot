"""출금 게이트웨이 정책 엔진 + 관제탑 명령 처리 검증 (§4.1 방어선 3~5)."""
import asyncio
from decimal import Decimal

from kimp.alerts.control import TelegramControl
from kimp.alerts.telegram import Alerter
from kimp.cycle.gateway import WithdrawalGateway


class FakeControl:
    def __init__(self, approve: bool):
        self.approve = approve
        self.requests: list[str] = []

    async def request_approval(self, text, timeout_sec):
        self.requests.append(text)
        return self.approve


class FakeBackend:
    def __init__(self):
        self.calls = []

    async def withdraw(self, coin, from_ex, to_ex, amount, address, **route):
        self.calls.append((coin, from_ex, to_ex, amount, address))
        self.routes = getattr(self, "routes", []) + [route]
        return "TX123"


CFG = {
    "mode": "supervised", "per_tx_usd_cap": 3000, "daily_usd_cap": 5000,
    "approval_timeout_sec": 1,
    "allowlist": [{"coin": "ONG", "from": "bithumb", "to": "binance", "address": "AXyzAddress123456",
                   "network": "ONT", "extra": {"exchange_name": "binance"}}],
}


def _gw(approve=True, cfg=None):
    be = FakeBackend()
    gw = WithdrawalGateway(cfg or CFG, FakeControl(approve), Alerter("", ""), be)
    return gw, be


def test_gateway_allowlist_deny():
    gw, be = _gw()
    async def go():
        assert await gw.request("XRP", "bithumb", "binance", Decimal(100), Decimal(500), "t") is None
        assert await gw.request("ONG", "upbit", "binance", Decimal(100), Decimal(500), "t") is None
        assert be.calls == []
    asyncio.run(go())


def test_gateway_caps():
    gw, be = _gw()
    async def go():
        assert await gw.request("ONG", "bithumb", "binance", Decimal(1), Decimal(3500), "t") is None  # 건당
        assert await gw.request("ONG", "bithumb", "binance", Decimal(1), Decimal(3000), "t") == "TX123"
        assert await gw.request("ONG", "bithumb", "binance", Decimal(1), Decimal(2500), "t") is None  # 일일 5000 초과
        assert len(be.calls) == 1
    asyncio.run(go())


def test_gateway_supervised_approve_and_deny():
    gw, be = _gw(approve=True)
    async def go():
        assert await gw.request("ONG", "bithumb", "binance", Decimal(700), Decimal(1000), "사이클 c1") == "TX123"
        assert be.calls[0][:3] == ("ONG", "bithumb", "binance")
    asyncio.run(go())

    gw2, be2 = _gw(approve=False)
    async def go2():
        assert await gw2.request("ONG", "bithumb", "binance", Decimal(700), Decimal(1000), "t") is None
        assert be2.calls == []
    asyncio.run(go2())


def test_gateway_no_control_denies():
    be = FakeBackend()
    gw = WithdrawalGateway(CFG, None, Alerter("", ""), be)
    async def go():
        assert await gw.request("ONG", "bithumb", "binance", Decimal(1), Decimal(100), "t") is None
        assert be.calls == []
    asyncio.run(go())


# ---------- 관제탑 명령 처리 ----------

def _ctl():
    c = TelegramControl("tok", "12345")
    c.on_command("/status", lambda: "OK-STATUS")
    c.on_command("/stop", lambda: "STOPPED")
    return c

def _msg(chat_id, text):
    return {"message": {"chat": {"id": chat_id}, "text": text}}


def test_control_command_and_auth():
    c = _ctl()
    assert c.handle_update(_msg(12345, "/status")) == ("12345", "OK-STATUS")
    assert c.handle_update(_msg(99999, "/status")) is None          # 화이트리스트 외 무시
    reply = c.handle_update(_msg(12345, "/unknown"))
    assert "/status" in reply[1]                                     # 도움말
    assert c.handle_update(_msg(12345, "그냥 텍스트")) is None


def test_control_approval_callback():
    c = _ctl()
    async def go():
        fut = asyncio.get_running_loop().create_future()
        c._pending["abc"] = fut
        r = c.handle_update({"callback_query": {
            "message": {"chat": {"id": 12345}}, "data": "ap:abc:1", "id": "cb1"}})
        assert r == ("12345", "✅ 승인됨") and fut.result() is True
        # 다른 채팅의 콜백은 무시
        fut2 = asyncio.get_running_loop().create_future()
        c._pending["def"] = fut2
        assert c.handle_update({"callback_query": {
            "message": {"chat": {"id": 666}}, "data": "ap:def:1", "id": "cb2"}}) is None
        assert not fut2.done()
    asyncio.run(go())


def test_gateway_passes_route_details_to_backend():
    """M3ⓓ: allowlist의 network/memo/extra가 백엔드까지 전달된다 (거래소별 출금 파라미터의 출처)."""
    gw, be = _gw()
    async def go():
        assert await gw.request("ONG", "bithumb", "binance", Decimal(1), Decimal(100), "t") == "TX123"
        assert be.routes[-1] == {"network": "ONT", "memo": "", "extra": {"exchange_name": "binance"}}
    asyncio.run(go())
