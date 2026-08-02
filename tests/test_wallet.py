from kimp.bus import Bus
from kimp.collectors.wallet_bithumb import BithumbWalletStatusCollector, parse_assetsstatus
from kimp.collectors.wallet_upbit import make_jwt, parse_wallet_status


def test_parse_upbit_wallet_status():
    data = [
        {"currency": "BTC", "wallet_state": "working"},
        {"currency": "xrp", "wallet_state": "deposit_only"},
        {"currency": "ZIL", "wallet_state": "withdraw_only"},
        {"currency": "SOL", "wallet_state": "paused"},
        {"currency": "ETC", "wallet_state": "unsupported"},
        {"currency": None, "wallet_state": "working"},   # 무시
        "garbage",                                        # 무시
    ]
    rows = dict((c, (d, w)) for c, d, w in parse_wallet_status(data))
    assert rows["BTC"] == (True, True)
    assert rows["XRP"] == (True, False)   # 대문자 정규화
    assert rows["ZIL"] == (False, True)
    assert rows["SOL"] == (False, False)  # paused → 보수적 정지
    assert rows["ETC"] == (False, False)
    assert len(rows) == 5


def test_make_jwt_shape():
    tok = make_jwt("ak", "sk")
    parts = tok.split(".")
    assert len(parts) == 3 and all(parts)
    # 같은 키라도 nonce가 달라 토큰이 매번 달라야 함
    assert tok != make_jwt("ak", "sk")


def test_process_initial_snapshot_flags_and_summary():
    c = BithumbWalletStatusCollector(Bus())
    parsed = [("BTC", True, True), ("ZIL", False, False), ("XRP", True, False)]
    events, suspended = c._process(parsed, ts=1)
    assert len(events) == 3 and all(e.initial for e in events)
    assert suspended == ["ZIL(입금·출금 정지)", "XRP(출금 정지)"]

    # 두 번째 폴링: 변화 1건만, initial=False, 요약 없음
    parsed2 = [("BTC", True, True), ("ZIL", True, True), ("XRP", True, False)]
    events2, suspended2 = c._process(parsed2, ts=2)
    assert len(events2) == 1
    assert events2[0].coin == "ZIL" and events2[0].deposit_ok and not events2[0].initial
    assert suspended2 == []


def test_parse_assetsstatus_normal():
    data = {
        "status": "0000",
        "data": {
            "BTC": {"withdrawal_status": 1, "deposit_status": 1},
            "XRP": {"withdrawal_status": 0, "deposit_status": 1},
            "sol": {"withdrawal_status": "1", "deposit_status": "0"},
        },
    }
    rows = dict((c, (d, w)) for c, d, w in parse_assetsstatus(data))
    assert rows["BTC"] == (True, True)
    assert rows["XRP"] == (True, False)
    assert rows["SOL"] == (False, True)  # 대문자 정규화 + 문자열 숫자 허용


def test_parse_assetsstatus_malformed():
    assert parse_assetsstatus({}) == []
    assert parse_assetsstatus({"data": "oops"}) == []
    assert parse_assetsstatus({"data": {"BTC": "oops", "ETH": {"withdrawal_status": None}}}) == []
