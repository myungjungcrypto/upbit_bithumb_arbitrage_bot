from kimp.bus import Bus
from kimp.collectors.wallet_bithumb import BithumbWalletStatusCollector, parse_assetsstatus


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
