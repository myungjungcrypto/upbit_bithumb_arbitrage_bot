from kimp.collectors.wallet_bithumb import parse_assetsstatus


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
