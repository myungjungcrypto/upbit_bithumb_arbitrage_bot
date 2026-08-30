import pytest

from kimp.symbols import (
    binance_symbol,
    leg_blocked,
    leg_key,
    okx_inst,
    parse_concat_symbol,
    parse_dash_code,
    parse_okx_inst,
    upbit_code,
)


def test_leg_blocked_grammar():
    bl = {"ZIL", "VIRTUAL@BITHUMB", "AI>BINANCE", "PROM@UPBIT>OKX"}
    assert leg_blocked(bl, "zil", "upbit", "okx")            # 전역
    assert leg_blocked(bl, "VIRTUAL", "bithumb", "binance")  # 국내 레그 (모든 해외)
    assert not leg_blocked(bl, "VIRTUAL", "upbit", "binance")
    assert leg_blocked(bl, "AI", "upbit", "binance")         # 해외 레그 (모든 국내)
    assert leg_blocked(bl, "AI", "bithumb", "binance")
    assert not leg_blocked(bl, "AI", "upbit", "okx")         # 다른 해외는 허용
    assert leg_blocked(bl, "PROM", "upbit", "okx")           # 특정 조합
    assert not leg_blocked(bl, "PROM", "upbit", "binance")
    assert not leg_blocked(bl, "PROM", "bithumb", "okx")
    assert not leg_blocked(set(), "BTC", "upbit", "binance")


def test_leg_key():
    assert leg_key("upbit", "okx") == "UPBIT>OKX"


def test_dash_roundtrip():
    assert upbit_code("BTC") == "KRW-BTC"
    assert parse_dash_code("KRW-BTC") == ("BTC", "KRW")
    assert parse_dash_code("KRW-USDT") == ("USDT", "KRW")


def test_concat_roundtrip():
    assert binance_symbol("XRP") == "XRPUSDT"
    assert parse_concat_symbol("XRPUSDT") == ("XRP", "USDT")
    with pytest.raises(ValueError):
        parse_concat_symbol("XRPBTC")


def test_okx_roundtrip():
    assert okx_inst("SOL") == "SOL-USDT"
    assert parse_okx_inst("SOL-USDT") == ("SOL", "USDT")
