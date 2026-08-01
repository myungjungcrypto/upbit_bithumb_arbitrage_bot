import pytest

from kimp.symbols import (
    binance_symbol,
    okx_inst,
    parse_concat_symbol,
    parse_dash_code,
    parse_okx_inst,
    upbit_code,
)


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
