"""거래소별 심볼 표기 매핑.

정규 표기: (base, quote) 튜플, 예: ("BTC", "KRW"), ("BTC", "USDT").
"""
from __future__ import annotations

DOMESTIC = ("upbit", "bithumb")
OVERSEAS = ("binance", "bybit", "okx")


def upbit_code(base: str, quote: str = "KRW") -> str:
    return f"{quote}-{base}"


# 빗썸 v1 WS API는 업비트와 동일한 코드 체계 사용
bithumb_code = upbit_code


def parse_dash_code(code: str) -> tuple[str, str]:
    """'KRW-BTC' → ('BTC', 'KRW') — 업비트/빗썸 공통."""
    quote, base = code.split("-", 1)
    return base, quote


def binance_symbol(base: str, quote: str = "USDT") -> str:
    return f"{base}{quote}"


def parse_concat_symbol(symbol: str, quote: str = "USDT") -> tuple[str, str]:
    """'BTCUSDT' → ('BTC', 'USDT') — 바이낸스/바이비트 공통."""
    if not symbol.endswith(quote):
        raise ValueError(f"unexpected symbol {symbol!r} (quote {quote!r})")
    return symbol[: -len(quote)], quote


bybit_symbol = binance_symbol


def okx_inst(base: str, quote: str = "USDT") -> str:
    return f"{base}-{quote}"


def parse_okx_inst(inst: str) -> tuple[str, str]:
    """'BTC-USDT' → ('BTC', 'USDT')."""
    base, quote = inst.split("-", 1)
    return base, quote
