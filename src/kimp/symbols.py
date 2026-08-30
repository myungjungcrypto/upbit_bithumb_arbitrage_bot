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


def leg_blocked(blocklist: set[str], coin: str, dom_ex: str, ovs_ex: str) -> bool:
    """trade_blocklist 레그 문법 (M2, 대문자 정규화 항목 가정 — V7 판정 결과 반영):

      "COIN"          — 전역 차단
      "COIN@DOM"      — 해당 국내 레그 차단 (모든 해외)
      "COIN>OVS"      — 해당 해외 레그 차단 (모든 국내; 예: 바낸 AI ≠ 업비트 AI)
      "COIN@DOM>OVS"  — 특정 (국내, 해외) 조합만 차단
    """
    c, d, o = coin.upper(), dom_ex.upper(), ovs_ex.upper()
    return (
        c in blocklist
        or f"{c}@{d}" in blocklist
        or f"{c}>{o}" in blocklist
        or f"{c}@{d}>{o}" in blocklist
    )


def leg_key(dom_ex: str, ovs_ex: str) -> str:
    """verified_ok.json v2의 검증 레그 표기 — "DOM>OVS" (대문자)."""
    return f"{dom_ex.upper()}>{ovs_ex.upper()}"
