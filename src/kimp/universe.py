"""거래 유니버스 자동 해석 — 국내 KRW 전 종목 ∩ 바이낸스 USDT (T3).

원칙: 수집은 넓게, 실행 후보는 P1 실측이 뽑는다. 모든 원격 조회는 실패 시
config 시드(coins)로 폴백한다 — 유니버스 조회 실패가 수집기를 막으면 안 된다.

주기 리프레시는 '신규 상장 감지 알림'까지만 수행한다 (T3: 신규 원화상장 = 기회 코어).
런타임 재구독은 하지 않는다 — 유니버스 반영은 재시작으로 (systemd 재시작이면 수 초).
"""
from __future__ import annotations

import asyncio
import logging

import aiohttp

from .config import Config
from .models import Health, now_ms

log = logging.getLogger("universe")

# 스테이블·원화연동 등 "코인"으로 취급하지 않는 심볼 (USDT는 레일이지 기회 코인이 아님)
STABLES = {"USDT", "USDC", "TUSD", "DAI", "FDUSD", "PYUSD", "USDS", "BUSD", "USD1"}

UPBIT_MARKETS_URL = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER_ALL_URL = "https://api.upbit.com/v1/ticker/all?quote_currencies=KRW"
BITHUMB_MARKETS_URL = "https://api.bithumb.com/v1/market/all"  # 업비트 호환 (VERIFY 실배포 시)
BINANCE_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"


async def _get_json(sess: aiohttp.ClientSession, url: str):
    async with sess.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def fetch_upbit_krw_bases(sess) -> set[str]:
    data = await _get_json(sess, UPBIT_MARKETS_URL)
    return {m["market"].split("-", 1)[1] for m in data if m.get("market", "").startswith("KRW-")}


async def fetch_bithumb_krw_bases(sess) -> set[str]:
    data = await _get_json(sess, BITHUMB_MARKETS_URL)
    return {m["market"].split("-", 1)[1] for m in data if m.get("market", "").startswith("KRW-")}


async def fetch_binance_usdt_bases(sess) -> set[str]:
    data = await _get_json(sess, BINANCE_EXCHANGE_INFO_URL)
    return {
        s["baseAsset"]
        for s in data.get("symbols", [])
        if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
        and s.get("isSpotTradingAllowed", True)
    }


async def fetch_bybit_usdt_bases(sess) -> set[str]:
    data = await _get_json(
        sess, "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000"
    )
    return {
        i["baseCoin"]
        for i in (data.get("result") or {}).get("list", [])
        if i.get("quoteCoin") == "USDT" and i.get("status") == "Trading"
    }


async def fetch_okx_usdt_bases(sess) -> set[str]:
    data = await _get_json(sess, "https://www.okx.com/api/v5/public/instruments?instType=SPOT")
    out = set()
    for i in data.get("data", []):
        inst = i.get("instId", "")
        if inst.endswith("-USDT") and i.get("state") == "live":
            out.add(inst.rsplit("-", 1)[0])
    return out


async def fetch_upbit_turnover(sess) -> dict[str, float]:
    """코인 → 24h 원화 거래대금. max_coins 컷의 정렬 기준."""
    data = await _get_json(sess, UPBIT_TICKER_ALL_URL)
    out: dict[str, float] = {}
    for t in data:
        market = t.get("market", "")
        if market.startswith("KRW-"):
            out[market.split("-", 1)[1]] = float(t.get("acc_trade_price_24h") or 0)
    return out


def build_universe(
    seed: list[str],
    upbit_bases: set[str] | None,
    bithumb_bases: set[str] | None,
    binance_bases: set[str] | None,
    turnover: dict[str, float],
    max_coins: int,
    include: list[str],
    exclude: list[str],
    bybit_bases: set[str] | None = None,
    okx_bases: set[str] | None = None,
) -> dict[str, list[str]]:
    """순수 함수 (테스트 대상). 반환: {"upbit": [...], "bithumb": [...], "binance": [...], "all": [...]}"""
    banned = STABLES | set(exclude)

    def clean(bases: set[str]) -> set[str]:
        return {b for b in bases if b not in banned}

    ovs = clean(binance_bases) if binance_bases is not None else None

    def domestic(dom_bases: set[str] | None) -> set[str]:
        if dom_bases is None:
            cand = set(seed) - banned  # 조회 실패 → 시드 폴백
        else:
            cand = clean(dom_bases)
        if ovs is not None:
            cand &= ovs
        elif dom_bases is not None:
            # 바이낸스 목록 실패 시 시드로 제한 (해외에 없는 코인 구독 방지)
            cand &= set(seed)
        return cand

    up = domestic(upbit_bases)
    bt = domestic(bithumb_bases)
    allc = up | bt | (set(include) - banned)

    if len(allc) > max_coins:
        ranked = sorted(allc, key=lambda c: (-turnover.get(c, 0.0), c))
        keep = set(ranked[:max_coins]) | (set(include) - banned)
        dropped = len(allc) - len(keep)
        if dropped > 0:
            log.info("universe capped: %d dropped by max_coins=%d (24h 거래대금 기준)", dropped, max_coins)
        allc = keep
        up &= allc
        bt &= allc

    def ovs_list(bases: set[str] | None) -> list[str]:
        """해외 거래소별 구독 목록 — 유니버스 ∩ 그 거래소 상장분 (조회 실패 시 시드)."""
        if bases is None:
            return sorted(allc & set(seed)) or sorted(set(seed) - banned)
        return sorted(allc & clean(bases))

    return {
        "upbit": sorted(up),
        "bithumb": sorted(bt),
        "binance": sorted(allc if ovs is None else (allc & ovs)),
        "bybit": ovs_list(bybit_bases),
        "okx": ovs_list(okx_bases),
        "all": sorted(allc),
    }


async def resolve_universe(cfg: Config) -> dict[str, list[str]]:
    ucfg = cfg.raw.get("universe", {})
    seed = cfg.coins
    if ucfg.get("mode", "auto") != "auto":
        return {"upbit": seed, "bithumb": seed, "binance": seed, "bybit": seed, "okx": seed, "all": seed}

    async with aiohttp.ClientSession(
        trust_env=True, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    ) as sess:
        async def safe(coro, name):
            try:
                return await coro
            except Exception as e:
                log.warning("universe fetch failed (%s): %r — 폴백 사용", name, e)
                return None

        upbit_b, bithumb_b, binance_b, bybit_b, okx_b, turnover = await asyncio.gather(
            safe(fetch_upbit_krw_bases(sess), "upbit"),
            safe(fetch_bithumb_krw_bases(sess), "bithumb"),
            safe(fetch_binance_usdt_bases(sess), "binance"),
            safe(fetch_bybit_usdt_bases(sess), "bybit"),
            safe(fetch_okx_usdt_bases(sess), "okx"),
            safe(fetch_upbit_turnover(sess), "turnover"),
        )
    uni = build_universe(
        seed,
        upbit_b,
        bithumb_b,
        binance_b,
        turnover or {},
        int(ucfg.get("max_coins", 150)),
        list(ucfg.get("include", [])),
        list(ucfg.get("exclude", [])),
        bybit_bases=bybit_b,
        okx_bases=okx_b,
    )
    log.info(
        "universe resolved: upbit=%d bithumb=%d binance=%d bybit=%d okx=%d all=%d",
        len(uni["upbit"]), len(uni["bithumb"]), len(uni["binance"]),
        len(uni["bybit"]), len(uni["okx"]), len(uni["all"]),
    )
    return uni


async def listing_watcher(bus, cfg: Config, known: set[str], stop: asyncio.Event) -> None:
    """신규 KRW 상장 감지 (T3 — 상장 초기 = 기회 코어). 재구독은 하지 않고 알림만."""
    interval = float(cfg.raw.get("universe", {}).get("refresh_sec", 1800))
    async with aiohttp.ClientSession(
        trust_env=True, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    ) as sess:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            try:
                current = await fetch_upbit_krw_bases(sess)
            except Exception:
                continue
            fresh = current - known
            if fresh:
                known |= fresh
                bus.publish(
                    "health",
                    Health("universe", "new_listing", f"업비트 신규 KRW 상장 감지: {sorted(fresh)} — 재시작 시 수집 편입", now_ms()),
                )
