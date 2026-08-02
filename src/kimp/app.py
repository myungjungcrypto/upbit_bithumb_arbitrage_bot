"""P0 앱 배선 — 수집기 → 버스 → 엔진/저장/알림, 시그널 종료 처리."""
from __future__ import annotations

import asyncio
import logging
import signal

from .alerts.telegram import CRIT, INFO, WARN, Alerter
from .bus import Bus
from .collectors.binance import BinanceCollector
from .collectors.bithumb import BithumbCollector
from .collectors.bybit import BybitCollector
from .collectors.fx import FxCollector
from .collectors.okx import OkxCollector
from .collectors.upbit import UpbitCollector
from .collectors.wallet_bithumb import BithumbWalletStatusCollector
from .config import Config
from .engine.books import BookStore
from .engine.runner import PremiumEngine
from .storage.parquet import (
    BufferedParquetWriter,
    book_row,
    fx_row,
    flusher,
    health_row,
    trade_row,
    wallet_row,
)
from .universe import listing_watcher, resolve_universe

log = logging.getLogger("app")

COLLECTOR_CLASSES = {
    "upbit": UpbitCollector,
    "bithumb": BithumbCollector,
    "binance": BinanceCollector,
    "bybit": BybitCollector,
    "okx": OkxCollector,
}


async def run(cfg: Config) -> None:
    bus = Bus()
    store = BookStore()

    # 유니버스 해석 (국내 KRW 전 종목 ∩ 바이낸스, 실패 시 config 시드 폴백)
    uni = await resolve_universe(cfg)
    log.info("universe: all=%d upbit=%d bithumb=%d binance=%d",
             len(uni["all"]), len(uni["upbit"]), len(uni["bithumb"]), len(uni["binance"]))
    engine = PremiumEngine(bus, store, cfg, coins=uni["all"])

    st = cfg.storage
    root = st.get("root", "data")
    flush_rows = int(st.get("flush_rows", 5000))
    flush_sec = float(st.get("flush_sec", 30))
    writers = {
        name: BufferedParquetWriter(root, name, flush_rows, flush_sec)
        for name in ("books", "trades", "fx", "premium", "health", "wallet")
    }

    alerter = Alerter(
        cfg.telegram_token,
        cfg.telegram_chat_id,
        cooldown_sec=float(cfg.alerts.get("cooldown_sec", 300)),
    )
    if cfg.telegram_enabled and alerter.live:
        log.info("telegram alerts: ON")
    else:
        log.info("telegram alerts: log-only (no token/chat_id)")

    stop = asyncio.Event()

    def _on_signal() -> None:
        log.info("shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    # --- 소비자 루프 ---
    book_snapshot_ms = int(st.get("book_snapshot_interval_ms", 1000))
    last_book_saved: dict[tuple, int] = {}

    async def consume_books() -> None:
        q = bus.subscribe("book")
        while not stop.is_set():
            try:
                b = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            store.update(b)
            engine.on_book(b)
            prev = last_book_saved.get(b.key, 0)
            if b.ts_local - prev >= book_snapshot_ms:
                last_book_saved[b.key] = b.ts_local
                writers["books"].add(book_row(b))

    async def consume_trades() -> None:
        q = bus.subscribe("trade")
        while not stop.is_set():
            try:
                t = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            writers["trades"].add(trade_row(t))

    async def consume_fx() -> None:
        q = bus.subscribe("fx")
        while not stop.is_set():
            try:
                f = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            engine.on_fx(f)
            writers["fx"].add(fx_row(f))

    edge_thr = float(cfg.alerts.get("net_edge_threshold", 0.005))

    async def consume_premium() -> None:
        q = bus.subscribe("premium")
        while not stop.is_set():
            try:
                rows = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            writers["premium"].add_many(rows)
            for r in rows:
                for direction in ("in", "out"):
                    net = r.get(f"{direction}_net")
                    if net is not None and net >= edge_thr:
                        alerter.alert(
                            INFO,
                            f"edge:{r['coin']}:{direction}:{r['dom_ex']}",
                            f"{r['coin']} {direction.upper()} 순엣지 {net*100:.2f}% "
                            f"({r['dom_ex']}↔{r['ovs_ex']}, ${r['notional_usd']:,.0f}, "
                            f"실행김프 {(r['exec_mid'] or 0)*100:.2f}%)",
                        )

    health_cd = float(cfg.alerts.get("health_cooldown_sec", 600))

    async def consume_health() -> None:
        q = bus.subscribe("health")
        while not stop.is_set():
            try:
                h = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            writers["health"].add(health_row(h))
            if h.status in ("stale", "error"):
                alerter.alert(WARN, f"health:{h.component}:{h.status}",
                              f"{h.component}: {h.status} — {h.detail}", cooldown=health_cd)
            elif h.status == "new_listing":
                alerter.alert(WARN, f"listing:{h.detail[:40]}", h.detail, cooldown=0)

    universe_set = set(uni["all"])

    async def consume_wallet() -> None:
        """빗썸 입출금 상태 변화 — T4 게이트 ①. 유니버스 코인의 정지/복구는 알림."""
        q = bus.subscribe("wallet")
        while not stop.is_set():
            try:
                w = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            writers["wallet"].add(wallet_row(w))
            if w.coin not in universe_set and w.coin != "USDT":
                continue
            key = f"wallet:{w.exchange}:{w.coin}"
            if not w.deposit_ok or not w.withdraw_ok:
                state = f"입금 {'가능' if w.deposit_ok else '정지'} / 출금 {'가능' if w.withdraw_ok else '정지'}"
                alerter.alert(WARN, key, f"{w.exchange} {w.coin} {state} — 해당 코인 김프 급등은 함정 가능성", cooldown=health_cd)
            else:
                alerter.alert(INFO, key + ":ok", f"{w.exchange} {w.coin} 입출금 정상화", cooldown=health_cd)

    async def status_reporter() -> None:
        """주기 상태 로그 + 텔레그램 하트비트 (PLAN §1.3 데드맨의 발신측)."""
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass
            counts = {c.name: c.msg_count for c in collectors}
            log.info("status: msgs=%s dropped=%s", counts, dict(bus.dropped))
            alerter.alert(INFO, "heartbeat", f"수집 정상 — 메시지 수신 {counts}", cooldown=3600)

    # --- 수집기 기동 ---
    # 국내·바이낸스는 해석된 유니버스, 바이비트·OKX는 시드 코인만 (P0: 김프 기준은 binance)
    per_exchange_coins = {
        "upbit": uni["upbit"],
        "bithumb": uni["bithumb"],
        "binance": uni["binance"],
        "bybit": cfg.coins,
        "okx": cfg.coins,
    }
    collectors = []
    for name in [*cfg.domestic, *cfg.overseas]:
        cls = COLLECTOR_CLASSES.get(name)
        if cls is None:
            log.warning("unknown exchange in config: %s", name)
            continue
        collectors.append(cls(bus, per_exchange_coins.get(name, cfg.coins)))
    fx = FxCollector(bus, cfg.fx.get("url", ""), float(cfg.fx.get("poll_sec", 5)))
    wallet = BithumbWalletStatusCollector(
        bus, float(cfg.raw.get("wallet_status", {}).get("bithumb_poll_sec", 60))
    )

    tasks = [
        *(asyncio.create_task(c.run(stop), name=f"collector.{c.name}") for c in collectors),
        asyncio.create_task(fx.run(stop), name="collector.fx"),
        asyncio.create_task(wallet.run(stop), name="collector.wallet.bithumb"),
        asyncio.create_task(listing_watcher(bus, cfg, set(uni["upbit"]), stop), name="universe.watcher"),
        asyncio.create_task(consume_books(), name="consume.books"),
        asyncio.create_task(consume_trades(), name="consume.trades"),
        asyncio.create_task(consume_fx(), name="consume.fx"),
        asyncio.create_task(consume_premium(), name="consume.premium"),
        asyncio.create_task(consume_health(), name="consume.health"),
        asyncio.create_task(consume_wallet(), name="consume.wallet"),
        asyncio.create_task(status_reporter(), name="status"),
        asyncio.create_task(flusher(list(writers.values()), stop), name="flusher"),
        asyncio.create_task(alerter.sender(stop), name="telegram.sender"),
    ]

    alerter.alert(
        INFO,
        "startup",
        f"P0 수집기 기동 — 거래소 {[c.name for c in collectors]}, 유니버스 {len(uni['all'])}개 코인 "
        f"(업비트 {len(uni['upbit'])} / 빗썸 {len(uni['bithumb'])} / 바이낸스 {len(uni['binance'])})",
    )
    try:
        await stop.wait()
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for w in writers.values():
            w.flush()
        log.info("shutdown complete")
