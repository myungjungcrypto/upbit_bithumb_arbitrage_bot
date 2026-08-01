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
)

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
    engine = PremiumEngine(bus, store, cfg)

    st = cfg.storage
    root = st.get("root", "data")
    flush_rows = int(st.get("flush_rows", 5000))
    flush_sec = float(st.get("flush_sec", 30))
    writers = {
        name: BufferedParquetWriter(root, name, flush_rows, flush_sec)
        for name in ("books", "trades", "fx", "premium", "health")
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
    collectors = []
    for name in [*cfg.domestic, *cfg.overseas]:
        cls = COLLECTOR_CLASSES.get(name)
        if cls is None:
            log.warning("unknown exchange in config: %s", name)
            continue
        collectors.append(cls(bus, cfg.coins))
    fx = FxCollector(bus, cfg.fx.get("url", ""), float(cfg.fx.get("poll_sec", 5)))

    tasks = [
        *(asyncio.create_task(c.run(stop), name=f"collector.{c.name}") for c in collectors),
        asyncio.create_task(fx.run(stop), name="collector.fx"),
        asyncio.create_task(consume_books(), name="consume.books"),
        asyncio.create_task(consume_trades(), name="consume.trades"),
        asyncio.create_task(consume_fx(), name="consume.fx"),
        asyncio.create_task(consume_premium(), name="consume.premium"),
        asyncio.create_task(consume_health(), name="consume.health"),
        asyncio.create_task(status_reporter(), name="status"),
        asyncio.create_task(flusher(list(writers.values()), stop), name="flusher"),
        asyncio.create_task(alerter.sender(stop), name="telegram.sender"),
    ]

    alerter.alert(INFO, "startup", f"P0 수집기 기동 — 거래소 {[c.name for c in collectors]}, 코인 {cfg.coins}")
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
