"""앱 배선 — 수집기 → 버스 → 엔진/저장/알림 + P2 페이퍼 트레이딩, 시그널 종료 처리."""
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from .alerts.telegram import CRIT, INFO, WARN, Alerter
from .bus import Bus
from .collectors.binance import BinanceCollector
from .collectors.bithumb import BithumbCollector
from .collectors.bybit import BybitCollector
from .collectors.fx import FxCollector
from .collectors.okx import OkxCollector
from .collectors.upbit import UpbitCollector
from .collectors.wallet_binance import BinanceWalletStatusCollector
from .collectors.wallet_bithumb import BithumbWalletStatusCollector
from .collectors.wallet_upbit import UpbitWalletStatusCollector
from .config import Config
from .cycle.risk import RiskManager
from .cycle.store import CycleStore
from .engine.books import BookStore
from .engine.runner import PremiumEngine
from .paper.engine import PaperEngine
from .storage.parquet import (
    BufferedParquetWriter,
    book_row,
    compact_old_partitions,
    fx_row,
    flusher,
    health_row,
    premium_store_gate,
    purge_old,
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
        for name in ("books", "trades", "fx", "premium", "health", "wallet", "paper_cycles")
    }

    alerter = Alerter(
        cfg.telegram_token,
        cfg.telegram_chat_id,
        cooldown_sec=float(cfg.alerts.get("cooldown_sec", 300)),
        max_immediate_per_min=int(cfg.alerts.get("max_immediate_per_min", 6)),
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
    # 거래소별 최신 입출금 상태 캐시 — T4 게이트 ①의 판단용 (P0는 빗썸만, 업비트/해외는 P0.5)
    wallet_state: dict[tuple[str, str], tuple[bool, bool]] = {}

    # --- P2 페이퍼 트레이딩 (T13 확정 한도로 전체 사이클 시뮬레이션) ---
    paper_cfg = cfg.raw.get("paper", {})
    risk = RiskManager(paper_cfg.get("risk", {}))
    cycle_store = CycleStore(Path(root) / "cycles.db")
    paper = PaperEngine(bus, store, cycle_store, risk, alerter, wallet_state, cfg, writers["paper_cycles"])
    log.info("paper trading: %s", "ON" if paper.enabled else "OFF")

    prem_min_change = float(st.get("premium_min_change", 0.0005))
    prem_heartbeat_ms = int(st.get("premium_heartbeat_ms", 10000))
    prem_last_stored: dict[tuple, tuple[float | None, float | None, int]] = {}

    async def consume_premium() -> None:
        q = bus.subscribe("premium")
        while not stop.is_set():
            try:
                rows = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            for r in rows:
                gk = (r["coin"], r["dom_ex"], r["notional_usd"])
                if premium_store_gate(
                    prem_last_stored.get(gk), r["in_net"], r["out_net"], r["ts"],
                    prem_min_change, prem_heartbeat_ms,
                ):
                    prem_last_stored[gk] = (r["in_net"], r["out_net"], r["ts"])
                    writers["premium"].add(r)
            for r in rows:
                for direction in ("in", "out"):
                    net = r.get(f"{direction}_net")
                    if net is None or net < edge_thr:
                        continue
                    coin, dom_ex = r["coin"], r["dom_ex"]
                    # T4 게이트: IN = 해외 출금 가능 ∧ 국내 입금 가능 / OUT = 국내 출금 가능 ∧ 해외 입금 가능
                    ws = wallet_state.get((dom_ex, coin))
                    ovs_ws = wallet_state.get((r["ovs_ex"], coin))
                    blocked = None
                    if ws is not None:
                        dep_ok, wd_ok = ws
                        if direction == "in" and not dep_ok:
                            blocked = f"{dom_ex} 입금 정지"
                        elif direction == "out" and not wd_ok:
                            blocked = f"{dom_ex} 출금 정지"
                    if blocked is None and ovs_ws is not None:
                        ovs_dep, ovs_wd = ovs_ws
                        if direction == "in" and not ovs_wd:
                            blocked = f"{r['ovs_ex']} 출금 정지"
                        elif direction == "out" and not ovs_dep:
                            blocked = f"{r['ovs_ex']} 입금 정지"
                    cap = r.get(f"{direction}_capacity_usd")
                    cap_txt = f", capacity ~${cap:,.0f}" if cap else ""
                    body = (
                        f"{coin} {direction.upper()} 순엣지 {net*100:.2f}% "
                        f"({dom_ex}↔{r['ovs_ex']}, ${r['notional_usd']:,.0f}, "
                        f"실행김프 {(r['exec_mid'] or 0)*100:.2f}%{cap_txt})"
                    )
                    if blocked:
                        alerter.alert(
                            WARN,
                            f"trap:{coin}:{direction}:{dom_ex}",
                            f"함정 — {coin} {blocked} 중인데 {direction.upper()} 엣지 "
                            f"{net*100:.1f}%. 먹을 수 없는 김프 (T4-①)",
                            cooldown=health_cd,
                        )
                    elif abs(net) >= 0.10:
                        alerter.alert(
                            WARN,
                            f"anomaly:{coin}:{direction}:{dom_ex}",
                            f"비정상 괴리 — {body} · 지갑 상태 미확인({dom_ex}) — 함정/데이터 이상 확인 필요",
                            cooldown=health_cd,
                        )
                    else:
                        alerter.alert(INFO, f"edge:{coin}:{direction}:{dom_ex}", body)

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
            elif h.status == "wallet_snapshot":
                alerter.alert(WARN, f"wallet_snapshot:{h.component}", h.detail, cooldown=health_cd)

    universe_set = set(uni["all"])

    async def consume_wallet() -> None:
        """빗썸 입출금 상태 변화 — T4 게이트 ①. 상태 캐시 갱신 + 유니버스 코인의 정지/복구 알림.

        기동 첫 스냅샷(initial)은 저장·캐시만 하고 개별 알림은 억제 — 정지 중인 코인은
        수집기가 요약 1건(health: wallet_snapshot)으로 보고한다."""
        q = bus.subscribe("wallet")
        while not stop.is_set():
            try:
                w = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            wallet_state[(w.exchange, w.coin)] = (w.deposit_ok, w.withdraw_ok)
            writers["wallet"].add(wallet_row(w))
            if w.initial:
                continue
            if w.coin not in universe_set and w.coin != "USDT":
                continue
            key = f"wallet:{w.exchange}:{w.coin}"
            if not w.deposit_ok or not w.withdraw_ok:
                state = f"입금 {'가능' if w.deposit_ok else '정지'} / 출금 {'가능' if w.withdraw_ok else '정지'}"
                alerter.alert(WARN, key, f"{w.exchange} {w.coin} {state} — 해당 코인 김프 급등은 함정 가능성", cooldown=health_cd)
            else:
                alerter.alert(INFO, key + ":ok", f"{w.exchange} {w.coin} 입출금 정상화", cooldown=health_cd)

    def _rss_mb() -> float:
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS"):
                        return int(line.split()[1]) / 1024
        except Exception:
            pass
        return 0.0

    retention = st.get("retention_days", {}) or {}
    retention_default = int(retention.get("default", 30))

    async def retention_janitor() -> None:
        """보존 초과 파티션 삭제 + 지난 파티션 소형 파일 컴팩션 — 기동 직후 1회 + 6시간마다.
        디스크 IO 작업이므로 이벤트 루프를 막지 않게 스레드 실행."""
        from datetime import datetime as _dt, timezone as _tz

        loop = asyncio.get_running_loop()
        while not stop.is_set():
            today = _dt.now(_tz.utc).date()
            try:
                await loop.run_in_executor(
                    None,
                    lambda: (
                        purge_old(root, retention, retention_default, today),
                        compact_old_partitions(root, today),
                    ),
                )
            except Exception:
                log.exception("retention janitor failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=6 * 3600)
                return
            except asyncio.TimeoutError:
                pass

    async def status_reporter() -> None:
        """주기 상태 로그 + 텔레그램 하트비트 (PLAN §1.3 데드맨의 발신측). RSS 추적은 누수 감시용."""
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass
            counts = {c.name: c.msg_count for c in collectors}
            open_n = len(risk.open_notional)
            log.info(
                "status: msgs=%s dropped=%s rss=%.0fMB paper(open=%d, today=$%.2f%s)",
                counts, dict(bus.dropped), _rss_mb(), open_n, risk.daily_pnl,
                f", L1:{risk.halted}" if risk.halted else "",
            )
            alerter.alert(
                INFO, "heartbeat",
                f"수집 정상 — RSS {_rss_mb():.0f}MB · PAPER: 진행 {open_n}건, 오늘 ${risk.daily_pnl:,.2f}"
                + (f" · ⚠️L1: {risk.halted}" if risk.halted else ""),
                cooldown=3600,
            )

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
    wcfg = cfg.raw.get("wallet_status", {})
    wallet_collectors = [
        BithumbWalletStatusCollector(bus, float(wcfg.get("bithumb_poll_sec", 60)))
    ]
    if cfg.upbit_access_key and cfg.upbit_secret_key:
        wallet_collectors.append(
            UpbitWalletStatusCollector(
                bus, cfg.upbit_access_key, cfg.upbit_secret_key,
                float(wcfg.get("upbit_poll_sec", 60)),
            )
        )
        log.info("upbit wallet status: ON (조회 전용 키 감지)")
    else:
        log.info("upbit wallet status: OFF (UPBIT_ACCESS_KEY/SECRET_KEY 미설정 — 함정 판정은 빗썸만)")
    if cfg.binance_api_key and cfg.binance_api_secret:
        wallet_collectors.append(
            BinanceWalletStatusCollector(
                bus, cfg.binance_api_key, cfg.binance_api_secret,
                float(wcfg.get("binance_poll_sec", 60)),
            )
        )
        log.info("binance wallet status: ON (V8 사각 해소 — 해외측 게이트 활성)")
    else:
        log.info("binance wallet status: OFF (BINANCE_API_KEY/SECRET 미설정 — V8 사각 존재)")

    tasks = [
        *(asyncio.create_task(c.run(stop), name=f"collector.{c.name}") for c in collectors),
        asyncio.create_task(fx.run(stop), name="collector.fx"),
        *(
            asyncio.create_task(w.run(stop), name=f"collector.wallet.{w.exchange}")
            for w in wallet_collectors
        ),
        asyncio.create_task(listing_watcher(bus, cfg, set(uni["upbit"]), stop), name="universe.watcher"),
        asyncio.create_task(consume_books(), name="consume.books"),
        asyncio.create_task(consume_trades(), name="consume.trades"),
        asyncio.create_task(consume_fx(), name="consume.fx"),
        asyncio.create_task(consume_premium(), name="consume.premium"),
        asyncio.create_task(consume_health(), name="consume.health"),
        asyncio.create_task(consume_wallet(), name="consume.wallet"),
        asyncio.create_task(status_reporter(), name="status"),
        asyncio.create_task(retention_janitor(), name="retention"),
        asyncio.create_task(paper.run(stop), name="paper"),
        asyncio.create_task(flusher(list(writers.values()), stop), name="flusher"),
        asyncio.create_task(alerter.sender(stop), name="telegram.sender"),
        asyncio.create_task(alerter.digest_loop(stop), name="telegram.digest"),
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
