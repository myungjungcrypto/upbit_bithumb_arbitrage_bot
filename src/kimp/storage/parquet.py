"""버퍼링 Parquet 라이터 — data/{table}/date=YYYY-MM-DD/part-*.parquet 파티션.

가격·수량은 str(Decimal)로 저장해 정밀도를 보존한다 (분석 시 필요한 정밀도로 캐스팅).
파생 비율값(엣지 등)은 엔진이 이미 float으로 변환해 넘긴다.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("storage.parquet")


class BufferedParquetWriter:
    def __init__(self, root: str | Path, table: str, flush_rows: int = 5000, flush_sec: float = 30.0) -> None:
        self.root = Path(root)
        self.table = table
        self.flush_rows = flush_rows
        self.flush_sec = flush_sec
        self._buf: list[dict] = []
        self._last_flush = time.monotonic()

    def add(self, row: dict) -> None:
        self._buf.append(row)
        if len(self._buf) >= self.flush_rows:
            self.flush()

    def add_many(self, rows: list[dict]) -> None:
        self._buf.extend(rows)
        if len(self._buf) >= self.flush_rows:
            self.flush()

    def maybe_flush(self) -> None:
        if self._buf and time.monotonic() - self._last_flush >= self.flush_sec:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        rows, self._buf = self._buf, []
        self._last_flush = time.monotonic()
        try:
            now = datetime.now(timezone.utc)
            d = self.root / self.table / f"date={now:%Y-%m-%d}"
            d.mkdir(parents=True, exist_ok=True)
            path = d / f"part-{now:%H%M%S}-{now.microsecond:06d}.parquet"
            pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
        except Exception:
            log.exception("flush failed for table=%s (%d rows dropped)", self.table, len(rows))


async def flusher(writers: list[BufferedParquetWriter], stop: asyncio.Event, interval: float = 5.0) -> None:
    """주기적 시간기반 플러시 + 종료 시 전량 플러시."""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        for w in writers:
            w.maybe_flush()
    for w in writers:
        w.flush()


def book_row(b) -> dict:
    """Book → 평탄화 행 (상위 15단, 가격·수량 str 보존)."""
    row = {
        "ts_local": b.ts_local,
        "ts_exchange": b.ts_exchange,
        "exchange": b.exchange,
        "base": b.base,
        "quote": b.quote,
    }
    for i in range(15):
        if i < len(b.bids):
            row[f"bid{i+1}_p"] = str(b.bids[i].price)
            row[f"bid{i+1}_s"] = str(b.bids[i].size)
        else:
            row[f"bid{i+1}_p"] = None
            row[f"bid{i+1}_s"] = None
        if i < len(b.asks):
            row[f"ask{i+1}_p"] = str(b.asks[i].price)
            row[f"ask{i+1}_s"] = str(b.asks[i].size)
        else:
            row[f"ask{i+1}_p"] = None
            row[f"ask{i+1}_s"] = None
    return row


def trade_row(t) -> dict:
    return {
        "ts_local": t.ts_local,
        "ts_exchange": t.ts_exchange,
        "exchange": t.exchange,
        "base": t.base,
        "quote": t.quote,
        "price": str(t.price),
        "size": str(t.size),
        "side": t.side,
    }


def fx_row(f) -> dict:
    return {"ts_local": f.ts_local, "pair": f.pair, "rate": str(f.rate), "source": f.source}


def health_row(h) -> dict:
    return {"ts_local": h.ts_local, "component": h.component, "status": h.status, "detail": h.detail}


def wallet_row(w) -> dict:
    return {
        "ts_local": w.ts_local,
        "exchange": w.exchange,
        "coin": w.coin,
        "deposit_ok": w.deposit_ok,
        "withdraw_ok": w.withdraw_ok,
    }
