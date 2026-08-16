"""버퍼링 Parquet 라이터 — data/{table}/date=YYYY-MM-DD/part-*.parquet 파티션.

가격·수량은 str(Decimal)로 저장해 정밀도를 보존한다 (분석 시 필요한 정밀도로 캐스팅).
파생 비율값(엣지 등)은 엔진이 이미 float으로 변환해 넘긴다.
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("storage.parquet")


def premium_store_gate(
    prev: tuple[float | None, float | None, int] | None,
    in_net: float | None,
    out_net: float | None,
    ts: int,
    min_change: float,
    heartbeat_ms: int,
) -> bool:
    """김프 틱 저장 게이트 — 순엣지가 min_change 이상 변했거나 heartbeat 주기가 지났을 때만 기록.

    계산은 매 틱 하되(트리거·알림은 실시간), 저장은 정보량이 있을 때만 — 디스크 폭주 방지.
    prev = (마지막 저장 in_net, out_net, ts). None이면 첫 기록."""
    if prev is None:
        return True
    p_in, p_out, p_ts = prev
    if ts - p_ts >= heartbeat_ms:
        return True

    def moved(a: float | None, b: float | None) -> bool:
        if (a is None) != (b is None):
            return True  # 깊이 소진 ↔ 회복 전환은 그 자체가 정보
        if a is None:
            return False
        return abs(a - b) >= min_change

    return moved(in_net, p_in) or moved(out_net, p_out)


def purge_old(root: str | Path, days_by_table: dict[str, int], default_days: int, today: date) -> list[str]:
    """보존 기간이 지난 date= 파티션 삭제. 삭제한 경로 목록 반환 (janitor에서 주기 호출)."""
    removed: list[str] = []
    rootp = Path(root)
    if not rootp.exists():
        return removed
    for table_dir in rootp.iterdir():
        if not table_dir.is_dir():
            continue
        days = int(days_by_table.get(table_dir.name, default_days))
        for part in table_dir.glob("date=*"):
            try:
                d = date.fromisoformat(part.name.split("=", 1)[1])
            except ValueError:
                continue
            if (today - d).days >= days:  # 보존 N일 = 오늘 포함 최근 N일 유지
                try:
                    shutil.rmtree(part)
                    removed.append(str(part))
                except OSError:
                    log.exception("retention purge failed: %s", part)
    if removed:
        log.info("retention purge: %d partitions removed", len(removed))
    return removed


def compact_partition(part_dir: Path) -> int:
    """파티션 내 소형 parquet 파일들을 단일 파일로 병합. 반환: 병합한 파일 수 (0=스킵).

    30초 플러시가 만드는 파일 폭탄(일 ~3천 개) 때문에 분석이 IO에 잡아먹히는 문제의 해법.
    스트리밍 병합이라 메모리 사용은 배치 크기로 제한됨. 스키마 불일치 등 실패 시 원본 유지."""
    import pyarrow.dataset as pads

    files = sorted(part_dir.glob("part-*.parquet"))
    if len(files) < 2:
        return 0
    tmp = part_dir / ".compact-tmp.parquet"
    writer = None
    try:
        dataset = pads.dataset([str(f) for f in files], format="parquet")
        for batch in dataset.scanner(batch_size=65536).to_batches():
            if writer is None:
                writer = pq.ParquetWriter(tmp, batch.schema, compression="zstd")
            writer.write_batch(batch)
        if writer is None:
            return 0
        writer.close()
        writer = None
        tmp.rename(part_dir / f"compacted-{files[0].name.removeprefix('part-')}")
        for f in files:
            f.unlink()
        return len(files)
    except Exception:
        log.exception("compaction failed (원본 유지): %s", part_dir)
        return 0
    finally:
        if writer is not None:
            writer.close()
        tmp.unlink(missing_ok=True)


def compact_old_partitions(root: str | Path, today: date, min_files: int = 24) -> int:
    """오늘 이전(불변) 파티션 중 파일 수가 많은 것들을 병합. 반환: 병합된 파일 총수."""
    total = 0
    rootp = Path(root)
    if not rootp.exists():
        return 0
    for table_dir in rootp.iterdir():
        if not table_dir.is_dir():
            continue
        for part in table_dir.glob("date=*"):
            try:
                d = date.fromisoformat(part.name.split("=", 1)[1])
            except ValueError:
                continue
            if d >= today:
                continue  # 오늘 파티션은 아직 쓰는 중 — 건드리지 않음
            if len(list(part.glob("part-*.parquet"))) >= min_files:
                total += compact_partition(part)
    if total:
        log.info("compaction: %d small files merged", total)
    return total


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
