"""사이클 저널 — SQLite(WAL) 영속화 (T6: 부수효과 전 기록, 재기동 이어가기)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .model import OPEN_STATES, Cycle


class CycleStore:
    def __init__(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS cycles ("
            "id TEXT PRIMARY KEY, state TEXT NOT NULL, updated_ms INTEGER NOT NULL, body TEXT NOT NULL)"
        )
        self._db.commit()

    def save(self, c: Cycle) -> None:
        """상태 전이마다 호출 — 부수효과(알림·다음 단계) 전에 기록한다."""
        ts = max(c.stamps.values()) if c.stamps else 0
        self._db.execute(
            "INSERT INTO cycles(id, state, updated_ms, body) VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET state=excluded.state, updated_ms=excluded.updated_ms, body=excluded.body",
            (c.id, c.state, ts, c.to_json()),
        )
        self._db.commit()

    def load_open(self) -> list[Cycle]:
        q = ",".join("?" for _ in OPEN_STATES)
        rows = self._db.execute(f"SELECT body FROM cycles WHERE state IN ({q})", OPEN_STATES).fetchall()
        return [Cycle.from_json(r[0]) for r in rows]

    def save_inv(self, row: dict) -> None:
        """재고 선배치형(§1.5) 슬라이스/리밸런싱 원장 — 즉시 종결이라 열린 상태 없음."""
        import json

        self._db.execute(
            "CREATE TABLE IF NOT EXISTS inv_cycles (ts INTEGER NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL)"
        )
        self._db.execute(
            "INSERT INTO inv_cycles(ts, kind, body) VALUES(?,?,?)",
            (int(row.get("ts", 0)), str(row.get("kind", "?")), json.dumps(row)),
        )
        self._db.commit()

    def inv_summary(self, today_start_ms: int) -> dict:
        """재고 모드 요약 — 슬라이스 손익과 리밸런싱 비용을 분리 집계 (§1.5 회계)."""
        import json

        out = {"slices": 0, "pnl_today": 0.0, "pnl_total": 0.0, "rebalances": 0, "rebalance_cost": 0.0}
        try:
            rows = self._db.execute("SELECT ts, kind, body FROM inv_cycles").fetchall()
        except sqlite3.OperationalError:
            return out  # 테이블 미생성 = 재고 모드 실행 이력 없음
        for ts, kind, body in rows:
            pnl = json.loads(body).get("pnl_usd") or 0.0
            if kind == "rebalance":
                out["rebalances"] += 1
                out["rebalance_cost"] += -pnl
            else:
                out["slices"] += 1
            out["pnl_total"] += pnl
            if ts >= today_start_ms:
                out["pnl_today"] += pnl
        return out

    def summary(self, today_start_ms: int) -> dict:
        """원장 요약 — /status·/report와 재기동 시 일손익 복원용 (T13 우회 방지)."""
        import json

        rows = self._db.execute("SELECT state, updated_ms, body FROM cycles").fetchall()
        out = {"settled": 0, "wins": 0, "pnl_total": 0.0, "pnl_today": 0.0, "open": 0, "by_coin": {}}
        for state, updated_ms, body in rows:
            if state in OPEN_STATES:
                out["open"] += 1
                continue
            if state not in ("SETTLED", "SETTLED_STUCK"):
                continue
            d = json.loads(body)
            pnl = d.get("pnl_usd") or 0.0
            out["settled"] += 1
            out["pnl_total"] += pnl
            out["by_coin"][d.get("coin", "?")] = out["by_coin"].get(d.get("coin", "?"), 0.0) + pnl
            if pnl > 0:
                out["wins"] += 1
            if updated_ms >= today_start_ms:
                out["pnl_today"] += pnl
        return out

    def close(self) -> None:
        self._db.close()
