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

    def close(self) -> None:
        self._db.close()
