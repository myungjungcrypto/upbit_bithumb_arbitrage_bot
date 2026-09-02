"""intent-first 실행 저널 (M3ⓒ) — 주문·출금은 DB에 먼저 기록하고 API를 쏜다.

인계서 §2.12/§9의 핵심 재사용: timeout·크래시·재시작에서 **중복 주문 0**을 만드는 구조.
  - begin()이 API 호출 전에 INTENT_RECORDED 행을 만든다 — "무엇을 하려 했는지"가 항상 남는다
  - 레그별 client_id는 저널 id에서 결정적으로 파생 — 재시작 후에도 같은 ID로 거래소 조회 가능
  - 상태 전이는 허용 맵으로 검증 — 코드 버그가 단계를 건너뛰면 즉시 예외
  - 라우트당 active 저널 1개 — 같은 레그에 두 사이클이 겹쳐 발사되는 것을 구조로 금지
  - next_action(): 재시작/timeout 복구의 순수 판단 함수 — 거래소 조회 결과를 받아
    "무엇을 해야 하는가"만 반환, 부수효과는 오케스트레이터(M3ⓓ~ⓕ)가 수행

전송형(§1.5) 사이클 상태:
  INTENT_RECORDED → BUY_SUBMITTED → BUY_DONE → WITHDRAW_REQUESTED → WITHDRAW_SENT
    → DEPOSIT_CREDITED → SELL_SUBMITTED → SELL_DONE → SETTLED
  예외: ABORTED(자금 이동 전 실패 — 안전 종결) / RECONCILE_REQUIRED(상태 불명·불일치 —
  자동화 중단, 수동 해소 후 재개. 어떤 비말단 상태에서도 진입 가능)
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ..models import now_ms
from .base import OrderResult

STATES = (
    "INTENT_RECORDED", "BUY_SUBMITTED", "BUY_DONE",
    "WITHDRAW_REQUESTED", "WITHDRAW_SENT", "DEPOSIT_CREDITED",
    "SELL_SUBMITTED", "SELL_DONE", "SETTLED",
    "ABORTED", "RECONCILE_REQUIRED",
)
TERMINAL = {"SETTLED", "ABORTED", "RECONCILE_REQUIRED"}

_ALLOWED: dict[str, set[str]] = {
    "INTENT_RECORDED": {"BUY_SUBMITTED", "ABORTED"},
    "BUY_SUBMITTED": {"BUY_DONE", "ABORTED"},
    "BUY_DONE": {"WITHDRAW_REQUESTED", "ABORTED"},
    "WITHDRAW_REQUESTED": {"WITHDRAW_SENT", "ABORTED"},
    "WITHDRAW_SENT": {"DEPOSIT_CREDITED"},
    "DEPOSIT_CREDITED": {"SELL_SUBMITTED"},
    "SELL_SUBMITTED": {"SELL_DONE", "SELL_SUBMITTED"},  # 재시도 = 새 attempt로 재제출 (IOC 전량 미체결 시)
    "SELL_DONE": {"SETTLED"},
}


class IllegalTransition(RuntimeError):
    pass


class ExecutionJournal:
    def __init__(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS exec_journal ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, route TEXT NOT NULL, state TEXT NOT NULL, "
            "created_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL, body TEXT NOT NULL)"
        )
        self._db.commit()

    # ---------- 생성·전이 ----------

    def begin(self, route: str, meta: dict) -> int | None:
        """API 호출 **전에** 호출. 라우트에 active 저널이 있으면 None (이중 발사 금지)."""
        if self.active_for(route) is not None:
            return None
        ts = now_ms()
        cur = self._db.execute(
            "INSERT INTO exec_journal(route, state, created_ms, updated_ms, body) VALUES(?,?,?,?,?)",
            (route, "INTENT_RECORDED", ts, ts, json.dumps({"meta": meta})),
        )
        self._db.commit()
        return int(cur.lastrowid)

    def advance(self, jid: int, state: str, patch: dict | None = None) -> None:
        """상태 전이 + body 병합 — 부수효과(API 호출·알림) **전에** 호출한다 (write-ahead)."""
        cur_state, body = self._get_raw(jid)
        if state != "RECONCILE_REQUIRED" and state not in _ALLOWED.get(cur_state, set()):
            raise IllegalTransition(f"journal {jid}: {cur_state} → {state} 불허")
        if cur_state in TERMINAL:
            raise IllegalTransition(f"journal {jid}: 말단 상태 {cur_state}에서 전이 불가")
        if patch:
            body.update(patch)
        self._db.execute(
            "UPDATE exec_journal SET state=?, updated_ms=?, body=? WHERE id=?",
            (state, now_ms(), json.dumps(body), jid),
        )
        self._db.commit()

    # ---------- 조회 ----------

    def _get_raw(self, jid: int) -> tuple[str, dict]:
        row = self._db.execute("SELECT state, body FROM exec_journal WHERE id=?", (jid,)).fetchone()
        if row is None:
            raise KeyError(f"journal {jid} 없음")
        return row[0], json.loads(row[1])

    def get(self, jid: int) -> dict:
        state, body = self._get_raw(jid)
        return {"id": jid, "state": state, **body}

    def active(self) -> list[dict]:
        q = ",".join("?" for _ in TERMINAL)
        rows = self._db.execute(
            f"SELECT id, route, state, body FROM exec_journal WHERE state NOT IN ({q})",
            tuple(TERMINAL),
        ).fetchall()
        return [{"id": r[0], "route": r[1], "state": r[2], **json.loads(r[3])} for r in rows]

    def active_for(self, route: str) -> dict | None:
        for j in self.active():
            if j["route"] == route:
                return j
        return None

    def close(self) -> None:
        self._db.close()


# ---------- client ID 파생 (결정적 — 재시작 복구의 열쇠) ----------

def buy_client_id(jid: int) -> str:
    return f"kimpj{jid}b"


def sell_client_id(jid: int, attempt: int = 0) -> str:
    """매도 재시도는 새 ID (업비트 identifier는 취소된 주문에도 소모되어 재사용 불가).
    attempt는 저널 body의 sell_attempt로 영속 — 재시작 후에도 같은 ID로 조회 가능."""
    return f"kimpj{jid}s{attempt}"


# ---------- 복구 판단 (순수 함수 — 부수효과 없음, 테스트 대상) ----------

def next_action(state: str, buy: OrderResult | None, sell: OrderResult | None) -> str:
    """재시작/timeout 시: 저널 상태 + 거래소 조회 결과 → 다음 행동.

    반환:
      abort            — 자금 이동 전 실패 확정 (주문 미접수·전량 미체결) → ABORTED로 닫기
      mark_buy_done    — 매수 체결 확인됨 → BUY_DONE 기록 후 출금 단계로
      wait_buy         — 매수 미종결 → 재조회 (재주문 금지)
      check_withdraw   — 출금 단계 상태를 게이트웨이/거래소에서 대조 (오케스트레이터 몫)
      wait_deposit     — 입금 감지 대기 재개
      mark_sell_done   — 매도 체결 확인됨 → SELL_DONE→SETTLED로
      wait_sell        — 매도 미종결 → 재조회 (재주문 금지)
      submit_sell      — 매도 발사(또는 전량 미체결 재시도) — **새 attempt의 client_id로** (identifier 소모됨)
      reconcile        — 판단 불가/불일치 → RECONCILE_REQUIRED (자동화 중단)
    """
    if state == "INTENT_RECORDED":
        return "abort"  # 주문 발사 전 — 아무것도 안 나갔음이 저널로 보장됨
    if state == "BUY_SUBMITTED":
        if buy is None or buy.status == "unknown":
            # 접수 여부 불명 — 거래소 조회가 '주문 없음'을 명시해도 v1은 보수적으로 사람 확인
            return "reconcile"
        if buy.status == "canceled":
            return "abort"
        if buy.status in ("filled", "partial"):
            return "mark_buy_done"
        return "wait_buy"
    if state in ("BUY_DONE", "WITHDRAW_REQUESTED", "WITHDRAW_SENT"):
        return "check_withdraw"
    if state == "DEPOSIT_CREDITED":
        return "submit_sell"
    if state == "SELL_SUBMITTED":
        if sell is None or sell.status == "unknown":
            return "reconcile"
        if sell.status == "canceled":
            return "submit_sell"  # 매도 IOC 전량 미체결 — 같은 ID 계열로 재시도 (코인은 이미 도착해 있음)
        if sell.status in ("filled", "partial"):
            return "mark_sell_done"
        return "wait_sell"
    if state == "SELL_DONE":
        return "mark_sell_done"  # SETTLED 마감만 남음
    return "reconcile"
