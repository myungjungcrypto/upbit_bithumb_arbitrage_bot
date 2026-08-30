"""intent-first 실행 저널 검증 (M3ⓒ) — 전이 규칙·단일 active·재시작 복구 판단."""
import pytest

from kimp.exec.base import OrderResult
from kimp.exec.journal import (
    TERMINAL,
    ExecutionJournal,
    IllegalTransition,
    buy_client_id,
    next_action,
    sell_client_id,
)
from kimp.models import D


def _j(tmp_path):
    return ExecutionJournal(tmp_path / "exec.db")


def test_intent_first_and_full_happy_path(tmp_path):
    j = _j(tmp_path)
    jid = j.begin("PROM:upbit>okx:in", {"notional_usd": 500})
    assert jid is not None
    assert j.get(jid)["state"] == "INTENT_RECORDED"          # API 호출 전에 이미 기록됨
    for st in ("BUY_SUBMITTED", "BUY_DONE", "WITHDRAW_REQUESTED", "WITHDRAW_SENT",
               "DEPOSIT_CREDITED", "SELL_SUBMITTED", "SELL_DONE", "SETTLED"):
        j.advance(jid, st, {"last": st})
    d = j.get(jid)
    assert d["state"] == "SETTLED" and d["last"] == "SETTLED" and d["meta"]["notional_usd"] == 500
    assert j.active() == []                                  # 말단 도달 → active에서 제외


def test_single_active_per_route(tmp_path):
    j = _j(tmp_path)
    jid = j.begin("PROM:upbit>okx:in", {})
    assert j.begin("PROM:upbit>okx:in", {}) is None          # 같은 라우트 이중 발사 금지
    assert j.begin("ONG:upbit>okx:in", {}) is not None       # 다른 라우트는 허용
    j.advance(jid, "BUY_SUBMITTED")
    j.advance(jid, "ABORTED")                                # RECONCILE 외 예외 전이도 허용 맵 검증
    assert j.begin("PROM:upbit>okx:in", {}) is not None      # 종결 후 재개 가능


def test_illegal_transitions_raise(tmp_path):
    j = _j(tmp_path)
    jid = j.begin("r", {})
    with pytest.raises(IllegalTransition):
        j.advance(jid, "SELL_DONE")                          # 단계 건너뛰기 금지
    j.advance(jid, "BUY_SUBMITTED")
    j.advance(jid, "RECONCILE_REQUIRED")                     # 어디서든 reconcile 진입은 허용
    with pytest.raises(IllegalTransition):
        j.advance(jid, "BUY_DONE")                           # 말단에서 전이 금지


def test_restart_recovery_load(tmp_path):
    j = _j(tmp_path)
    jid = j.begin("PROM:upbit>okx:in", {"qty": "5"})
    j.advance(jid, "BUY_SUBMITTED")
    j.close()
    j2 = _j(tmp_path)                                        # 재시작
    act = j2.active()
    assert len(act) == 1 and act[0]["id"] == jid and act[0]["state"] == "BUY_SUBMITTED"


def test_client_ids_deterministic_and_valid():
    assert buy_client_id(7) == "kimpj7b" == buy_client_id(7)
    assert sell_client_id(7) == "kimpj7s0"
    assert sell_client_id(7, 2) == "kimpj7s2"                # 재시도 = 새 ID (identifier 소모 대응)
    assert all(c.isalnum() for c in sell_client_id(123456, 3)) and len(sell_client_id(123456, 3)) <= 32


def _o(status, qty="0"):
    return OrderResult("okx", "1", "cid", status, filled_qty=D(qty))


def test_next_action_matrix():
    assert next_action("INTENT_RECORDED", None, None) == "abort"           # 발사 전 = 안전 종결
    assert next_action("BUY_SUBMITTED", None, None) == "reconcile"         # 접수 불명 → 사람
    assert next_action("BUY_SUBMITTED", _o("unknown"), None) == "reconcile"
    assert next_action("BUY_SUBMITTED", _o("canceled"), None) == "abort"   # 전량 미체결 = 자금 이동 없음
    assert next_action("BUY_SUBMITTED", _o("partial", "3"), None) == "mark_buy_done"
    assert next_action("BUY_SUBMITTED", _o("filled", "5"), None) == "mark_buy_done"
    assert next_action("BUY_SUBMITTED", _o("open"), None) == "wait_buy"    # 재주문 금지, 재조회만
    for st in ("BUY_DONE", "WITHDRAW_REQUESTED", "WITHDRAW_SENT"):
        assert next_action(st, None, None) == "check_withdraw"
    assert next_action("DEPOSIT_CREDITED", None, None) == "submit_sell"
    assert next_action("SELL_SUBMITTED", None, None) == "reconcile"
    assert next_action("SELL_SUBMITTED", None, _o("canceled")) == "submit_sell"   # 코인은 도착해 있음 — 재시도
    assert next_action("SELL_SUBMITTED", None, _o("filled", "5")) == "mark_sell_done"
    assert next_action("SELL_SUBMITTED", None, _o("open")) == "wait_sell"
    assert next_action("SELL_DONE", None, None) == "mark_sell_done"
    assert TERMINAL == {"SETTLED", "ABORTED", "RECONCILE_REQUIRED"}
