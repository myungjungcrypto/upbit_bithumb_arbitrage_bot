"""알림 정책 검증 — INFO 예산·다이제스트·쿨다운·WARN 우회."""
from kimp.alerts.telegram import INFO, WARN, Alerter


def test_info_budget_diverts_overflow_to_digest():
    a = Alerter("tok", "chat", cooldown_sec=0, max_immediate_per_min=3)
    for i in range(5):
        a.alert(INFO, f"k{i}", f"m{i}")
    assert a._queue.qsize() == 3      # 예산 내 즉시 전송
    assert len(a._digest) == 2        # 초과분은 다이제스트로


def test_warn_bypasses_budget():
    a = Alerter("tok", "chat", cooldown_sec=0, max_immediate_per_min=1)
    a.alert(INFO, "i1", "m")
    a.alert(INFO, "i2", "m")          # 예산 소진 → 다이제스트
    a.alert(WARN, "w1", "m")          # WARN은 예산 무관 즉시
    assert a._queue.qsize() == 2
    assert len(a._digest) == 1


def test_cooldown_suppresses_repeat_entirely():
    a = Alerter("tok", "chat", cooldown_sec=300, max_immediate_per_min=10)
    a.alert(INFO, "same", "m1")
    a.alert(INFO, "same", "m2")       # 쿨다운 — 다이제스트에도 안 들어감
    assert a._queue.qsize() == 1
    assert len(a._digest) == 0


def test_log_only_mode_never_queues():
    a = Alerter("", "", cooldown_sec=0)
    a.alert(INFO, "k", "m")
    a.alert(WARN, "k2", "m")
    assert a._queue.qsize() == 0
