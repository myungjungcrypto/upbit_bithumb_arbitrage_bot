"""verify_universe.judge_legs 순수 로직 검증 (M2 — 레그별 판정·blocklist 압축)."""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "verify_universe", Path(__file__).parent.parent / "scripts" / "verify_universe.py"
)
vu = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vu)


def _maps(**kw):
    """{ex: {coin: ids}} 헬퍼 — ids는 set으로 변환."""
    return {ex: {c: set(ids) for c, ids in m.items()} for ex, m in kw.items()}


def test_identity_per_leg_ticker_collision():
    """실사례 AI: 업비트=젠신 vs 바낸=Sleepless — 바낸 레그만 차단, OKX 레그(젠신)는 허용.
    빗썸 CG 수집이 통째로 실패(빈 맵)면 프록시 금지 → 빗썸 레그는 아예 등재 안 됨 (리뷰 결함 수정)."""
    dom = _maps(upbit={"AI": {"gensyn"}}, bithumb={})
    ovs = _maps(binance={"AI": {"sleepless-ai"}}, bybit={}, okx={"AI": {"gensyn"}})
    verified, blocklist, _ = vu.judge_legs(dom, ovs, {}, {}, bithumb_listed={"AI"})
    assert verified == {"AI": ["upbit>okx"]}
    assert "AI>binance" in blocklist
    assert "AI" not in blocklist


def test_all_legs_fail_global_block():
    dom = _maps(upbit={"XX": {"a"}}, bithumb={})
    ovs = _maps(binance={"XX": {"b"}}, bybit={"XX": {"c"}}, okx={})
    verified, blocklist, _ = vu.judge_legs(dom, ovs, {}, {})
    assert "XX" not in verified
    assert "XX" in blocklist                     # 상장 레그 전멸 → 전역 차단
    assert not any("@" in b or ">" in b for b in blocklist)


def test_network_overlap_per_leg():
    """자산은 동일하나 업비트-바낸만 네트워크 불일치 → 그 조합만 차단."""
    dom = _maps(upbit={"C": {"coin-c"}}, bithumb={"C": {"coin-c"}})
    ovs = _maps(binance={"C": {"coin-c"}}, bybit={"C": {"coin-c"}}, okx={})
    dom_nets = {"upbit": {"C": {"ETH"}}, "bithumb": {"C": {"BSC"}}}
    ovs_nets = {"binance": {"C": {"BSC"}}, "bybit": {"C": {"ETH", "BSC"}}}
    verified, blocklist, _ = vu.judge_legs(dom, ovs, dom_nets, ovs_nets)
    assert verified["C"] == ["bithumb>binance", "bithumb>bybit", "upbit>bybit"]
    assert "C@upbit>binance" in blocklist
    assert "C@upbit" not in blocklist            # upbit>bybit는 살아있으므로 국내 레그 전체 차단 아님


def test_bithumb_proxy_only_when_collected_and_listed():
    """프록시 발동 조건: 빗썸 CG 수집 성공(비어있지 않음) ∧ 실제 빗썸 상장 코인 (리뷰 결함 2건 수정)."""
    dom = _maps(upbit={"D": {"coin-d"}}, bithumb={"OTHER": {"x"}})  # 수집 성공, D만 매핑 부재
    ovs = _maps(binance={"D": {"coin-d"}}, bybit={}, okx={})
    # ① 상장 확인됨 → 프록시 통과
    verified, blocklist, lines = vu.judge_legs(dom, ovs, {}, {}, bithumb_listed={"D"})
    assert set(verified["D"]) == {"upbit>binance", "bithumb>binance"}
    assert any("PROXY" in ln for ln in lines)
    assert not blocklist
    # ② 빗썸 미상장 코인 → 빗썸 레그 미등재 (신규 상장 선등재 방지)
    verified2, _, _ = vu.judge_legs(dom, ovs, {}, {}, bithumb_listed=set())
    assert verified2 == {"D": ["upbit>binance"]}
    # ③ 상장 목록 조회 실패(None) → 프록시 전면 비활성
    verified3, _, _ = vu.judge_legs(dom, ovs, {}, {}, bithumb_listed=None)
    assert verified3 == {"D": ["upbit>binance"]}
    # ④ 빗썸 CG 수집 전체 실패(빈 맵) → 상장 확인돼도 프록시 금지
    dom_fail = _maps(upbit={"D": {"coin-d"}}, bithumb={})
    verified4, _, _ = vu.judge_legs(dom_fail, ovs, {}, {}, bithumb_listed={"D"})
    assert verified4 == {"D": ["upbit>binance"]}


def test_domestic_mapping_failure_is_unknown_not_silent():
    """국내 CG 매핑 실패(빈 ID)는 무흔적 탈락이 아니라 UNKNOWN + blocklist 압축 (리뷰 결함 수정)."""
    dom = _maps(upbit={"F": {""}}, bithumb={"F": {"coin-f"}})
    ovs = _maps(binance={"F": {"coin-f"}}, bybit={}, okx={})
    verified, blocklist, lines = vu.judge_legs(dom, ovs, {}, {})
    assert verified == {"F": ["bithumb>binance"]}
    assert "F@upbit" in blocklist
    assert any("UNKNOWN" in ln and "upbit>binance" in ln for ln in lines)


def test_compat_ok_excludes_partially_failed_coins():
    """구엔진 호환 "ok"는 전 레그 무결 코인만 — 구엔진은 코인 단위로 전 레그를 연다 (리뷰 결함 수정)."""
    verified = {"AI": ["upbit>okx"], "XRP": ["bithumb>binance", "upbit>binance"], "AIX": ["upbit>binance"]}
    blocklist = {"AI>binance", "ZIL"}
    assert vu.compat_ok(verified, blocklist) == ["AIX", "XRP"]  # AI 제외, 접두 유사(AIX)는 오탐 없음


def test_dom_leg_full_fail_compresses_to_dom_entry():
    """한 국내 거래소의 전 해외 레그 실패 → "COIN@dom" 하나로 압축."""
    dom = _maps(upbit={"E": {"real-e"}}, bithumb={"E": {"fake-e"}})
    ovs = _maps(binance={"E": {"real-e"}}, bybit={"E": {"real-e"}}, okx={})
    verified, blocklist, _ = vu.judge_legs(dom, ovs, {}, {})
    assert set(verified["E"]) == {"upbit>binance", "upbit>bybit"}
    assert "E@bithumb" in blocklist
    assert "E@bithumb>binance" not in blocklist  # 압축됨


def test_norm_net_aliases():
    assert vu.norm_net("BASE_ETH") == "BASE" and vu.norm_net("ARB_ETH") == "ARBITRUM"  # 빗썸 L2 표기 (실측 2026-09-02)
    assert vu.norm_net("ZK_ETH") == "ZKSYNC" and vu.norm_net("Sonic") == "S" and vu.norm_net("KUSAMA") == "KSM"
    assert vu.norm_net("MTL_ETH") == "MTL_ETH"                     # 실제 다른 네트워크 — 의도적 미통합
    assert vu.norm_net("ERC20") == "ETH"
    assert vu.norm_net("Arbi") == "ARBITRUM"     # 바이비트 표기
    assert vu.norm_net("Avalanche C-Chain") == "AVAX"
