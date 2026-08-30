"""유니버스 해석 순수 로직 검증 — 교집합·폴백·컷·스테이블 제외."""
from kimp.universe import STABLES, build_universe

SEED = ["BTC", "ETH", "XRP"]


def test_intersection_and_stable_exclusion():
    uni = build_universe(
        seed=SEED,
        upbit_bases={"BTC", "ETH", "XRP", "USDT", "KRWONLY"},
        bithumb_bases={"BTC", "DOGE"},
        binance_bases={"BTC", "ETH", "XRP", "DOGE", "USDC"},
        turnover={},
        max_coins=150,
        include=[],
        exclude=[],
    )
    assert uni["upbit"] == ["BTC", "ETH", "XRP"]      # USDT(스테이블)·KRWONLY(해외 미상장) 제외
    assert uni["bithumb"] == ["BTC", "DOGE"]
    assert set(uni["all"]) == {"BTC", "ETH", "XRP", "DOGE"}
    assert set(uni["binance"]) == {"BTC", "ETH", "XRP", "DOGE"}


def test_binance_fetch_failure_limits_to_seed():
    uni = build_universe(
        seed=SEED,
        upbit_bases={"BTC", "ETH", "XRP", "DOGE", "PEPE"},
        bithumb_bases=None,
        binance_bases=None,
        turnover={},
        max_coins=150,
        include=[],
        exclude=[],
    )
    # 해외 목록 실패 → 국내 목록은 시드로 제한, 빗썸 실패 → 시드 폴백
    assert uni["upbit"] == ["BTC", "ETH", "XRP"]
    assert uni["bithumb"] == ["BTC", "ETH", "XRP"]


def test_all_fetch_failure_falls_back_to_seed():
    uni = build_universe(SEED, None, None, None, {}, 150, [], [])
    assert uni["all"] == sorted(SEED)


def test_max_coins_cut_by_turnover():
    bases = {f"C{i}" for i in range(10)}
    turnover = {f"C{i}": float(i) for i in range(10)}  # C9가 최대
    uni = build_universe([], bases, None, bases, turnover, 3, [], [])
    assert set(uni["all"]) == {"C9", "C8", "C7"}


def test_include_exclude():
    uni = build_universe(
        seed=[],
        upbit_bases={"BTC", "BAD"},
        bithumb_bases=None,
        binance_bases={"BTC", "BAD", "FORCED"},
        turnover={},
        max_coins=150,
        include=["FORCED"],
        exclude=["BAD"],
    )
    assert "BAD" not in uni["all"]
    assert "FORCED" in uni["all"]


def test_stables_never_in_universe():
    uni = build_universe([], STABLES | {"BTC"}, None, STABLES | {"BTC"}, {}, 150, [], [])
    assert uni["all"] == ["BTC"]


def test_multi_overseas_lists_are_universe_intersections():
    """M1: bybit/okx 구독 목록 = 유니버스 ∩ 각 거래소 상장분, 조회 실패 시 시드 폴백."""
    uni = build_universe(
        seed=SEED,
        upbit_bases={"BTC", "ETH", "XRP", "DOGE"},
        bithumb_bases=set(),
        binance_bases={"BTC", "ETH", "XRP", "DOGE"},
        turnover={},
        max_coins=150,
        include=[],
        exclude=[],
        bybit_bases={"BTC", "XRP", "NOTKRW"},   # NOTKRW는 국내 미상장 → 제외
        okx_bases=None,                          # 조회 실패
    )
    assert uni["bybit"] == ["BTC", "XRP"]
    assert uni["okx"] == sorted(set(SEED) & set(uni["all"]))  # 폴백: 유니버스 ∩ 시드
