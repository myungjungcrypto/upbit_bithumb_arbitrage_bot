#!/usr/bin/env python3
"""P1 예비 분석 — 수집된 premium 시계열에서 기회 통계를 뽑는다.

산출:
  1. 코인×거래소×금액대별 기회 에피소드: 횟수, 지속시간(중앙값/p90), 평균·최대 순엣지
     → T3 유니버스 선정과 **엣지 반감기**(= 우리가 얼마나 빨라야 하는가, §1.4 속도 예산)의 근거
  2. 진입 시점 이후 실행김프 변화 분포 (+5/15/30분)
     → 전송창 김프 지속성 = naked(O1) 실비용과 함정 리스크의 정량화

사용:
  pip install polars
  python scripts/analyze_premium.py --data data [--thr 0.005] [--coin XRP] [--since 2026-08-05]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

try:
    import polars as pl
except ImportError:
    sys.exit("polars가 필요합니다: pip install polars")


HORIZONS_MIN = (5, 15, 30)


def load(data_root: str, coin: str | None, since: str | None) -> pl.DataFrame:
    # 필요한 컬럼만 프로젝션 — 소형 파일 다수 환경에서 IO·메모리 수배 절감
    cols = [
        "ts", "coin", "dom_ex", "notional_usd", "in_net", "out_net", "exec_mid",
        "in_capacity_usd", "out_capacity_usd",
    ]
    lf = pl.scan_parquet(f"{data_root}/premium/**/*.parquet").select(cols)
    if coin:
        lf = lf.filter(pl.col("coin") == coin)
    if since:
        ts = int(datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        lf = lf.filter(pl.col("ts") >= ts)
    df = lf.collect()
    if df.is_empty():
        sys.exit(f"데이터 없음: {data_root}/premium (coin={coin}, since={since})")
    return df.sort("ts")


def episodes(df: pl.DataFrame, edge_col: str, thr: float, gap_ms: int = 5000) -> pl.DataFrame:
    """(coin, dom_ex, notional_usd)별로 엣지≥thr 연속 구간을 에피소드로 묶는다."""
    d = (
        df.select(["ts", "coin", "dom_ex", "notional_usd", edge_col])
        .drop_nulls(edge_col)
        .filter(pl.col(edge_col) >= thr)
        .sort(["coin", "dom_ex", "notional_usd", "ts"])
    )
    if d.is_empty():
        return d
    d = d.with_columns(
        (
            (pl.col("ts") - pl.col("ts").shift(1).over(["coin", "dom_ex", "notional_usd"]) > gap_ms)
            .fill_null(True)
            .cum_sum()
            .over(["coin", "dom_ex", "notional_usd"])
        ).alias("episode")
    )
    return (
        d.group_by(["coin", "dom_ex", "notional_usd", "episode"])
        .agg(
            start=pl.col("ts").min(),
            duration_s=((pl.col("ts").max() - pl.col("ts").min()) / 1000),
            peak=pl.col(edge_col).max(),
            mean=pl.col(edge_col).mean(),
            ticks=pl.len(),
        )
        .sort("start")
    )


def episode_summary(ep: pl.DataFrame) -> pl.DataFrame:
    return (
        ep.group_by(["coin", "dom_ex", "notional_usd"])
        .agg(
            n_episodes=pl.len(),
            med_duration_s=pl.col("duration_s").median(),
            p90_duration_s=pl.col("duration_s").quantile(0.9),
            mean_peak_pct=(pl.col("peak").mean() * 100),
            max_peak_pct=(pl.col("peak").max() * 100),
        )
        .sort("n_episodes", descending=True)
    )


def persistence(df: pl.DataFrame, ep: pl.DataFrame) -> pl.DataFrame:
    """에피소드 시작 시점 대비 +N분 후 실행김프 변화(pp) 분포 — 전송창 리스크 프록시."""
    base = df.select(["ts", "coin", "dom_ex", "notional_usd", "exec_mid"]).drop_nulls("exec_mid")
    entries = ep.select(["coin", "dom_ex", "notional_usd", "start"]).join(
        base.rename({"ts": "start", "exec_mid": "exec_at_entry"}),
        on=["coin", "dom_ex", "notional_usd", "start"],
        how="inner",
    )
    rows = []
    for h in HORIZONS_MIN:
        target = entries.with_columns((pl.col("start") + h * 60_000).alias("t_h")).sort("t_h")
        joined = target.join_asof(
            base.sort("ts").rename({"exec_mid": "exec_at_h"}),
            left_on="t_h",
            right_on="ts",
            by=["coin", "dom_ex", "notional_usd"],
            strategy="nearest",
            tolerance=60_000,
        ).drop_nulls("exec_at_h")
        if joined.is_empty():
            continue
        delta = joined.with_columns(((pl.col("exec_at_h") - pl.col("exec_at_entry")) * 100).alias("delta_pp"))
        q = delta.select(
            p10=pl.col("delta_pp").quantile(0.1),
            p50=pl.col("delta_pp").quantile(0.5),
            p90=pl.col("delta_pp").quantile(0.9),
            n=pl.len(),
        ).row(0)
        rows.append({"horizon_min": h, "p10_pp": q[0], "p50_pp": q[1], "p90_pp": q[2], "n": q[3]})
    return pl.DataFrame(rows)


def load_wallet(data_root: str) -> pl.DataFrame | None:
    """입출금 상태 이력 — 가상 수확의 함정 배제용 (진입 시점 상태를 as-of 결합)."""
    try:
        w = (
            pl.scan_parquet(f"{data_root}/wallet/**/*.parquet")
            .select(["ts_local", "exchange", "coin", "deposit_ok", "withdraw_ok"])
            .collect()
        )
        return w if not w.is_empty() else None
    except Exception:
        return None


def hypothetical_harvest(
    df: pl.DataFrame,
    ep: pl.DataFrame,
    direction: str,
    cycle_cap: float,
    cycle_gap_min: float,
    wallet: pl.DataFrame | None,
) -> pl.DataFrame:
    """가상 수확 시뮬레이션 — 실거래가 아니라 '봇이 있었다면'의 추정 (P1).

    에피소드 진입 시점의 순엣지·capacity로 사이클 1회를 가정:
      명목 = min(진입시점 capacity, cycle_cap), 손익 = 명목 × 진입시점 순엣지
    코인·거래소별 재진입 쿨다운(cycle_gap_min)으로 전송·정산 소요를 보수 반영.
    **지갑 게이트 적용**: 진입 시점에 IN=국내 입금 가능 / OUT=국내 출금 가능으로
    확인된 에피소드만 집계 (미확인 = 제외, T4-① 보수 원칙 — RVN·ZIL류 함정 김프 배제).
    OUT(역프)은 헤지 시 진입가가 락되므로 가장 신뢰 가능. IN(naked)은 전송 중
    김프 변동 미반영 — persistence 표의 p10/p90을 오차 범위로 볼 것."""
    if wallet is None:
        return pl.DataFrame([])  # 지갑 이력 없으면 함정 판정 불가 → 집계하지 않음 (부풀리기 방지)
    cap_col = f"{direction}_capacity_usd"
    net_col = f"{direction}_net"
    entries = (
        ep.select(["coin", "dom_ex", "notional_usd", "start"])
        .join(
            df.select(["ts", "coin", "dom_ex", "notional_usd", net_col, cap_col]).rename({"ts": "start"}),
            on=["coin", "dom_ex", "notional_usd", "start"],
            how="inner",
        )
        .sort("start")
    )
    if entries.is_empty():
        return pl.DataFrame([])
    w = wallet.rename({"ts_local": "wts", "exchange": "dom_ex"}).sort("wts")
    entries = entries.join_asof(
        w, left_on="start", right_on="wts", by=["dom_ex", "coin"], strategy="backward"
    )
    flag = "deposit_ok" if direction == "in" else "withdraw_ok"
    entries = entries.filter(pl.col(flag) == True).sort("start")  # noqa: E712 — null(미확인)도 제외
    rows = []
    last_entry: dict[tuple, int] = {}
    gap_ms = int(cycle_gap_min * 60_000)
    for r in entries.iter_rows(named=True):
        key = (r["coin"], r["dom_ex"])
        if key in last_entry and r["start"] - last_entry[key] < gap_ms:
            continue
        cap = r.get(cap_col)
        edge = r.get(net_col)
        if cap is None or edge is None or cap <= 0:
            continue
        last_entry[key] = r["start"]
        notional = min(cap, cycle_cap)
        rows.append(
            {
                "coin": r["coin"],
                "dom_ex": r["dom_ex"],
                "start": r["start"],
                "notional_usd": notional,
                "edge": edge,
                "profit_usd": notional * edge,
            }
        )
    return pl.DataFrame(rows)


def print_harvest(df: pl.DataFrame, ep: pl.DataFrame, direction: str, label: str, wallet, args) -> float:
    h = hypothetical_harvest(df, ep, direction, args.cycle_cap, args.cycle_minutes, wallet)
    if h.is_empty():
        print(f"  {label}: 가상 사이클 없음 (지갑 게이트 통과분 기준)")
        return 0.0
    span_days = max((df["ts"].max() - df["ts"].min()) / 86_400_000, 1e-9)
    total = float(h["profit_usd"].sum())
    print(
        f"  {label}: 가상 사이클 {len(h)}건, 평균 엣지 {float(h['edge'].mean())*100:.2f}%, "
        f"평균 명목 ${float(h['notional_usd'].mean()):,.0f}"
    )
    print(
        f"    → 총 가상손익 ${total:,.0f} ({span_days:.1f}일) ≈ ${total/span_days:,.0f}/일 "
        f"= 자본 ${args.capital:,.0f} 대비 일 {total/span_days/args.capital*100:.2f}%"
    )
    top = (
        h.group_by("coin")
        .agg(profit=pl.col("profit_usd").sum(), cycles=pl.len())
        .sort("profit", descending=True)
        .head(10)
    )
    with pl.Config(tbl_rows=10):
        print(top)
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="data")
    ap.add_argument("--thr", type=float, default=0.005, help="순엣지 임계 (기본 0.5%%)")
    ap.add_argument("--coin", default=None)
    ap.add_argument("--since", default=None, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--cycle-cap", type=float, default=2000, help="가상 사이클당 명목 상한 USD (T13 제안치)")
    ap.add_argument("--cycle-minutes", type=float, default=30, help="코인·거래소별 재진입 쿨다운 (전송+정산 가정)")
    ap.add_argument("--capital", type=float, default=30000, help="수익률 환산 기준 자본 USD")
    args = ap.parse_args()

    df = load(args.data, args.coin, args.since)
    span_h = (df["ts"].max() - df["ts"].min()) / 3_600_000
    print(f"# premium ticks: {len(df):,} rows, {span_h:.1f}h, coins={df['coin'].n_unique()}")

    wallet = load_wallet(args.data)
    if wallet is None:
        print("(주의: 지갑 상태 데이터 없음 — 가상 수확은 함정 판정 불가로 생략됨)")
    base_rung = float(df["notional_usd"].min())
    harvest_total = 0.0
    for direction, col, dkey, dlabel in (
        ("INBOUND (김프 방향)", "in_net", "in", "IN (naked — 전송 중 김프 변동 미반영, 아래 persistence가 오차 범위)"),
        ("OUTBOUND (역프 방향)", "out_net", "out", "OUT (헤지 시 진입가 락 — 가장 신뢰 가능한 추정)"),
    ):
        ep = episodes(df, col, args.thr)
        print(f"\n## {direction} — 순엣지 ≥ {args.thr*100:.2f}% 에피소드")
        if ep.is_empty():
            print("  (없음)")
            continue
        with pl.Config(tbl_rows=args.top, tbl_width_chars=140):
            print(episode_summary(ep))
        pers = persistence(df, ep)
        if not pers.is_empty():
            print("  진입 후 실행김프 변화 (pp, 전송창 프록시 — naked 리스크·함정 판단 근거):")
            print(pers)
        print(f"\n### ⚠️ 가상 수확 시뮬레이션 — 실거래 아님 (사이클 ${args.cycle_cap:,.0f}·쿨다운 {args.cycle_minutes:.0f}분 가정)")
        harvest_total += print_harvest(df, ep.filter(pl.col("notional_usd") == base_rung), dkey, dlabel, wallet, args)

    if harvest_total:
        span_days = max((df["ts"].max() - df["ts"].min()) / 86_400_000, 1e-9)
        print(
            f"\n# 합계(가상): ${harvest_total:,.0f} / {span_days:.1f}일 ≈ "
            f"${harvest_total/span_days:,.0f}/일, 자본 대비 일 {harvest_total/span_days/args.capital*100:.2f}% "
            f"— 진입 시점 지갑 상태 확인분만 집계(미확인 제외), 체결·전송 실패 미반영"
        )


if __name__ == "__main__":
    main()
