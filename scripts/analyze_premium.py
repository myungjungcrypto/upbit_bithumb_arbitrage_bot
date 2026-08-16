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
    cols = ["ts", "coin", "dom_ex", "notional_usd", "in_net", "out_net", "exec_mid"]
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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="data")
    ap.add_argument("--thr", type=float, default=0.005, help="순엣지 임계 (기본 0.5%%)")
    ap.add_argument("--coin", default=None)
    ap.add_argument("--since", default=None, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()

    df = load(args.data, args.coin, args.since)
    span_h = (df["ts"].max() - df["ts"].min()) / 3_600_000
    print(f"# premium ticks: {len(df):,} rows, {span_h:.1f}h, coins={df['coin'].n_unique()}")

    for direction, col in (("INBOUND (김프 방향)", "in_net"), ("OUTBOUND (역프 방향)", "out_net")):
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


if __name__ == "__main__":
    main()
