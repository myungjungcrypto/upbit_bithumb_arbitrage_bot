#!/usr/bin/env python3
"""P1 예비 분석 — premium 시계열에서 기회 통계 + 가상 수확 추정.

메모리 설계: **일 파티션 단위 청크 처리** — 전체 기간을 한 번에 로드하지 않고
하루치만 메모리에 올린다 (8GB 공유 서버 OOM 방지). 자정 경계에서 에피소드가
분할되는 근사가 있으나 통계적으로 무시 가능.

산출:
  1. 코인×거래소×금액대별 기회 에피소드: 횟수, 지속시간(중앙값/p90), 평균·최대 순엣지
     → T3 유니버스 선정과 엣지 반감기(= 얼마나 빨라야 하는가, §1.4 속도 예산)의 근거
  2. 진입 후 실행김프 변화 분포 (+5/15/30분) → naked(O1) 실비용·함정 리스크 정량화
  3. 가상 수확 시뮬레이션 (실거래 아님) — 진입 시점 지갑 상태 확인분만, 함정 김프 배제

사용:
  python scripts/analyze_premium.py --thr 0.005 [--coin XRP] [--since 2026-08-14]

서버 보호 실행(권장 — 분석이 폭주해도 박스가 아니라 분석만 죽는다):
  sudo systemd-run --scope -p MemoryMax=4G -p MemorySwapMax=0 --uid=kimp \\
    bash -c 'cd /opt/kimp/app && nice -n 10 /opt/kimp/venv/bin/python scripts/analyze_premium.py --thr 0.005'
"""
from __future__ import annotations

import argparse
import gc
import os
import sys
from datetime import date
from pathlib import Path

# 공유 서버 보호: 병렬 파일 디코드의 순간 메모리 폭을 제한 (import 전에 설정해야 적용됨)
os.environ.setdefault("POLARS_MAX_THREADS", "2")

try:
    import polars as pl
except ImportError:
    sys.exit("polars가 필요합니다: pip install polars")

HORIZONS_MIN = (5, 15, 30)
COLS = [
    "ts", "coin", "dom_ex", "notional_usd", "in_net", "out_net", "exec_mid",
    "in_capacity_usd", "out_capacity_usd",
]


def day_partitions(data_root: str, since: date | None) -> list[tuple[date, Path]]:
    parts: list[tuple[date, Path]] = []
    for p in sorted(Path(data_root, "premium").glob("date=*")):
        try:
            d = date.fromisoformat(p.name.split("=", 1)[1])
        except ValueError:
            continue
        if since and d < since:
            continue
        parts.append((d, p))
    return parts


F32_COLS = ["in_net", "out_net", "exec_mid", "in_capacity_usd", "out_capacity_usd"]


def load_day(part_dir: Path, coin: str | None) -> pl.DataFrame:
    lf = pl.scan_parquet(str(part_dir / "*.parquet")).select(COLS)
    if coin:
        lf = lf.filter(pl.col("coin") == coin)
    # 메모리 다이어트: 문자열 → Categorical(정수 코드), 지표 → f32 — 12M행 파티션이 ~1/3로
    # (main()의 enable_string_cache 전제 — 일 경계·wallet 조인에서 카테고리 호환)
    return (
        lf.collect()
        .with_columns(
            pl.col("coin").cast(pl.Categorical),
            pl.col("dom_ex").cast(pl.Categorical),
            *(pl.col(c).cast(pl.Float32) for c in F32_COLS),
        )
        .sort("ts")
    )


def load_wallet(data_root: str) -> pl.DataFrame | None:
    """입출금 상태 이력 (소량) — 가상 수확의 함정 배제용."""
    try:
        w = (
            pl.scan_parquet(f"{data_root}/wallet/**/*.parquet")
            .select(["ts_local", "exchange", "coin", "deposit_ok", "withdraw_ok"])
            .collect()
        )
        if w.is_empty():
            return None
        return (
            w.rename({"ts_local": "wts", "exchange": "dom_ex"})
            .with_columns(pl.col("coin").cast(pl.Categorical), pl.col("dom_ex").cast(pl.Categorical))
            .sort("wts")
        )
    except Exception:
        return None


def episodes(df: pl.DataFrame, edge_col: str, thr: float, gap_ms: int = 60_000) -> pl.DataFrame:
    """(coin, dom_ex, notional_usd)별로 엣지≥thr 연속 구간을 에피소드로 묶는다.

    gap_ms는 저장 게이트의 하트비트(30s)보다 커야 한다 — 작으면 지속 기회 하나가
    수백 개의 1틱 에피소드로 조각나 통계·메모리가 모두 깨진다 (2026-08-23 실측 버그)."""
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


def persistence_samples(df: pl.DataFrame, ep: pl.DataFrame) -> pl.DataFrame:
    """에피소드 진입 대비 +N분 실행김프 변화 샘플 [delta_pp, horizon_min] — 일 청크 내에서 산출."""
    base = df.select(["ts", "coin", "dom_ex", "notional_usd", "exec_mid"]).drop_nulls("exec_mid")
    entries = ep.select(["coin", "dom_ex", "notional_usd", "start"]).join(
        base.rename({"ts": "start", "exec_mid": "exec_at_entry"}),
        on=["coin", "dom_ex", "notional_usd", "start"],
        how="inner",
    )
    if len(entries) > 50_000:  # 메모리 안전벨트 — 통계에는 5만 샘플이면 충분
        entries = entries.head(50_000)
    out = []
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
        out.append(
            joined.select(
                ((pl.col("exec_at_h") - pl.col("exec_at_entry")) * 100).alias("delta_pp"),
                pl.lit(h).alias("horizon_min"),
            )
        )
    return pl.concat(out) if out else pl.DataFrame([])


def harvest_cycles(
    df: pl.DataFrame,
    ep: pl.DataFrame,
    direction: str,
    wallet: pl.DataFrame,
    cycle_cap: float,
    cooldown_ms: int,
    last_entry: dict[tuple, int],
    max_edge: float,
    suspects: list[dict],
) -> list[dict]:
    """에피소드 → 가상 사이클 목록. 진입 시점 지갑 상태 as-of 결합 (미확인 제외, T4-①).

    지속 에피소드는 쿨다운 주기마다 재진입을 생성한다 — 2시간 열려 있는 기회는
    사이클을 여러 번 돈다 (첫 진입은 실측 엣지, 재진입은 에피소드 평균 엣지로 근사).
    last_entry(쿨다운 상태)는 일 경계를 넘어 유지."""
    cap_col = f"{direction}_capacity_usd"
    net_col = f"{direction}_net"
    starts = (
        ep.select(["coin", "dom_ex", "notional_usd", "start", "duration_s", "mean"])
        .join(
            df.select(["ts", "coin", "dom_ex", "notional_usd", net_col, cap_col]).rename({"ts": "start"}),
            on=["coin", "dom_ex", "notional_usd", "start"],
            how="inner",
        )
        .sort("start")
    )
    if starts.is_empty():
        return []
    starts = starts.join_asof(
        wallet, left_on="start", right_on="wts", by=["dom_ex", "coin"], strategy="backward"
    )
    flag = "deposit_ok" if direction == "in" else "withdraw_ok"
    starts = starts.filter(pl.col(flag) == True).sort("start")  # noqa: E712 — null(미확인)도 제외

    rows: list[dict] = []
    for r in starts.iter_rows(named=True):
        cap, entry_edge = r.get(cap_col), r.get(net_col)
        if cap is None or entry_edge is None or cap <= 0:
            continue
        key = (r["coin"], r["dom_ex"])
        notional = min(cap, cycle_cap)
        duration_ms = int((r.get("duration_s") or 0) * 1000)
        mean_edge = r.get("mean") or entry_edge
        # 엣지 상한 초과 = 티커 충돌/해외측 전송불가 의심 (V7) — 집계 대신 요주의 목록으로.
        # 진짜 김프는 재정거래로 눌리므로 초대형 엣지가 '지속'되는 것 자체가 전송 불가의 증거
        target = suspects if entry_edge > max_edge or mean_edge > max_edge else rows
        t = r["start"]
        first = True
        while t <= r["start"] + duration_ms:
            if key not in last_entry or t - last_entry[key] >= cooldown_ms:
                last_entry[key] = t
                edge = entry_edge if first else mean_edge  # 재진입은 평균 엣지 근사
                target.append(
                    {"coin": r["coin"], "dom_ex": r["dom_ex"], "start": t,
                     "notional_usd": notional, "edge": edge, "profit_usd": notional * edge}
                )
            first = False
            t += cooldown_ms
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="data")
    ap.add_argument("--thr", type=float, default=0.005, help="순엣지 임계 (기본 0.5%%)")
    ap.add_argument("--coin", default=None)
    ap.add_argument("--since", default=None, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--cycle-cap", type=float, default=2000, help="가상 사이클당 명목 상한 USD (T13 제안치)")
    ap.add_argument("--cycle-minutes", type=float, default=30, help="코인·거래소별 재진입 쿨다운")
    ap.add_argument("--capital", type=float, default=30000, help="수익률 환산 기준 자본 USD")
    ap.add_argument("--gap-seconds", type=float, default=60, help="에피소드 병합 간격 (저장 하트비트 30s보다 커야 함)")
    ap.add_argument("--max-edge", type=float, default=0.05,
                    help="가상 수확 엣지 상한 — 초과분은 티커충돌/전송불가 의심으로 제외·별도 보고 (V7)")
    args = ap.parse_args()

    pl.enable_string_cache()  # Categorical을 일 경계·wallet 조인·concat에서 호환시키는 전역 캐시

    since = date.fromisoformat(args.since) if args.since else None
    parts = day_partitions(args.data, since)
    if not parts:
        sys.exit(f"데이터 없음: {args.data}/premium (since={args.since})")

    wallet = load_wallet(args.data)
    if wallet is None:
        print("(주의: 지갑 상태 데이터 없음 — 가상 수확은 함정 판정 불가로 생략됨)")

    directions = (
        ("in", "in_net", "INBOUND (김프 방향)", "IN (naked — 전송 중 김프 변동 미반영, persistence가 오차 범위)"),
        ("out", "out_net", "OUTBOUND (역프 방향)", "OUT (헤지 시 진입가 락 — 가장 신뢰 가능한 추정)"),
    )
    ep_all: dict[str, list] = {"in": [], "out": []}
    pers_all: dict[str, list] = {"in": [], "out": []}
    harvest_rows: dict[str, list] = {"in": [], "out": []}
    suspect_rows: dict[str, list] = {"in": [], "out": []}
    last_entry: dict[str, dict] = {"in": {}, "out": {}}
    gap_ms = int(args.cycle_minutes * 60_000)
    total_rows = 0
    ts_min = ts_max = None
    base_rung: float | None = None

    for d, p in parts:
        day = load_day(p, args.coin)
        if day.is_empty():
            continue
        total_rows += len(day)
        ts_min = day["ts"].min() if ts_min is None else min(ts_min, day["ts"].min())
        ts_max = day["ts"].max() if ts_max is None else max(ts_max, day["ts"].max())
        if base_rung is None:
            base_rung = float(day["notional_usd"].min())
        print(f"  [{d}] {len(day):,} rows", file=sys.stderr)
        for dkey, col, *_ in directions:
            ep = episodes(day, col, args.thr, int(args.gap_seconds * 1000))
            if ep.is_empty():
                continue
            ep_all[dkey].append(ep)
            ps = persistence_samples(day, ep)
            if not ps.is_empty():
                pers_all[dkey].append(ps)
            if wallet is not None:
                harvest_rows[dkey].extend(
                    harvest_cycles(
                        day, ep.filter(pl.col("notional_usd") == base_rung), dkey, wallet,
                        args.cycle_cap, gap_ms, last_entry[dkey],
                        args.max_edge, suspect_rows[dkey],
                    )
                )
        del day  # 일 청크 메모리 즉시 반환
        gc.collect()

    span_days = max(((ts_max or 0) - (ts_min or 0)) / 86_400_000, 1e-9)
    print(f"# premium ticks: {total_rows:,} rows, {span_days:.1f}일, 파티션 {len(parts)}개 (일 청크 처리)")

    harvest_total = 0.0
    for dkey, _col, section, hlabel in directions:
        print(f"\n## {section} — 순엣지 ≥ {args.thr*100:.2f}% 에피소드 (자정 경계 분할 근사)")
        if not ep_all[dkey]:
            print("  (없음)")
            continue
        ep_cat = pl.concat(ep_all[dkey])
        with pl.Config(tbl_rows=args.top, tbl_width_chars=140):
            print(episode_summary(ep_cat))
        if pers_all[dkey]:
            q = (
                pl.concat(pers_all[dkey])
                .group_by("horizon_min")
                .agg(
                    p10_pp=pl.col("delta_pp").quantile(0.1),
                    p50_pp=pl.col("delta_pp").quantile(0.5),
                    p90_pp=pl.col("delta_pp").quantile(0.9),
                    n=pl.len(),
                )
                .sort("horizon_min")
            )
            print("  진입 후 실행김프 변화 (pp, 전송창 프록시 — naked 리스크·함정 판단 근거):")
            print(q)

        print(f"\n### ⚠️ 가상 수확 시뮬레이션 — 실거래 아님 (사이클 ${args.cycle_cap:,.0f}·쿨다운 {args.cycle_minutes:.0f}분)")
        rows = harvest_rows[dkey]
        if not rows:
            print(f"  {hlabel}: 가상 사이클 없음 (지갑 게이트 통과분 기준)")
            continue
        h = pl.DataFrame(rows)
        total = float(h["profit_usd"].sum())
        harvest_total += total
        print(
            f"  {hlabel}: 가상 사이클 {len(h)}건, 평균 엣지 {float(h['edge'].mean())*100:.2f}%, "
            f"평균 명목 ${float(h['notional_usd'].mean()):,.0f}"
        )
        print(
            f"    → 총 가상손익 ${total:,.0f} ({span_days:.1f}일) ≈ ${total/span_days:,.0f}/일 "
            f"= 자본 ${args.capital:,.0f} 대비 일 {total/span_days/args.capital*100:.2f}%"
        )
        top = (
            h.group_by("coin").agg(profit=pl.col("profit_usd").sum(), cycles=pl.len())
            .sort("profit", descending=True).head(10)
        )
        with pl.Config(tbl_rows=10):
            print(top)
        if suspect_rows[dkey]:
            s = pl.DataFrame(suspect_rows[dkey])
            s_top = (
                s.group_by("coin").agg(would_be_profit=pl.col("profit_usd").sum(), cycles=pl.len(),
                                       max_edge_pct=(pl.col("edge").max() * 100))
                .sort("would_be_profit", descending=True).head(10)
            )
            print(
                f"  🚨 요주의 — 엣지 >{args.max_edge*100:.0f}% 지속으로 집계 제외 "
                f"(티커 충돌/전송불가 의심 = V7 검증 대상, 총 ${float(s['profit_usd'].sum()):,.0f}어치 '가짜 기회'):"
            )
            with pl.Config(tbl_rows=10):
                print(s_top)

    if harvest_total:
        print(
            f"\n# 합계(가상): ${harvest_total:,.0f} / {span_days:.1f}일 ≈ "
            f"${harvest_total/span_days:,.0f}/일, 자본 대비 일 {harvest_total/span_days/args.capital*100:.2f}% "
            f"— 진입 시점 지갑 상태 확인분만 집계(미확인 제외), 체결·전송 실패 미반영"
        )


if __name__ == "__main__":
    main()
