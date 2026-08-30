#!/usr/bin/env python3
"""페이퍼 트레이딩 성적표 — cycles.db 원장 요약 (stdlib only, 언제든 즉시 실행).

사용: /opt/kimp/venv/bin/python scripts/paper_report.py [--db data/cycles.db]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

OPEN_STATES = ("SIGNAL", "ENTERED", "IN_FLIGHT", "ARRIVED")


def num(v) -> float:
    if isinstance(v, str) and v.startswith("D:"):
        return float(v[2:])
    return float(v or 0)


def ts_str(ms) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")


def load_blocklist(config_path: str) -> set[str]:
    try:
        import yaml

        cfg = yaml.safe_load(open(config_path))
        return {s.upper() for s in cfg.get("universe", {}).get("trade_blocklist", [])}
    except Exception:
        return set()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/cycles.db")
    ap.add_argument("--config", default="config/default.yaml")
    args = ap.parse_args()

    rows = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True).execute(
        "SELECT body FROM cycles"
    ).fetchall()
    cycles = [json.loads(r[0]) for r in rows]
    if not cycles:
        raise SystemExit("사이클 없음 — 페이퍼가 아직 진입 전이거나 DB 경로 확인")

    # 현행 차단 목록 기준으로 과거 정산분을 소급 분류 — 차단 배포 전 진입분(전송 불가 판정)
    # 은 '실현 불가능했던 손익'이므로 청정 집계에서 제외
    bl = load_blocklist(args.config)

    def blocked(c) -> bool:
        # 레그 문법 (kimp.symbols.leg_blocked와 동일 — 이 스크립트는 stdlib 단독 실행 유지)
        coin, dom = c["coin"].upper(), c["dom_ex"].upper()
        ovs = (c.get("ovs_ex") or "binance").upper()
        return coin in bl or f"{coin}@{dom}" in bl or f"{coin}>{ovs}" in bl or f"{coin}@{dom}>{ovs}" in bl

    all_settled = [c for c in cycles if c["state"] in ("SETTLED", "SETTLED_STUCK")]
    tainted = [c for c in all_settled if blocked(c)]
    settled = [c for c in all_settled if not blocked(c)]
    if tainted:
        tp = sum(c.get("pnl_usd") or 0 for c in tainted)
        print(f"⚠️ 차단 레그의 과거 정산분 {len(tainted)}건 ${tp:+,.2f} — 전송 불가 판정 이전 진입분, 아래 집계에서 제외됨")
    open_ = [c for c in cycles if c["state"] in OPEN_STATES]
    void = [c for c in cycles if c["state"] == "VOID"]

    print(f"# 페이퍼 성적표 (UTC {datetime.now(timezone.utc):%m-%d %H:%M}) — 실거래 아님")
    print(f"사이클: 정산 {len(settled)} / 진행 {len(open_)} / 무효 {len(void)}")

    if settled:
        pnl = sum(c.get("pnl_usd") or 0 for c in settled)
        notional = sum(num(c["notional_usd"]) for c in settled)
        wins = sum(1 for c in settled if (c.get("pnl_usd") or 0) > 0)
        stuck = sum(1 for c in settled if c["state"] == "SETTLED_STUCK")
        print(f"\n총손익 ${pnl:,.2f} · 투입누적 ${notional:,.0f} · 사이클당 평균 {pnl/notional*100:.3f}%"
              f" · 승률 {wins}/{len(settled)} ({wins/len(settled)*100:.0f}%)"
              + (f" · STUCK {stuck}건" if stuck else ""))

        drifts = [
            (c["pnl_usd"] / num(c["notional_usd"]) - c["entry_edge"]) * 100
            for c in settled
            if num(c["notional_usd"]) > 0 and c.get("entry_edge") is not None and c.get("pnl_usd") is not None
        ]
        if drifts:
            drifts.sort()
            mid = drifts[len(drifts) // 2]
            print(f"드리프트(실현−기대, %p): 중앙값 {mid:+.3f} · 최악 {drifts[0]:+.3f} · 최선 {drifts[-1]:+.3f}"
                  f"  ← naked 리스크·슬리피지 갭의 실측")

        by_day: dict[str, float] = defaultdict(float)
        for c in settled:
            ts = c["stamps"].get("SETTLED") or c["stamps"].get("SETTLED_STUCK") or 0
            by_day[datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()] += c.get("pnl_usd") or 0
        print("\n일별 손익:")
        for d in sorted(by_day):
            print(f"  {d}  ${by_day[d]:+,.2f}")

        agg: dict[tuple, list] = defaultdict(lambda: [0, 0.0])
        for c in settled:
            k = (c["coin"], c["kind"], c["dom_ex"], c.get("ovs_ex") or "binance")
            agg[k][0] += 1
            agg[k][1] += c.get("pnl_usd") or 0
        print("\n코인×방향×레그 상위 (손익순) — M3 라이브 거래소 선정 근거:")
        for (coin, kind, dom, ovs), (n, p) in sorted(agg.items(), key=lambda kv: -kv[1][1])[:12]:
            print(f"  {coin:8} {kind.upper():3} {dom:8}↔{ovs:8} {n:3}건  ${p:+,.2f}")

    if open_:
        print("\n진행 중:")
        now = datetime.now(timezone.utc).timestamp() * 1000
        for c in sorted(open_, key=lambda c: c.get("arrival_at_ms") or 0):
            eta = ((c.get("arrival_at_ms") or now) - now) / 60000
            print(f"  {c['coin']:8} {c['kind'].upper():3} {c['dom_ex']:8}↔{c.get('ovs_ex') or 'binance':8} ${num(c['notional_usd']):,.0f}"
                  f" @기대 {(c.get('entry_edge') or 0)*100:.2f}%  도착 {'지남' if eta <= 0 else f'{eta:.0f}분 후'}"
                  f"{' (헤지락)' if c.get('hedged') else ' (naked)'}")


if __name__ == "__main__":
    main()
