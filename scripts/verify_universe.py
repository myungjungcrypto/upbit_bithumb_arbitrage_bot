#!/usr/bin/env python3
"""V7 자산 동일성 검증 — 티커가 아니라 정식 자산 ID로 대조 (T3 하드 게이트 ⑧).

원리: 티커는 거래소마다 다른 자산을 가리킬 수 있다 (실사례: 업비트 AI=젠신 vs
바이낸스 AI=Sleepless AI, 2026-08-28 확인). CoinGecko는 거래소 마켓을 컨트랙트 기준
정식 자산에 매핑하므로, 양쪽 마켓의 CoinGecko ID를 비교하면 컨트랙트 대조와 동등하다
(네이티브 코인까지 커버). 추가로 네트워크 교집합(전송 가능성)도 검사한다.

판정:
  OK            — 동일 자산 (+네트워크 겹침 확인 시 명시)
  MISMATCH      — 다른 자산 (티커 충돌) → trade_blocklist 대상
  UNKNOWN       — 매핑 실패 → 보수적으로 blocklist 권고
  NO_NET_OVERLAP— 동일 자산이나 전송 네트워크 불일치 → blocklist 대상

사용 (서버, ~2분 소요 — CoinGecko 무료 티어 레이트리밋 준수):
  sudo -u kimp bash -c 'set -a; . /opt/kimp/env; set +a; cd /opt/kimp/app && /opt/kimp/venv/bin/python scripts/verify_universe.py'
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
CG = "https://api.coingecko.com/api/v3"

# 네트워크 명칭 정규화 (업비트 net_type ↔ 바이낸스 network)
NET_ALIAS = {
    "ERC20": "ETH", "ETHEREUM": "ETH",
    "BEP20": "BSC", "BEP20(BSC)": "BSC", "BNB SMART CHAIN": "BSC", "BNB SMART CHAIN(BEP20)": "BSC",
    "TRC20": "TRX", "TRON": "TRX",
    "SOLANA": "SOL", "SPL": "SOL",
    "POLYGON": "MATIC", "POLYGON POS": "MATIC",
    "ARBITRUM ONE": "ARBITRUM", "ARB": "ARBITRUM",
}


def get(url: str, headers: dict | None = None):
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def cg_exchange_map(exchange_id: str, target: str, max_pages: int = 25) -> dict[str, set[str]]:
    """CoinGecko 거래소 티커 → {베이스티커: {coingecko_id, ...}}. 페이지당 3초 대기 (무료 티어)."""
    out: dict[str, set[str]] = {}
    for page in range(1, max_pages + 1):
        try:
            data = get(f"{CG}/exchanges/{exchange_id}/tickers?page={page}")
        except Exception as e:
            print(f"  (경고: {exchange_id} p{page} 조회 실패 {e!r} — 여기까지 수집분으로 진행)", file=sys.stderr)
            break
        tickers = data.get("tickers") or []
        if not tickers:
            break
        for t in tickers:
            if t.get("target") == target:
                out.setdefault(str(t.get("base", "")).upper(), set()).add(t.get("coin_id", ""))
        print(f"  [{exchange_id}] page {page}: 누적 {len(out)} 심볼", file=sys.stderr)
        time.sleep(3)
    return out


def norm_net(name: str) -> str:
    u = str(name).upper().strip()
    return NET_ALIAS.get(u, u)


def upbit_networks() -> dict[str, set[str]]:
    """업비트 코인별 입금 네트워크 (net_type) — 조회 전용 키 필요, 없으면 빈 dict."""
    ak, sk = os.environ.get("UPBIT_ACCESS_KEY", ""), os.environ.get("UPBIT_SECRET_KEY", "")
    if not (ak and sk):
        return {}
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from kimp.collectors.wallet_upbit import make_jwt  # stdlib JWT 재사용

    data = get("https://api.upbit.com/v1/status/wallet", {"Authorization": f"Bearer {make_jwt(ak, sk)}"})
    out: dict[str, set[str]] = {}
    for row in data if isinstance(data, list) else []:
        cur, net = row.get("currency"), row.get("net_type")
        if cur and net:
            out.setdefault(cur.upper(), set()).add(norm_net(net))
    return out


def binance_networks() -> dict[str, set[str]]:
    """바이낸스 코인별 출금 가능 네트워크 — 조회 전용 키 필요, 없으면 빈 dict."""
    ak, sk = os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", "")
    if not (ak and sk):
        return {}
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from kimp.collectors.wallet_binance import sign_query

    qs = sign_query(sk, {"timestamp": int(time.time() * 1000), "recvWindow": 10000})
    data = get(f"https://api.binance.com/sapi/v1/capital/config/getall?{qs}", {"X-MBX-APIKEY": ak})
    out: dict[str, set[str]] = {}
    for row in data if isinstance(data, list) else []:
        coin = row.get("coin")
        for n in row.get("networkList") or []:
            if coin and n.get("withdrawEnable"):
                out.setdefault(coin.upper(), set()).add(norm_net(n.get("network", "")))
    return out


def main() -> None:
    print("1/3 CoinGecko 매핑 수집 중 (업비트 KRW · 바이낸스 USDT)...", file=sys.stderr)
    up = cg_exchange_map("upbit", "KRW")
    bn = cg_exchange_map("binance", "USDT")
    print("2/3 네트워크 정보 수집 중 (키 있으면)...", file=sys.stderr)
    up_nets, bn_nets = upbit_networks(), binance_networks()
    net_check = bool(up_nets and bn_nets)

    coins = sorted(set(up) & set(bn))
    print(f"3/3 판정 — 교집합 {len(coins)}개 심볼 (네트워크 검사: {'ON' if net_check else 'OFF — 키 없음'})\n",
          file=sys.stderr)

    blocklist: list[str] = []
    print(f"{'코인':8} {'판정':16} 상세")
    for c in coins:
        u_ids, b_ids = up[c] - {""}, bn[c] - {""}
        if not u_ids or not b_ids:
            verdict, detail = "UNKNOWN", f"매핑 실패 (upbit={u_ids or '-'}, binance={b_ids or '-'})"
            blocklist.append(c)
        elif u_ids & b_ids:
            if net_check and c in up_nets and c in bn_nets and not (up_nets[c] & bn_nets[c]):
                verdict, detail = "NO_NET_OVERLAP", f"동일 자산이나 네트워크 불일치 (upbit={sorted(up_nets[c])}, binance={sorted(bn_nets[c])})"
                blocklist.append(c)
            else:
                verdict, detail = "OK", f"id={sorted(u_ids & b_ids)[0]}"
        else:
            verdict, detail = "MISMATCH", f"충돌! upbit={sorted(u_ids)} vs binance={sorted(b_ids)}"
            blocklist.append(c)
        if verdict != "OK":
            print(f"{c:8} {verdict:16} {detail}")
    print(f"\nOK {len(coins) - len(blocklist)} / 차단 권고 {len(blocklist)}")
    print("\nconfig/default.yaml에 반영할 값:")
    print(f"  trade_blocklist: [{', '.join(sorted(blocklist))}]")


if __name__ == "__main__":
    main()
