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

사용 (서버, 첫 실행 ~5-10분 — CoinGecko 무료 티어 분당 ~5회 준수, 이후 24h 캐시로 즉시):
  sudo -u kimp bash -c 'set -a; . /opt/kimp/env; set +a; cd /opt/kimp/app && /opt/kimp/venv/bin/python scripts/verify_universe.py'
선택: COINGECKO_API_KEY 환경변수(무료 데모 키)를 넣으면 분당 30회로 빨라짐.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
CG = "https://api.coingecko.com/api/v3"

# 네트워크 명칭 정규화 (업비트 net_type ↔ 바이낸스 network).
# 2026-08-28 실측 반영: 같은 체인의 거래소별 표기 차이는 합치되,
# 체인 마이그레이션·주소체계 차이(NEO/NEO3, CHZ/CHZ2, A/EOS, SEI/SEIEVM, MTL/METAL,
# LISK/ETH, ZIL/BSC 등)는 **의도적으로 합치지 않음** — 실제 전송 불가 위험이라 차단 유지.
NET_ALIAS = {
    "ERC20": "ETH", "ETHEREUM": "ETH",
    "BEP20": "BSC", "BEP20(BSC)": "BSC", "BNB SMART CHAIN": "BSC", "BNB SMART CHAIN(BEP20)": "BSC",
    "TRC20": "TRX", "TRON": "TRX",
    "SOLANA": "SOL", "SPL": "SOL",
    "POLYGON": "MATIC", "POLYGON POS": "MATIC", "POL": "MATIC",
    "ARBITRUM ONE": "ARBITRUM", "ARB": "ARBITRUM",
    "BASENET": "BASE",           # 업비트 표기 → Base
    "LINEANET": "LINEA",
    "ZKSYNCERA": "ZKSYNC",       # zkSync Era 표기 통일
    "WAXP": "WAX",
    "AVAXC": "AVAX", "AVAX C-CHAIN": "AVAX",
    "XPL": "PLASMA",
    "OPTIMISM": "OP",
}


def get(url: str, headers: dict | None = None, retries: int = 6):
    """429는 Retry-After(기본 60초) 대기 후 재시도 — 무료 티어에서 중단 대신 완주."""
    h = {**UA, **(headers or {})}
    key = os.environ.get("COINGECKO_API_KEY", "")
    if key and "coingecko" in url:
        h["x-cg-demo-api-key"] = key
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and i < retries - 1:
                wait = int(e.headers.get("Retry-After") or 0) or 60
                print(f"  (레이트리밋 — {wait}초 대기 후 재시도 {i+1}/{retries-1})", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("rate limit persists")


def cg_exchange_map(exchange_id: str, target: str, max_pages: int = 25) -> dict[str, set[str]]:
    """CoinGecko 거래소 티커 → {베이스티커: {coingecko_id, ...}}.

    무료 티어(분당 ~5회) 준수: 페이지당 10초 + 429 백오프. 결과는 24시간 파일 캐시
    (data/cg_cache_*.json) — 재실행·부분 실패 시 재수집 비용 제거."""
    cache = Path("data") / f"cg_cache_{exchange_id}_{target}.json"
    if cache.exists() and time.time() - cache.stat().st_mtime < 86400:
        raw = json.loads(cache.read_text())
        print(f"  [{exchange_id}] 캐시 사용 ({len(raw)} 심볼, {cache})", file=sys.stderr)
        return {k: set(v) for k, v in raw.items()}

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
        time.sleep(10)
    if out:
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps({k: sorted(v) for k, v in out.items()}))
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


def bithumb_networks() -> dict[str, set[str]]:
    """빗썸 코인별 네트워크 (신 API v1/status/wallet, 조회 전용 키) — 키 없으면 빈 dict."""
    ak, sk = os.environ.get("BITHUMB_API_KEY", ""), os.environ.get("BITHUMB_API_SECRET", "")
    if not (ak and sk):
        return {}
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from kimp.collectors.wallet_bithumb import make_bithumb_jwt

    try:
        data = get("https://api.bithumb.com/v1/status/wallet",
                   {"Authorization": f"Bearer {make_bithumb_jwt(ak, sk)}"})
    except Exception as e:
        print(f"  (경고: 빗썸 v1 지갑 조회 실패 {e!r} — 빗썸 레그 네트워크 검사 생략)", file=sys.stderr)
        return {}
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
    up_nets, bt_nets, bn_nets = upbit_networks(), bithumb_networks(), binance_networks()

    coins = sorted(set(up) & set(bn))
    print(
        f"3/3 판정 — 교집합 {len(coins)}개 심볼 "
        f"(네트워크 검사: 업비트 {'ON' if up_nets and bn_nets else 'OFF'} / 빗썸 {'ON' if bt_nets and bn_nets else 'OFF'})\n",
        file=sys.stderr,
    )

    blocklist: list[str] = []
    ok_coins: list[str] = []
    print(f"{'코인':8} {'판정':16} 상세")
    for c in coins:
        u_ids, b_ids = up[c] - {""}, bn[c] - {""}
        if not u_ids or not b_ids:
            print(f"{c:8} {'UNKNOWN':16} 매핑 실패 (upbit={u_ids or '-'}, binance={b_ids or '-'})")
            blocklist.append(c)
            continue
        if not (u_ids & b_ids):
            print(f"{c:8} {'MISMATCH':16} 충돌! upbit={sorted(u_ids)} vs binance={sorted(b_ids)}")
            blocklist.append(c)
            continue
        # 자산 동일 — 국내 레그별 네트워크 겹침 검사 (검사 불가 레그는 통과로 두되 표기)
        leg_blocks = []
        for dom, nets in (("upbit", up_nets), ("bithumb", bt_nets)):
            if nets and bn_nets and c in nets and c in bn_nets and not (nets[c] & bn_nets[c]):
                leg_blocks.append(dom)
                print(f"{c:8} {'NO_NET_OVERLAP':16} {dom} 레그 — {dom}={sorted(nets[c])} vs binance={sorted(bn_nets[c])}")
        if len(leg_blocks) >= 2:
            blocklist.append(c)               # 양쪽 다 막힘 → 전역 차단
        else:
            blocklist.extend(f"{c}@{d}" for d in leg_blocks)  # 한쪽만 → 레그 차단
            ok_coins.append(c)                # 최소 한 레그는 거래 가능 → allowlist 포함

    print(f"\nOK {len(ok_coins)} / 차단 권고 {len(blocklist)} (전역+레그)")
    print("\nconfig/default.yaml에 반영할 값:")
    print(f"  trade_blocklist: [{', '.join(sorted(blocklist))}]")

    # allowlist 파일 — 페이퍼/실행 엔진이 자동 사용: 여기 없는 코인(=미검증·신규 상장)은 거래 차단
    out_path = Path("data") / "verified_ok.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"generated_ms": int(time.time() * 1000), "ok": sorted(ok_coins)}))
    print(f"\nallowlist 저장: {out_path} ({len(ok_coins)}종) — 재시작 시 엔진이 자동 적용, 신규 상장은 재실행 전까지 차단")


if __name__ == "__main__":
    main()
