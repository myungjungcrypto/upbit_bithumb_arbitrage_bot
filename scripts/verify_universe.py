#!/usr/bin/env python3
"""V7 자산 동일성 검증 — 티커가 아니라 정식 자산 ID로 (국내×해외) 레그별 대조 (M2).

원리: 티커는 거래소마다 다른 자산을 가리킬 수 있다 (실사례: 업비트 AI=젠신 vs
바이낸스 AI=Sleepless AI, 2026-08-28 확인). CoinGecko는 거래소 마켓을 컨트랙트 기준
정식 자산에 매핑하므로, 양쪽 마켓의 CoinGecko ID를 비교하면 컨트랙트 대조와 동등하다
(네이티브 코인까지 커버). 추가로 네트워크 교집합(전송 가능성)도 레그별로 검사한다.

M2 확장: 해외가 바낸 단독 → 바낸·바이비트·OKX. 판정 단위가 코인 → (코인, 국내, 해외)
레그로 바뀌고, 산출물 verified_ok.json이 레그 단위 v2 포맷("legs")이 된다.
바낸에선 다른 자산인 티커가 OKX에선 같은 자산일 수 있다 — 레그별로만 열고 닫는다.

레그 판정:
  OK            — 동일 자산 (+네트워크 겹침 확인 시)  → allowlist 등재
  MISMATCH      — 다른 자산 (티커 충돌)               → 그 레그 차단
  UNKNOWN       — 매핑 실패                           → 보수적으로 그 레그 차단
  NO_NET_OVERLAP— 동일 자산이나 전송 네트워크 불일치   → 그 레그 차단
  PROXY         — 빗썸 매핑 부재로 업비트 ID를 대용    → 통과시키되 표기 (구버전 동작 유지)

trade_blocklist 문법 (kimp.symbols.leg_blocked): "COIN" 전역 / "COIN@dom" 국내 레그 /
"COIN>ovs" 해외 레그 / "COIN@dom>ovs" 특정 조합.

사용 (서버, 첫 실행 ~15-20분 — CoinGecko 무료 티어 분당 ~5회 준수, 이후 24h 캐시로 즉시):
  sudo -u kimp bash -c 'set -a; . /opt/kimp/env; set +a; cd /opt/kimp/app && /opt/kimp/venv/bin/python scripts/verify_universe.py'
선택: COINGECKO_API_KEY 환경변수(무료 데모 키)를 넣으면 분당 30회로 빨라짐.
OKX/바이비트 읽기 전용 키가 .env에 있으면 해당 레그 네트워크 검사도 켜진다.
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
    "AVAXC": "AVAX", "AVAX C-CHAIN": "AVAX", "AVALANCHE C-CHAIN": "AVAX", "AVALANCHE C": "AVAX",
    "XPL": "PLASMA",
    "OPTIMISM": "OP",
    "ARBI": "ARBITRUM",          # 바이비트 표기
    # OKX chain 접미사는 네이티브 체인을 고유명으로 씀 ("BTC-Bitcoin" → BITCOIN) — 티커로 정규화
    # (2026-08-30 리뷰: 별칭 없으면 업비트/바낸의 "BTC" 표기와 영영 안 겹쳐 정상 레그가 오차단됨)
    "BITCOIN": "BTC", "RIPPLE": "XRP", "LITECOIN": "LTC", "DOGECOIN": "DOGE",
    "CARDANO": "ADA", "POLKADOT": "DOT", "BITCOIN CASH": "BCH", "STELLAR": "XLM",
    "ALGORAND": "ALGO", "COSMOS": "ATOM", "HEDERA": "HBAR", "FILECOIN": "FIL",
    "TEZOS": "XTZ", "NEAR PROTOCOL": "NEAR", "TONCOIN": "TON", "APTOS": "APT", "KASPA": "KAS",
}

# CoinGecko 거래소 ID (오기 시 해당 거래소 매핑이 0건으로 나오고 경고 출력 — 레그는 UNKNOWN 차단)
DOM_CG = {"upbit": ("upbit", "KRW"), "bithumb": ("bithumb", "KRW")}
OVS_CG = {"binance": ("binance", "USDT"), "bybit": ("bybit_spot", "USDT"), "okx": ("okex", "USDT")}


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


def okx_networks() -> dict[str, set[str]]:
    """OKX 코인별 체인 (M2) — 3요소 키 필요, 없으면 빈 dict. "USDT-ERC20" → ERC20 정규화."""
    ak = os.environ.get("OKX_API_KEY", "")
    sk = os.environ.get("OKX_API_SECRET", "")
    pp = os.environ.get("OKX_API_PASSPHRASE", "")
    if not (ak and sk and pp):
        return {}
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from kimp.collectors.wallet_okx import okx_timestamp, sign_okx

    ts = okx_timestamp()
    path = "/api/v5/asset/currencies"
    try:
        data = get(f"https://www.okx.com{path}", {
            "OK-ACCESS-KEY": ak,
            "OK-ACCESS-SIGN": sign_okx(sk, ts, "GET", path),
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": pp,
        })
    except Exception as e:
        print(f"  (경고: OKX 통화 조회 실패 {e!r} — OKX 레그 네트워크 검사 생략)", file=sys.stderr)
        return {}
    out: dict[str, set[str]] = {}
    rows = data.get("data") if isinstance(data, dict) else None
    for row in rows if isinstance(rows, list) else []:
        ccy, chain = row.get("ccy"), str(row.get("chain") or "")
        net = chain.split("-", 1)[1] if "-" in chain else chain
        if ccy and net:
            out.setdefault(str(ccy).upper(), set()).add(norm_net(net))
    return out


def bybit_networks() -> dict[str, set[str]]:
    """바이비트 코인별 체인 (M2) — 읽기 전용 키 필요, 없으면 빈 dict."""
    ak, sk = os.environ.get("BYBIT_API_KEY", ""), os.environ.get("BYBIT_API_SECRET", "")
    if not (ak and sk):
        return {}
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from kimp.collectors.wallet_bybit import sign_bybit

    ts = str(int(time.time() * 1000))
    try:
        data = get("https://api.bybit.com/v5/asset/coin/query-info", {
            "X-BAPI-API-KEY": ak, "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": "10000",
            "X-BAPI-SIGN": sign_bybit(sk, ts, ak, "10000", ""),
        })
    except Exception as e:
        print(f"  (경고: 바이비트 코인 조회 실패 {e!r} — 바이비트 레그 네트워크 검사 생략)", file=sys.stderr)
        return {}
    out: dict[str, set[str]] = {}
    result = (data.get("result") or {}) if isinstance(data, dict) else {}
    for row in result.get("rows") or []:
        coin = row.get("coin")
        for ch in row.get("chains") or []:
            net = ch.get("chain") or ch.get("chainType") or ""
            if coin and net:
                out.setdefault(str(coin).upper(), set()).add(norm_net(net))
    return out


def judge_legs(
    dom_maps: dict[str, dict[str, set[str]]],
    ovs_maps: dict[str, dict[str, set[str]]],
    dom_nets: dict[str, dict[str, set[str]]],
    ovs_nets: dict[str, dict[str, set[str]]],
    bithumb_listed: set[str] | None = None,
) -> tuple[dict[str, list[str]], set[str], list[str]]:
    """(코인, 국내, 해외) 레그별 판정 — 순수 함수 (테스트 대상).

    반환: (verified {코인: ["dom>ovs", ...]}, blocklist 항목 집합, 출력 라인들).
    빗썸 프록시(업비트 ID 대용)는 ①빗썸 CG 수집이 성공했고(맵 비어있지 않음) ②빗썸에
    실제 상장된 코인일 때만 — 수집 전체 실패나 미상장 코인으로 번지면 미검증 레그가
    allowlist에 실린다 (2026-08-30 리뷰 확정 결함 2건의 재발 방지 조건)."""
    coins = sorted(
        set().union(*[set(m) for m in dom_maps.values()] or [set()])
        & set().union(*[set(m) for m in ovs_maps.values()] or [set()])
    )
    verified: dict[str, list[str]] = {}
    blocklist: set[str] = set()
    lines: list[str] = []
    for c in coins:
        def can_proxy() -> bool:
            return bool(dom_maps.get("bithumb")) and bithumb_listed is not None and c in bithumb_listed

        def dom_ids(d: str) -> tuple[set[str], bool]:
            if c in dom_maps[d]:
                return dom_maps[d][c] - {""}, False  # 빈 집합 = 매핑 실패 → 레그 UNKNOWN 처리
            if d == "bithumb" and can_proxy():
                return dom_maps.get("upbit", {}).get(c, set()) - {""}, True
            return set(), False

        # 판정 대상 국내 레그: CG에 마켓이 있거나(빈 ID 포함 — UNKNOWN으로 남겨야 함) 프록시 조건 충족
        doms = [d for d in dom_maps if c in dom_maps[d] or (d == "bithumb" and can_proxy())]
        ovss = [o for o in ovs_maps if c in ovs_maps[o]]
        ok_legs: list[str] = []
        bad: set[tuple[str, str]] = set()
        for d in doms:
            d_ids, proxied = dom_ids(d)
            for o in ovss:
                o_ids = ovs_maps[o].get(c, set()) - {""}
                leg = f"{d}>{o}"
                if not d_ids or not o_ids:
                    bad.add((d, o))
                    lines.append(
                        f"{c:8} {'UNKNOWN':16} {leg} 매핑 실패 ({d}={sorted(d_ids) or '-'}, {o}={sorted(o_ids) or '-'})"
                    )
                    continue
                if not (d_ids & o_ids):
                    bad.add((d, o))
                    src = f"{d}(업비트 프록시)" if proxied else d
                    lines.append(f"{c:8} {'MISMATCH':16} {leg} 충돌! {src}={sorted(d_ids)} vs {o}={sorted(o_ids)}")
                    continue
                dn, on = dom_nets.get(d) or {}, ovs_nets.get(o) or {}
                if dn and on and c in dn and c in on and not (dn[c] & on[c]):
                    bad.add((d, o))
                    lines.append(f"{c:8} {'NO_NET_OVERLAP':16} {leg} — {d}={sorted(dn[c])} vs {o}={sorted(on[c])}")
                    continue
                if proxied:
                    lines.append(f"{c:8} {'PROXY':16} {leg} — 빗썸 CG 매핑 부재(상장 확인됨), 업비트 ID 대용으로 통과")
                ok_legs.append(leg)
        if not ok_legs:
            if doms and ovss:
                blocklist.add(c)  # 상장 레그 전멸 → 전역 차단
            continue
        verified[c] = sorted(ok_legs)
        # 실패 레그 → 최소 blocklist 항목으로 압축 (allowlist가 이미 정확 차단하지만 config에도 명시)
        for d in doms:
            if ovss and all((d, o) in bad for o in ovss):
                blocklist.add(f"{c}@{d}")
        for o in ovss:
            if doms and all((d, o) in bad for d in doms):
                blocklist.add(f"{c}>{o}")
        for d, o in bad:
            if f"{c}@{d}" not in blocklist and f"{c}>{o}" not in blocklist:
                blocklist.add(f"{c}@{d}>{o}")
    return verified, blocklist, lines


def compat_ok(verified: dict[str, list[str]], blocklist: set[str]) -> list[str]:
    """구버전 엔진 호환 "ok" 키 — **실패 레그가 하나도 없는 코인만**.

    구(pre-M2) 엔진은 "ok"를 코인 단위로 읽어 (국내×해외) 전 레그를 열고 "COIN>ovs"
    blocklist 문법도 모른다 — 부분 검증 코인을 실으면 롤백/부분 배포 시 MISMATCH 레그가
    열린다 (2026-08-30 리뷰 확정 결함). 항목이 코인을 참조하는지는 구분자 포함 접두로 판정."""
    return sorted(
        c for c in verified
        if not any(b == c or b.startswith(f"{c}@") or b.startswith(f"{c}>") for b in blocklist)
    )


def bithumb_listed_bases() -> set[str] | None:
    """빗썸 실제 상장 KRW 심볼 (public API, 키 불필요) — 프록시의 상장 근거.
    실패 시 None → 프록시 전면 비활성 (CG 매핑 있는 코인만 판정되는 보수 동작)."""
    try:
        data = get("https://api.bithumb.com/v1/market/all")
        return {
            str(m.get("market", "")).split("-", 1)[1]
            for m in (data if isinstance(data, list) else [])
            if str(m.get("market", "")).startswith("KRW-")
        }
    except Exception as e:
        print(f"  (경고: 빗썸 마켓 조회 실패 {e!r} — 빗썸 프록시 비활성, CG 매핑 있는 코인만 판정)", file=sys.stderr)
        return None


def main() -> None:
    print("1/3 CoinGecko 매핑 수집 중 — 국내 KRW 2곳 · 해외 USDT 3곳 (첫 실행 ~15-20분, 이후 24h 캐시)...",
          file=sys.stderr)
    dom_maps = {d: cg_exchange_map(cg_id, tgt) for d, (cg_id, tgt) in DOM_CG.items()}
    ovs_maps = {o: cg_exchange_map(cg_id, tgt) for o, (cg_id, tgt) in OVS_CG.items()}
    empty_maps = [name for name, m in [*dom_maps.items(), *ovs_maps.items()] if not m]
    for name in empty_maps:
        print(f"  (경고: {name} CoinGecko 매핑 0건 — 수집 불완전이면 기존 allowlist를 보존하고 저장하지 않음)", file=sys.stderr)

    print("2/3 네트워크 정보 수집 중 (키 있으면)...", file=sys.stderr)
    dom_nets = {"upbit": upbit_networks(), "bithumb": bithumb_networks()}
    ovs_nets = {"binance": binance_networks(), "bybit": bybit_networks(), "okx": okx_networks()}
    net_on = " ".join(f"{ex}:{'ON' if nets else 'OFF'}" for ex, nets in {**dom_nets, **ovs_nets}.items())
    listed = bithumb_listed_bases()

    verified, blocklist, lines = judge_legs(dom_maps, ovs_maps, dom_nets, ovs_nets, bithumb_listed=listed)
    n_cand = len(set().union(*[set(m) for m in dom_maps.values()] or [set()])
                 & set().union(*[set(m) for m in ovs_maps.values()] or [set()]))
    print(f"3/3 판정 — 후보 {n_cand}개 심볼, (국내 2 × 해외 3) 레그별 (네트워크 검사 {net_on})\n", file=sys.stderr)
    print(f"{'코인':8} {'판정':16} 상세")
    for ln in lines:
        print(ln)

    n_legs = sum(len(v) for v in verified.values())
    print(f"\nOK {len(verified)}종 / 검증 레그 {n_legs}개 / 차단 권고 {len(blocklist)}건 (전역+레그)")
    print("\nconfig/default.yaml에 반영할 값:")
    print(f"  trade_blocklist: [{', '.join(sorted(blocklist))}]")

    # 수집 불완전 시 저장 거부 — 빈 산출물이 기존 allowlist를 덮어쓰면 전 레그가 잠기고(fail-closed라 안전은
    # 하나) 열린 사이클까지 소급 무효되므로, 부분 실패는 '기존 파일 보존 + 재실행'이 올바른 동작 (2026-08-30 리뷰)
    if empty_maps or not verified:
        sys.exit(f"\n수집 불완전 (빈 매핑: {', '.join(empty_maps) or '-'} / 검증 {len(verified)}종) — "
                 f"verified_ok.json 저장 안 함. 네트워크·캐시 복구 후 재실행하세요.")

    # allowlist v2 — 엔진이 레그 단위로 자동 사용: 목록에 없는 (코인, 레그)는 거래 차단. 원자적 쓰기.
    out_path = Path("data") / "verified_ok.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({
        "generated_ms": int(time.time() * 1000),
        "legs": verified,
        # 구버전 엔진 호환: 전 레그 무결 코인만 — 구엔진은 코인 단위로 전 레그를 열기 때문 (compat_ok 참조)
        "ok": compat_ok(verified, blocklist),
    }))
    os.replace(tmp, out_path)
    print(f"\nallowlist 저장: {out_path} ({len(verified)}종·{n_legs}레그) — 재시작 시 엔진이 자동 적용, 신규 상장은 재실행 전까지 차단")


if __name__ == "__main__":
    main()
