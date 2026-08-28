"""사이클 모델 (T6). 상태 전이는 전부 타임스탬프와 함께 stamps에 기록 — §1.4 단계별 측정의 원천."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from decimal import Decimal

from ..models import now_ms

# 상태 (T6 확정 흐름의 페이퍼 부분집합 — Live에서 WITHDRAW_*/CHAIN_* 세분화 추가)
SIGNAL = "SIGNAL"
ENTERED = "ENTERED"            # 매수 체결 (+ OUT은 헤지 락 포함) — 페이퍼는 VWAP 즉시 체결 가정
IN_FLIGHT = "IN_FLIGHT"        # 전송 중 (페이퍼: 코인별 예상 전송시간 타이머)
ARRIVED = "ARRIVED"            # 입금 크레딧 — 매도 실행 시점
SETTLED = "SETTLED"
SETTLED_STUCK = "SETTLED_STUCK"  # 도착 시 유동성 소진 반복 → 강제 정산 (WARN)
VOID = "VOID"                    # 소급 무효 (예: trade_blocklist 등재) — 손익 미집계

OPEN_STATES = (SIGNAL, ENTERED, IN_FLIGHT, ARRIVED)


@dataclass
class Cycle:
    kind: str                  # "in" | "out"
    coin: str
    dom_ex: str
    ovs_ex: str
    notional_usd: Decimal      # 투입 명목 (USD 기준)
    hedged: bool = False
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    state: str = SIGNAL
    stamps: dict = field(default_factory=dict)   # state -> ts_ms
    qty: Decimal | None = None
    entry_edge: float | None = None              # 진입 시점 순엣지 (기대)
    locked_usdt: Decimal | None = None           # OUT 헤지 락 금액
    arrival_at_ms: int | None = None
    pnl_usd: float | None = None
    note: str = ""

    def stamp(self, state: str) -> None:
        self.state = state
        self.stamps[state] = now_ms()

    def to_json(self) -> str:
        d = dict(self.__dict__)
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = f"D:{v}"
        return json.dumps(d)

    @classmethod
    def from_json(cls, raw: str) -> "Cycle":
        d = json.loads(raw)
        for k, v in d.items():
            if isinstance(v, str) and v.startswith("D:"):
                d[k] = Decimal(v[2:])
        c = cls.__new__(cls)
        c.__dict__.update(d)
        return c
