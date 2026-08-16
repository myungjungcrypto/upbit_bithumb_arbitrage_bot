"""저장 게이트·보존 janitor·컴팩션 검증 — 디스크 폭주·파일 폭탄 재발 방지의 핵심 로직."""
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from kimp.storage.parquet import compact_old_partitions, premium_store_gate, purge_old


def test_compaction_merges_small_files(tmp_path):
    part = tmp_path / "premium" / "date=2026-08-01"
    part.mkdir(parents=True)
    for i in range(3):
        pq.write_table(
            pa.Table.from_pylist([{"ts": i * 10 + j, "coin": "XRP", "v": float(j)} for j in range(5)]),
            part / f"part-{i:03d}.parquet",
        )
    today_part = tmp_path / "premium" / "date=2026-08-04"
    today_part.mkdir()
    pq.write_table(pa.Table.from_pylist([{"ts": 1, "coin": "BTC", "v": 0.0}]), today_part / "part-000.parquet")

    merged = compact_old_partitions(tmp_path, today=date(2026, 8, 4), min_files=2)

    assert merged == 3
    files = list(part.glob("*.parquet"))
    assert len(files) == 1 and files[0].name.startswith("compacted-")
    assert pq.read_table(files[0]).num_rows == 15  # 행 보존
    # 오늘 파티션은 건드리지 않음
    assert len(list(today_part.glob("part-*.parquet"))) == 1


def test_gate_first_row_always_stored():
    assert premium_store_gate(None, 0.001, -0.002, 1000, 0.0005, 10000)


def test_gate_small_change_suppressed():
    prev = (0.0010, -0.0020, 1000)
    assert not premium_store_gate(prev, 0.0012, -0.0021, 2000, 0.0005, 10000)


def test_gate_meaningful_change_stored():
    prev = (0.0010, -0.0020, 1000)
    assert premium_store_gate(prev, 0.0016, -0.0020, 2000, 0.0005, 10000)   # in 6bp 이동
    assert premium_store_gate(prev, 0.0010, -0.0028, 2000, 0.0005, 10000)   # out 8bp 이동


def test_gate_heartbeat():
    prev = (0.0010, -0.0020, 1000)
    assert not premium_store_gate(prev, 0.0010, -0.0020, 5000, 0.0005, 10000)
    assert premium_store_gate(prev, 0.0010, -0.0020, 11001, 0.0005, 10000)


def test_gate_none_flip_is_information():
    prev = (0.0010, None, 1000)
    assert premium_store_gate(prev, None, None, 2000, 0.0005, 10000)   # 깊이 소진 전환
    assert premium_store_gate(prev, 0.0010, -0.001, 2000, 0.0005, 10000)  # 회복 전환


def test_purge_old(tmp_path):
    for table, dates in {
        "books": ["2026-07-25", "2026-08-01", "2026-08-03"],
        "premium": ["2026-07-01", "2026-08-03"],
        "unknown": ["2026-07-01"],
        }.items():
        for d in dates:
            (tmp_path / table / f"date={d}").mkdir(parents=True)
            (tmp_path / table / f"date={d}" / "part.parquet").write_text("x")
    (tmp_path / "premium" / "date=badname").mkdir()  # 파싱 불가 → 건너뜀

    removed = purge_old(tmp_path, {"books": 3, "trades": 14, "default": 30}, 30, date(2026, 8, 4))

    assert (tmp_path / "books" / "date=2026-08-03").exists()      # 3일 이내 유지
    assert not (tmp_path / "books" / "date=2026-08-01").exists()  # 3일 초과 삭제
    assert not (tmp_path / "books" / "date=2026-07-25").exists()
    assert (tmp_path / "premium" / "date=2026-08-03").exists()
    assert not (tmp_path / "premium" / "date=2026-07-01").exists()  # default 30일 초과
    assert not (tmp_path / "unknown" / "date=2026-07-01").exists()  # 미지정 테이블도 default 적용
    assert (tmp_path / "premium" / "date=badname").exists()
    assert len(removed) == 4
