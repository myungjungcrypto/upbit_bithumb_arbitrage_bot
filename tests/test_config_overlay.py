"""config/local.yaml 오버레이 — 서버 전용 값을 git 밖에서 깊은 병합."""
from kimp.config import load_config


def test_local_overlay_deep_merges(tmp_path):
    (tmp_path / "default.yaml").write_text(
        "execution:\n  mode: \"off\"\n  routes: []\n  max_cycles: 1\npaper:\n  enabled: true\n"
    )
    cfg = load_config(tmp_path / "default.yaml")
    assert cfg.raw["execution"]["mode"] == "off"
    (tmp_path / "local.yaml").write_text(
        "execution:\n  mode: dry_run\n  routes:\n    - {coin: PROM, dom: upbit, ovs: okx, direction: in}\n"
    )
    cfg = load_config(tmp_path / "default.yaml")
    e = cfg.raw["execution"]
    assert e["mode"] == "dry_run" and e["routes"][0]["coin"] == "PROM"
    assert e["max_cycles"] == 1 and cfg.raw["paper"]["enabled"] is True   # 미지정 키는 default 유지
