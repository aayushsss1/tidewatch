import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))
import report  # noqa: E402

CSV = Path(__file__).parent / "testdata" / "mini.csv"
F, A = "headline/w1-flash-5x", "headline/b-adversarial"


@pytest.fixture(scope="module")
def runs():
    return report.load(CSV)


def get(runs, sc, cfg, g, rep=0):
    return runs[(runs.scenario == sc) & (runs.config == cfg) & (runs.guardrails == g) & (runs.repeat == rep)].iloc[0]


def test_workload_rows_are_summed(runs):
    r = get(runs, F, "hpa", "n/a")
    assert (r.breach_min, r.overprov_rm, r.underprov_rm) == (40, 150, 15)


def test_lead_row_no_double_counting(runs):
    r = get(runs, F, "tidewatch-agent", "matched", 1)
    assert r.usd == pytest.approx(0.02) and r.tokens == 1200 and r.llm_calls == 4
    # 2 repeats x (flash 0.02 + control 0.01 + adversarial 0.01): usd lives on lead rows only
    assert runs[runs.config == "tidewatch-agent"].usd.sum() == pytest.approx(2 * 0.04)


def test_pairing(runs):
    m = report.paired(runs, "tidewatch-agent", "tidewatch-baseline", "matched")
    assert len(m) == 6  # 3 scenarios x 2 repeats, each joined to exactly one baseline run
    row = m[(m.scenario == F) & (m.repeat == 0)].iloc[0]
    assert row.d_breach_min == 10 - 30 and row.d_overprov_rm == 210 - 150 and row.d_underprov_rm == 7 - 12
    # controller configs pair under every profile
    assert len(report.paired(runs, "keda-cron", "tidewatch-baseline", "default")) == 1


def test_percentages_and_extra(runs):
    h = {r["config"]: r for r in report.headline(runs, "matched")}
    assert h["hpa"]["prevented_pct"] == 0
    assert h["tidewatch-baseline"]["prevented_pct"] == pytest.approx(100 * (40 - 30) / 40)
    assert h["regex-rule"]["prevented_pct"] == pytest.approx(100 * (40 - 15) / 40)
    assert h["regex-rule"]["extra_rm"] == 280 - 150
    a = h["tidewatch-agent"]  # per-repeat breach totals 10 and 6 -> median 8
    assert a["breach"] == 8 and a["repeats"] == 2 and a["prevented_pct"] == pytest.approx(100 * 32 / 40)
    assert a["usd_per_run"] == pytest.approx((0.02 + 0.01 + 0.01) / 3)


def test_share_underprov_le(runs):
    s = report.build(runs)["profiles"]["matched"]["paired"]["tidewatch-baseline"]["all"]
    assert s["n"] == 6 and s["share_underprov_le"] == 1.0
    assert report.build(runs)["profiles"]["matched"]["paired"]["regex-rule"]["all"]["n"] == 6


def test_b_section_flags_never_below(runs):
    b = {(x["family"], x["config"]): x for x in report.b_section(runs, "matched")}
    assert b[("b-adversarial", "tidewatch-agent")]["never_below_baseline"] is True
    assert b[("b-adversarial", "regex-rule")]["overprov_delta"] == 260
    assert b[("b-adversarial", "keda-cron")]["overprov_delta"] == 460


def test_safety_flags_nonzero(runs):
    assert report.safety(runs) == {"safety_violations": 0, "skipped_steps": 0}
    bad = runs.copy()
    bad.loc[0, "safety_violations"] = 1
    assert report.safety(bad)["safety_violations"] == 1


def test_no_agent_case(tmp_path):
    df = pd.read_csv(CSV, keep_default_na=False)
    df[df.config != "tidewatch-agent"].to_csv(tmp_path / "noagent.csv", index=False)
    out = tmp_path / "out"
    report.main([str(tmp_path / "noagent.csv"), "--out", str(out)])
    md = (out / "summary.md").read_text()
    assert "Agent results pending" in md and "How to read this" in md
    assert (out / "summary.json").exists() and list(out.glob("*.png"))


def test_full_report_writes_everything(tmp_path):
    report.main([str(CSV), "--out", str(tmp_path)])
    md = (tmp_path / "summary.md").read_text()
    assert "Paired deltas" in md and "WARNING" not in md
    assert len(list(tmp_path.glob("*.png"))) == 6  # 3 figures x 2 profiles
