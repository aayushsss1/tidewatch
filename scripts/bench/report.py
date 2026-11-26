#!/usr/bin/env python3
"""Benchmark report generator for tidewatch-bench results.csv.

Usage: python scripts/bench/report.py <results.csv> --out docs/benchmarks/<run-name>/

Row model (see cmd/tidewatch-bench/run.go): one CSV row per (run x workload). A run is
(scenario, config, guardrails, repeat, seed). Per-workload columns are summed over a run's rows;
run-level columns (usd, tokens, ...) are read from the lead row only, so they are never double
counted. Pairing joins on (scenario, seed); Tidewatch-mode configs also join on guardrails,
controller configs (guardrails "n/a") pair against every profile.
"""
import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HPA, KEDA_P, KEDA_C = "hpa", "keda-prometheus", "keda-cron"
BASE, REGEX, AGENT, ORACLE = "tidewatch-baseline", "regex-rule", "tidewatch-agent", "oracle"
CONFIGS = [HPA, KEDA_P, KEDA_C, BASE, REGEX, AGENT, ORACLE]
TW_CONFIGS = {BASE, REGEX, AGENT}
FAMILIES = ["flash-3x", "flash-5x", "cascade-4x", "flash-5x-slow", "cascade-4x-slow", "control"]
B_FAMILIES = ["b-adversarial", "b-wrong-time"]
SUM_COLS = ["breach_min", "overprov_rm", "underprov_rm", "scale_events", "lift_rm",
            "safety_violations", "skipped_steps"]
LEAD_COLS = ["investigations", "llm_calls", "input_tokens", "output_tokens", "usd", "wall_seconds"]
RUN_KEY = ["scenario", "config", "guardrails", "repeat", "seed"]
# Okabe-Ito, colorblind safe.
COLORS = {HPA: "#000000", KEDA_P: "#E69F00", KEDA_C: "#56B4E9", BASE: "#009E73",
          REGEX: "#F0E442", AGENT: "#D55E00", ORACLE: "#0072B2"}


def family(scenario: str) -> str:
    name = scenario.split("/")[-1]
    return re.sub(r"^w\d+-", "", name)


def load(path) -> pd.DataFrame:
    """Reads results.csv and returns one row per run."""
    df = pd.read_csv(path, keep_default_na=False)
    df["lead"] = df["lead"].astype(str).str.lower().eq("true")
    sums = df.groupby(RUN_KEY, as_index=False)[SUM_COLS].sum()
    lead = df[df["lead"]][RUN_KEY + LEAD_COLS]
    if lead.duplicated(RUN_KEY).any():
        raise ValueError("more than one lead row for a run")
    runs = sums.merge(lead, on=RUN_KEY, how="left").fillna({c: 0 for c in LEAD_COLS})
    runs["family"] = runs["scenario"].map(family)
    runs["tokens"] = runs["input_tokens"] + runs["output_tokens"]
    return runs


def profiles(runs: pd.DataFrame) -> list:
    p = sorted(g for g in runs["guardrails"].unique() if g != "n/a")
    return sorted(p, key=lambda g: (g != "matched", g))


def view(runs: pd.DataFrame, profile: str) -> pd.DataFrame:
    """Runs visible under a guardrails profile: its Tidewatch configs plus the controllers."""
    return runs[(runs["guardrails"] == profile) | (runs["guardrails"] == "n/a")]


def paired(runs: pd.DataFrame, a: str, b: str, profile: str) -> pd.DataFrame:
    """Per-run deltas a - b, joined on (scenario, seed) [+ guardrails for Tidewatch configs].

    Config a's repeats each pair with config b's single run. Columns are suffixed _a/_b, plus d_<col>.
    """
    v = view(runs, profile)
    ra, rb = v[v["config"] == a], v[v["config"] == b]
    cols = ["breach_min", "overprov_rm", "underprov_rm", "lift_rm"]
    m = ra[["scenario", "seed", "repeat", "family"] + cols].merge(
        rb[["scenario", "seed"] + cols], on=["scenario", "seed"], suffixes=("_a", "_b"))
    for c in cols:
        m["d_" + c] = m[c + "_a"] - m[c + "_b"]
    return m


def is_b(df):
    return df["family"].isin(B_FAMILIES)


def iqr(x):
    x = np.asarray(x, dtype=float)
    return float(np.percentile(x, 25)), float(np.percentile(x, 75))


def per_repeat_totals(v: pd.DataFrame, config: str) -> pd.DataFrame:
    """Per-repeat totals of one config over the event (non-B) scenarios."""
    r = v[(v["config"] == config) & ~is_b(v)]
    return r.groupby("repeat")[["breach_min", "overprov_rm", "underprov_rm", "usd", "tokens"]].sum()


def pct_prevented(hpa_breach, breach):
    return None if not hpa_breach else 100.0 * (hpa_breach - breach) / hpa_breach


def headline(runs: pd.DataFrame, profile: str) -> list:
    v = view(runs, profile)
    hpa = per_repeat_totals(v, HPA)
    hb, ho = (hpa["breach_min"].sum(), hpa["overprov_rm"].sum()) if len(hpa) else (0, 0)
    rows = []
    for cfg in CONFIGS:
        t = per_repeat_totals(v, cfg)
        if t.empty:
            continue
        row = {"config": cfg, "repeats": int(len(t))}
        for col, key in (("breach_min", "breach"), ("overprov_rm", "overprov"), ("underprov_rm", "underprov")):
            row[key] = float(t[col].median())
            if len(t) > 1:
                row[key + "_iqr"] = iqr(t[col])
        row["prevented_pct"] = pct_prevented(hb, row["breach"])
        row["extra_rm"] = row["overprov"] - ho
        if len(t) > 1:
            row["prevented_pct_iqr"] = tuple(pct_prevented(hb, x) if hb else None
                                             for x in (row["breach_iqr"][1], row["breach_iqr"][0]))
            row["extra_rm_iqr"] = tuple(x - ho for x in row["overprov_iqr"])
        if cfg == AGENT:
            ev = v[(v["config"] == AGENT) & ~is_b(v)]
            bb = v[(v["config"] == AGENT) & is_b(v)]
            row["usd_per_run"], row["tokens_per_run"] = float(ev["usd"].mean()), float(ev["tokens"].mean())
            if len(bb):
                row["b_usd_per_run"], row["b_tokens_per_run"] = float(bb["usd"].mean()), float(bb["tokens"].mean())
        rows.append(row)
    return rows


def pair_stats(m: pd.DataFrame) -> dict:
    if m.empty:
        return {"n": 0}
    return {"n": int(len(m)),
            "breach_delta_sum": float(m["d_breach_min"].sum()),
            "breach_delta_median": float(m["d_breach_min"].median()),
            "underprov_delta_sum": float(m["d_underprov_rm"].sum()),
            "overprov_delta_sum": float(m["d_overprov_rm"].sum()),
            "overprov_delta_median": float(m["d_overprov_rm"].median()),
            "share_underprov_le": float((m["d_underprov_rm"] <= 0).mean())}


def per_family(runs: pd.DataFrame, profile: str) -> pd.DataFrame:
    v = view(runs, profile)
    v = v[v["family"].isin(FAMILIES + B_FAMILIES)]
    g = v.groupby(["family", "config", "repeat"])["breach_min"].sum().reset_index()
    g = g.groupby(["family", "config"])["breach_min"].median().unstack("config")
    fams = [f for f in FAMILIES + B_FAMILIES if f in g.index]
    return g.reindex(index=fams, columns=[c for c in CONFIGS if c in g.columns])


def b_section(runs: pd.DataFrame, profile: str) -> list:
    out = []
    for fam in B_FAMILIES:
        for cfg in [AGENT, BASE, REGEX, KEDA_C]:
            m = paired(runs[runs["family"] == fam], cfg, BASE, profile)
            if m.empty:
                continue
            out.append({"family": fam, "config": cfg, "n": int(len(m)),
                        "breach": float(m["breach_min_a"].median()),
                        "overprov_delta": float(m["d_overprov_rm"].median()),
                        "overprov_delta_max": float(m["d_overprov_rm"].max()),
                        "lift_rm": float(m["lift_rm_a"].median()),
                        "underprov_delta_max": float(m["d_underprov_rm"].max()),
                        # Only the lift configs can only add capacity; keda-cron is informational.
                        "never_below_baseline": (bool((m["d_underprov_rm"] <= 0).all())
                                                 if cfg in (AGENT, REGEX) else None)})
    return out


def safety(runs: pd.DataFrame) -> dict:
    return {"safety_violations": int(runs["safety_violations"].sum()),
            "skipped_steps": int(runs["skipped_steps"].sum())}


def build(runs: pd.DataFrame) -> dict:
    has_agent = bool((runs["config"] == AGENT).any())
    res = {"has_agent": has_agent, "safety": safety(runs), "profiles": {}}
    for p in profiles(runs):
        pr = {"headline": headline(runs, p), "b": b_section(runs, p), "paired": {}}
        if has_agent:
            for other in (BASE, REGEX):
                m = paired(runs, AGENT, other, p)
                pr["paired"][other] = {"events": pair_stats(m[~is_b(m)]), "all": pair_stats(m)}
        res["profiles"][p] = pr
    return res


# ---------- figures ----------

def figures(runs: pd.DataFrame, profile: str, out: Path) -> list:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False})
    v = view(runs, profile)
    files = []

    # 1. breach-minutes by config, grouped by family
    tab = per_family(runs, profile)
    fig, ax = plt.subplots(figsize=(11, 5))
    w = 0.8 / max(len(tab.columns), 1)
    for i, cfg in enumerate(tab.columns):
        ax.bar(np.arange(len(tab)) + i * w, tab[cfg].values, w, label=cfg, color=COLORS[cfg])
    ax.set_xticks(np.arange(len(tab)) + 0.4 - w / 2)
    ax.set_xticklabels(tab.index, rotation=25, ha="right")
    ax.set_ylabel("breach-minutes (summed over scenarios)")
    ax.set_title(f"SLO breach-minutes by config and scenario family ({profile} guardrails)")
    ax.legend(ncol=4, fontsize=9)
    fig.tight_layout()
    f = f"breach_by_family_{profile}.png"
    fig.savefig(out / f, dpi=130)
    plt.close(fig)
    files.append((f, "Breach-minutes by config, grouped by family"))

    # 2. replica-minutes (over and under)
    hl = headline(runs, profile)
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(hl))
    ax.bar(x - 0.2, [r["overprov"] for r in hl], 0.4, label="over-provisioned", color="#0072B2")
    ax.bar(x + 0.2, [r["underprov"] for r in hl], 0.4, label="under-provisioned", color="#D55E00")
    ax.set_xticks(x)
    ax.set_xticklabels([r["config"] for r in hl], rotation=25, ha="right")
    ax.set_ylabel("replica-minutes (event scenarios)")
    ax.set_title(f"Replica-minutes by config ({profile} guardrails)")
    ax.legend()
    fig.tight_layout()
    f = f"replica_minutes_{profile}.png"
    fig.savefig(out / f, dpi=130)
    plt.close(fig)
    files.append((f, "Over- and under-provisioned replica-minutes by config"))

    # 3. cost/benefit scatter
    fig, ax = plt.subplots(figsize=(8, 6))
    for r in hl:
        if r["config"] == HPA or r["prevented_pct"] is None:
            continue
        xerr = yerr = None
        if "extra_rm_iqr" in r:
            lo, hi = r["extra_rm_iqr"]
            xerr = [[max(r["extra_rm"] - lo, 0)], [max(hi - r["extra_rm"], 0)]]
            ylo, yhi = r["prevented_pct_iqr"]
            if ylo is not None:
                yerr = [[max(r["prevented_pct"] - ylo, 0)], [max(yhi - r["prevented_pct"], 0)]]
        y = r["prevented_pct"]
        ax.errorbar(r["extra_rm"], y, xerr=xerr, yerr=yerr, fmt="o", ms=9,
                    color=COLORS[r["config"]], mec="black", label=r["config"], capsize=4)
    ax.axhline(0, color="grey", lw=0.8)
    ax.axvline(0, color="grey", lw=0.8)
    ax.set_xlabel("extra replica-minutes vs HPA (cost)")
    ax.set_ylabel("% breach-minutes prevented vs HPA (benefit)")
    ax.set_title(f"Cost/benefit vs HPA ({profile} guardrails)\nup and left is better; agent bars are IQR over repeats")
    ax.legend(fontsize=9)
    fig.tight_layout()
    f = f"cost_benefit_{profile}.png"
    fig.savefig(out / f, dpi=130)
    plt.close(fig)
    files.append((f, "Cost/benefit scatter vs HPA"))
    return files


# ---------- markdown ----------

def num(x, d=0):
    return "-" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:,.{d}f}"


def span(iqr_, d=0):
    return "" if not iqr_ or None in iqr_ else f" [{num(iqr_[0], d)}-{num(iqr_[1], d)}]"


def md_table(header, rows):
    return "\n".join(["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
                     + ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])


def render(runs: pd.DataFrame, res: dict, figs: dict, title: str) -> str:
    L = [f"# Benchmark summary: {title}", ""]
    L += ["## How to read this", "",
          "- Breach-minute: a simulated minute where offered load exceeds ready capacity. Lower is better.",
          "- Replica-minutes are split into over-provisioned (cost) and under-provisioned (capacity shortfall).",
          "- All deltas are paired: runs of the same scenario share the cluster seed (common random numbers), "
          "and Tidewatch-mode configs also pair on the guardrails profile.",
          "- Headline numbers cover the event scenarios (flash, cascade, slow-start, control). The two B scenarios "
          "have their own section. Agent figures are the median [IQR] over repeats of the per-repeat totals.",
          "- The 'never less capacity than baseline' check is empirical over the runs here, not a proof.",
          "- Simulated cluster: see the methodology README for threats to validity.", ""]
    s = res["safety"]
    L += ["## Safety", ""]
    if s["safety_violations"] or s["skipped_steps"]:
        L += [f"> **WARNING: SAFETY CHECK FAILED.** safety_violations={s['safety_violations']}, "
              f"skipped_steps={s['skipped_steps']}. Both must be 0. Do not publish these results.", ""]
    else:
        L += ["safety_violations = 0, skipped_steps = 0 across all runs.", ""]
    for p, pr in res["profiles"].items():
        L += [f"## Guardrails profile: {p}", "", "### Headline", ""]
        rows = []
        for r in pr["headline"]:
            agent = r["config"] == AGENT
            rows.append([r["config"], num(r["breach"]) + (span(r.get("breach_iqr")) if agent else ""),
                         num(r["prevented_pct"], 1) + ("%" if r["prevented_pct"] is not None else ""),
                         num(r["extra_rm"]) + (span(r.get("extra_rm_iqr")) if agent else ""),
                         num(r["underprov"]),
                         f"${r['usd_per_run']:.4f} / {num(r['tokens_per_run'])}" if agent else "-"])
        L += [md_table(["config", "breach-min", "% prevented vs HPA", "extra replica-min vs HPA",
                        "underprov replica-min", "agent $/run / tokens/run (event scenarios)"], rows), ""]
        if not res["has_agent"]:
            L += ["Agent results pending.", ""]
        elif any("b_usd_per_run" in r for r in pr["headline"]):
            r = next(r for r in pr["headline"] if "b_usd_per_run" in r)
            L += [f"Agent cost per run on the B scenarios: ${r['b_usd_per_run']:.4f} / {num(r['b_tokens_per_run'])} tokens.", ""]
        else:
            L += ["### Paired deltas (agent minus comparison)", ""]
            rows = []
            for other, d in pr["paired"].items():
                e, a = d["events"], d["all"]
                if not e["n"]:
                    continue
                rows.append([f"agent - {other}", e["n"], num(e["breach_delta_sum"]), num(e["breach_delta_median"], 1),
                             num(e["underprov_delta_sum"]), num(e["overprov_delta_sum"]),
                             num(e["overprov_delta_median"], 1),
                             f"{100 * a['share_underprov_le']:.1f}% ({a['n']} runs)"])
            L += [md_table(["pair", "n", "breach delta (sum)", "breach delta (median)", "underprov delta (sum)",
                            "agent waste: overprov delta (sum)", "overprov delta (median)",
                            "share agent underprov <= comparison (empirical, all scenarios)"], rows), ""]
        L += ["### Breach-minutes per family (median over repeats)", ""]
        tab = per_family(runs, p)
        L += [md_table(["family"] + list(tab.columns),
                       [[f] + [num(x) for x in tab.loc[f]] for f in tab.index]), ""]
        L += ["### B scenarios (comparison is against tidewatch-baseline)", ""]
        if pr["b"]:
            L += [md_table(["scenario", "config", "n", "breach", "overprov delta (median)", "overprov delta (max)",
                            "lift replica-min", "underprov delta (max)", "never below baseline"],
                           [[b["family"], b["config"], b["n"], num(b["breach"]), num(b["overprov_delta"]),
                             num(b["overprov_delta_max"]), num(b["lift_rm"]), num(b["underprov_delta_max"]),
                             {True: "yes", False: "NO", None: "n/a (informational)"}[b["never_below_baseline"]]]
                            for b in pr["b"]]), ""]
        else:
            L += ["No B-scenario rows.", ""]
        L += ["### Figures", ""] + [f"![{cap}]({fn})" for fn, cap in figs.get(p, [])] + [""]
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("csv")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    runs = load(a.csv)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    res = build(runs)
    for p in res["profiles"]:
        if res["has_agent"]:
            for other in (BASE, REGEX):
                paired(runs, AGENT, other, p).to_csv(out / f"paired_{other}_{p}.csv", index=False)
    figs = {p: figures(runs, p, out) for p in res["profiles"]}
    (out / "summary.md").write_text(render(runs, res, figs, out.name or "results") + "\n")
    (out / "summary.json").write_text(json.dumps(res, indent=2, default=lambda o: None if o is None else str(o)) + "\n")
    s = res["safety"]
    if s["safety_violations"] or s["skipped_steps"]:
        print(f"WARNING: SAFETY CHECK FAILED {s}", file=sys.stderr)
    print(f"wrote {out}/summary.md, summary.json and {sum(len(f) for f in figs.values())} figures")


if __name__ == "__main__":
    main()
