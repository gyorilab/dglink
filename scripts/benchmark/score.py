"""
Score the benchmark.

Two layers, kept deliberately separate:

1. OBJECTIVE metrics (no ground truth needed) — turns, tokens, latency, cost per arm.
   These directly substantiate "the API-only agent is slower and does more work".

2. CORRECTNESS (needs human labels in labels_to_fill.csv) — per-arm accuracy against a
   *curated* verdict. We report this as the primary correctness number.

   We also compute the "agreement analysis" the benchmark notes described (things one arm
   found that the other didn't). IMPORTANT: that analysis treats the union of the two
   agents as ground truth, so it silently ignores anything BOTH miss — it inflates recall
   and is circular for the headline claim. It is reported as a clearly-labelled SECONDARY
   view only. Prefer the curated accuracy for any claim you put in front of your PI.

Usage:
    python scripts/benchmark/score.py
"""

import re
import csv
import json
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load_results() -> list[dict]:
    p = HERE / "results.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


# --------------------------------------------------------------- panel (auto) scoring
def load_gold() -> dict:
    p = HERE / "gold_gene_panel.json"
    return json.loads(p.read_text()) if p.exists() else {}


def load_panel_questions() -> dict:
    """id -> {gold_field, panel}, from questions_panel.jsonl (if present)."""
    p = HERE / "questions_panel.jsonl"
    if not p.exists():
        return {}
    out = {}
    for line in p.read_text().splitlines():
        if line.strip():
            q = json.loads(line)
            if q.get("kind") == "panel":
                out[q["id"]] = {"gold_field": q["gold_field"], "panel": q["panel"]}
    return out


def gold_positive_set(gold: dict, panel: list, field: str) -> set:
    """Genes in the panel whose gold label for `field` is True."""
    pos = set()
    for g in panel:
        cana = (gold.get(g, {}).get("cananolab") or {}).get("present")
        pdc = (gold.get(g, {}).get("pdc") or {}).get("present")
        gdc = (gold.get(g, {}).get("gdc") or {}).get("present")
        val = {"cananolab": cana, "cananolab_and_pdc": cana and pdc,
               "cananolab_and_gdc": cana and gdc}.get(field)
        if val:
            pos.add(g)
    return pos


def parse_present(answer: str, panel: list) -> set:
    """Extract the model's predicted-present set from its `PRESENT:` line."""
    if not answer:
        return set()
    upper = {g.upper(): g for g in panel}
    line = None
    for m in re.finditer(r"(?im)^\s*PRESENT:\s*(.*)$", answer):
        line = m.group(1)  # keep the LAST PRESENT: line
    if line is None:  # fall back to scanning the whole answer for panel symbols
        line = answer
    toks = re.split(r"[^A-Za-z0-9]+", line)
    return {upper[t.upper()] for t in toks if t.upper() in upper}


def _prf(pred: set, gold_pos: set, panel_set: set) -> dict:
    tp = pred & gold_pos
    fp = pred - gold_pos
    fn = gold_pos - pred
    tn = (panel_set - pred) & (panel_set - gold_pos)
    prec = len(tp) / (len(tp) + len(fp)) if (tp or fp) else 0.0
    rec = len(tp) / len(gold_pos) if gold_pos else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    acc = (len(tp) + len(tn)) / len(panel_set) if panel_set else 0.0
    return {"precision": prec, "recall": rec, "f1": f1, "accuracy": acc,
            "tp": len(tp), "fp": sorted(fp), "fn": sorted(fn)}


def panel_scoring(results: list[dict]) -> None:
    gold = load_gold()
    qmeta = load_panel_questions()
    panel_ids = sorted({r["id"] for r in results if r.get("kind") == "panel"} & set(qmeta))
    if not gold or not panel_ids:
        return
    print("\n=== PANEL ACCURACY (automatic, vs independent gold — PRIMARY) ===")
    for qid in panel_ids:
        panel = qmeta[qid]["panel"]
        field = qmeta[qid]["gold_field"]
        gold_pos = gold_positive_set(gold, panel, field)
        print(f"\n[{qid}]  field={field}  gold_positive={len(gold_pos)}/{len(panel)}")
        for r in [x for x in results if x["id"] == qid]:
            pred = parse_present(r.get("answer"), panel)
            m = _prf(pred, gold_pos, set(panel))
            print(f"  {r['arm']:10s} P={m['precision']:.2f} R={m['recall']:.2f} "
                  f"F1={m['f1']:.2f} acc={m['accuracy']:.2f}  (tp={m['tp']})")
            if m["fp"]:
                print(f"             false-positives (claimed, gold=no): {', '.join(m['fp'])}")
            if m["fn"]:
                print(f"             false-negatives (missed):           {', '.join(m['fn'])}")


def load_labels() -> dict:
    """(id, arm) -> verdict, from the human-filled sheet (blank rows ignored)."""
    p = HERE / "labels_to_fill.csv"
    if not p.exists():
        return {}
    labels = {}
    with p.open() as f:
        for row in csv.DictReader(f):
            verdict = (list(row.values())[2] or "").strip().lower()
            if verdict:
                labels[(row["id"], row["arm"])] = verdict
    return labels


def _agg(values: list) -> dict:
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return {}
    return {"mean": round(statistics.mean(vals), 1),
            "median": round(statistics.median(vals), 1),
            "max": max(vals)}


def objective(results: list[dict]) -> None:
    print("\n=== OBJECTIVE effort/latency (per arm) ===")
    arms = sorted({r["arm"] for r in results})
    for arm in arms:
        rs = [r for r in results if r["arm"] == arm]
        errs = sum(1 for r in rs if r.get("error") or r.get("is_error"))
        print(f"\n[{arm}]  n={len(rs)}  failures={errs}")
        for metric in ("num_turns", "output_tokens", "input_tokens", "wall_sec", "total_cost_usd"):
            print(f"  {metric:16s} {_agg([r.get(metric) for r in rs])}")


def correctness(results: list[dict], labels: dict) -> None:
    if not labels:
        print("\n=== CORRECTNESS === (no labels yet — fill labels_to_fill.csv)")
        return
    arms = sorted({r["arm"] for r in results})
    ids = sorted({r["id"] for r in results})

    print("\n=== CORRECTNESS (curated verdicts — PRIMARY) ===")
    for arm in arms:
        verds = [labels.get((i, arm)) for i in ids if (i, arm) in labels]
        n = len(verds)
        correct = sum(1 for v in verds if v == "correct")
        partial = sum(1 for v in verds if v == "partial")
        print(f"  [{arm}] accuracy={correct}/{n}"
              f"  (+{partial} partial)" if n else f"  [{arm}] no labels")

    # SECONDARY: agreement analysis (union-as-truth) — caveated on purpose
    baselines = [a for a in arms if a != "dglink"]
    if "dglink" not in arms or not baselines:
        return
    baseline = baselines[0]  # gc_api / pdc_api / whichever control ran
    print(f"\n=== AGREEMENT ANALYSIS (SECONDARY — dglink vs {baseline}, union-as-truth, biased) ===")
    dglink_ok = {i for i in ids if labels.get((i, "dglink")) in ("correct", "partial")}
    api_ok = {i for i in ids if labels.get((i, baseline)) in ("correct", "partial")}
    print(f"  both correct:               {sorted(dglink_ok & api_ok)}")
    print(f"  dglink only (baseline miss):{sorted(dglink_ok - api_ok)}")
    print(f"  baseline only (dglink miss):{sorted(api_ok - dglink_ok)}")
    print(f"  neither:                    {sorted(set(ids) - dglink_ok - api_ok)}  <-- invisible to union-truth")


def main() -> None:
    results = load_results()
    objective(results)
    panel_scoring(results)
    correctness(results, load_labels())


if __name__ == "__main__":
    main()
