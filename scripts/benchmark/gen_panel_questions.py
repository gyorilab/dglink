#!/usr/bin/env python
"""
Generate the per-gene "membership" benchmark questions from the gold panel.

Instead of open-ended "list some shared genes" (which lets the arms answer at wildly
different scope — the graph dumps thousands, the API samples a handful — and can't be
graded), we ask a yes/no over a FIXED panel. Both arms answer over the same candidates and
emit a machine-checkable `PRESENT:` line, so score.py can compute precision/recall/F1
against the independent gold in gold_gene_panel.json.

Writes scripts/benchmark/questions_panel.jsonl (two questions):
  * panel_cananolab      — present in a caNanoLab data file?          (gold: cananolab)
  * panel_cana_pdc       — present in BOTH caNanoLab file AND a PDC study? (gold: cananolab_and_pdc)

Usage: python scripts/benchmark/gen_panel_questions.py
"""
import os
import json

HERE = os.path.dirname(os.path.abspath(__file__))
gold = json.load(open(os.path.join(HERE, "gold_gene_panel.json")))
PANEL = [g for g in gold if g != "_meta"]
panel_str = ", ".join(PANEL)

FMT = (
    "End your answer with EXACTLY one final line listing only the panel genes that satisfy "
    "the condition (comma-separated symbols from the panel, or the word `none`):\n"
    "PRESENT: <gene1, gene2, ...>"
)

QUESTIONS = [
    {
        "id": "panel_cananolab",
        "kind": "panel",
        "gold_field": "cananolab",
        "panel": PANEL,
        "question": (
            "For each gene in this panel, determine whether the gene appears in any DATA FILE "
            "of the caNanoLab study (NCI General Commons portal, phs_accession 10.17917). "
            "Use your available tools to check the study's file contents.\n\n"
            f"PANEL: {panel_str}\n\n" + FMT
        ),
    },
    {
        "id": "panel_cana_pdc",
        "kind": "panel",
        "gold_field": "cananolab_and_pdc",
        "panel": PANEL,
        "question": (
            "For each gene in this panel, determine whether it is present in BOTH (a) a data "
            "file of the caNanoLab study (NCI General Commons portal, phs_accession 10.17917) "
            "AND (b) a PDC proteomic study. A gene counts only if BOTH conditions hold. Use "
            "your available tools to check each condition.\n\n"
            f"PANEL: {panel_str}\n\n" + FMT
        ),
    },
]

out = os.path.join(HERE, "questions_panel.jsonl")
with open(out, "w") as f:
    for q in QUESTIONS:
        f.write(json.dumps(q) + "\n")

# print the gold positives for each question so you can eyeball what "correct" means
def positive(field):
    pos = []
    for g in PANEL:
        c = (gold[g]["cananolab"] or {}).get("present")
        p = (gold[g]["pdc"] or {}).get("present")
        val = c if field == "cananolab" else (c and p)
        if val:
            pos.append(g)
    return pos

print(f"wrote {out} with {len(QUESTIONS)} questions over {len(PANEL)} genes")
for q in QUESTIONS:
    pos = positive(q["gold_field"])
    print(f"  {q['id']:16s} gold_positive ({len(pos)}/{len(PANEL)}): {', '.join(pos)}")
