"""
Run the DGLink benchmark: the SAME Claude Code harness against each question, once per
arm, changing only the toolset (the single controlled variable).

  * arm "dglink"  -> the DGLink knowledge graph via the neo4j MCP server
  * arm "pdc_api" -> the raw PDC GraphQL + file-download MCP server (control)

Both run headless (`claude -p ... --output-format json`), which reports tokens, turns,
duration and cost — the objective "slower / more effort" metrics that need no ground
truth. Answer correctness is scored separately (see score.py) after human labelling.

Usage:
    python scripts/benchmark/run_benchmark.py                 # all questions, both arms
    python scripts/benchmark/run_benchmark.py --arms pdc_api  # one arm
    python scripts/benchmark/run_benchmark.py --model opus --max-turns 40

Both arms run with all built-in Claude Code tools (Bash/Read/Grep/...) DISALLOWED, so the
only capability that differs between arms is the MCP server. This is essential: otherwise
the API-only baseline can just read the graph TSVs or run Cypher and cheat (see
DISALLOWED_BUILTINS below).

Prereqs:
  * `claude` CLI on PATH; neo4j running with the merged graph loaded (for the dglink arm)
  * Verify the CLI flags below against your version: `claude --help`
"""

import os
import csv
import json
import time
import argparse
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent

ARMS = {
    "dglink": {
        "config": HERE / "mcp_configs" / "dglink_neo4j.json",
        # server-level allow so every tool from that MCP server is pre-approved (no prompt)
        "allowed": "mcp__neo4j",
        # the graph merges all three portals; a node's `portal` list records every portal
        # it came from, so cross-portal questions are answerable directly.
        "prompt_suffix": (
            " The knowledge graph merges the GDC, GC (General Commons) and PDC portals into "
            "one graph. Every node/edge has a `portal` list recording which portal(s) it "
            "came from; a node shared across portals lists all of them (e.g. "
            "['gdc','gc','pdc']). Use that to answer per-portal and cross-portal questions."
        ),
    },
    # unified raw-API baseline covering all three portals (per-portal + cross-portal)
    "crdc_api": {
        "config": HERE / "mcp_configs" / "crdc_api.json",
        "allowed": "mcp__crdc_api",
        "prompt_suffix": "",
    },
    # single-portal baselines kept for targeted runs
    "gc_api": {
        "config": HERE / "mcp_configs" / "gc_api.json",
        "allowed": "mcp__gc_api",
        "prompt_suffix": "",
    },
    "pdc_api": {
        "config": HERE / "mcp_configs" / "pdc_api.json",
        "allowed": "mcp__pdc_api",
        "prompt_suffix": "",
    },
}

SYSTEM_PROMPT = (
    "You are a biomedical data analyst answering a question about NCI Cancer Research "
    "Data Commons data. Use ONLY the provided tools — do not rely on prior knowledge. "
    "Be concise, and cite the study ids / file ids you used to reach the answer. If you "
    "cannot determine the answer with the tools, say so explicitly."
)

# Built-in Claude Code tools are denied on EVERY arm so the only difference between arms
# is the MCP server (the single controlled variable). Without this, the API-only baseline
# can bypass its toolset entirely: `Bash` can open the neo4j bolt port or `docker exec`
# cypher-shell, and `Read`/`Grep`/`Glob` can read the merged-graph TSVs and repo source —
# i.e. reach the very graph it is supposed to lack. (Observed exactly this: the crdc_api
# baseline read the graph TSVs and ran Cypher, reproducing the graph's answers and
# invalidating the comparison.) `--allowedTools` only pre-approves tools; it does NOT
# exclude the built-ins, so they must be explicitly disallowed.
# NOTE: `--disallowedTools` is variadic (`<tools...>`) — each name must be its OWN argv
# element. Passing one space-joined string makes the CLI treat the whole string as a single
# (nonexistent) tool name and deny NOTHING. Keep this a list and splat it into the command.
DISALLOWED_BUILTINS = [
    "Bash", "Read", "Edit", "Write", "MultiEdit", "NotebookEdit",
    "Glob", "Grep", "WebFetch", "WebSearch", "Task",
]


def load_questions(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def run_one(question: str, arm: str, model: str, max_turns: int, timeout: int) -> dict:
    a = ARMS[arm]
    cmd = [
        "claude", "-p", question,
        "--output-format", "json",
        "--mcp-config", str(a["config"]),
        # CRITICAL: --mcp-config is ADDITIVE. Without --strict-mcp-config, every ambient MCP
        # server in the user/project config (notably a `neo4j` server, plus Open Targets,
        # ChEMBL, PubMed, ...) stays connected on EVERY arm — so the API-only baseline could
        # just call mcp__neo4j__read_neo4j_cypher (pre-approved in .claude/settings.local.json)
        # and read the graph directly. Strict mode restricts each arm to ONLY its own config.
        "--strict-mcp-config",
        "--allowedTools", a["allowed"],
        # deny built-ins on every arm so no agent can reach the graph outside its MCP server
        # (splat: each tool name must be a separate argv element — the flag is variadic)
        "--disallowedTools", *DISALLOWED_BUILTINS,
        "--max-turns", str(max_turns),
        "--append-system-prompt", SYSTEM_PROMPT + a.get("prompt_suffix", ""),
    ]
    if model:
        cmd += ["--model", model]

    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=REPO_ROOT
        )
        raw = proc.stdout
    except subprocess.TimeoutExpired:
        return {"arm": arm, "error": "timeout", "wall_sec": round(time.time() - t0, 1)}
    wall = time.time() - t0

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"arm": arm, "error": "unparseable_output", "wall_sec": round(wall, 1),
                "raw_stdout": raw[:2000], "stderr": proc.stderr[:2000]}

    usage = data.get("usage", {}) or {}
    return {
        "arm": arm,
        "answer": data.get("result"),
        "is_error": data.get("is_error"),
        "num_turns": data.get("num_turns"),          # proxy for tool-call effort
        "duration_ms": data.get("duration_ms"),
        "wall_sec": round(wall, 1),
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "total_cost_usd": data.get("total_cost_usd"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--questions", default=str(HERE / "questions_crdc.jsonl"),
                    help="question set to run; pairs with --arms (default: the CRDC set, "
                         "which is what the published cross-portal results were run on)")
    ap.add_argument("--out", default=str(HERE / "results.jsonl"))
    ap.add_argument("--arms", nargs="+", default=["dglink", "crdc_api"], choices=list(ARMS))
    ap.add_argument("--model", default=os.environ.get("BENCH_MODEL", "sonnet"),
                    help="Claude Code model alias/id (e.g. sonnet, opus). Pin one for a fair run.")
    ap.add_argument("--max-turns", type=int, default=30)
    ap.add_argument("--timeout", type=int, default=1200)
    args = ap.parse_args()

    questions = load_questions(Path(args.questions))
    out_path = Path(args.out)
    labels_path = HERE / "labels_to_fill.csv"

    ## check mode ## 
    write_mode = 'a' if out_path.exists() else "w"
    with out_path.open(write_mode) as out:
        for q in questions:
            for arm in args.arms:
                print(f"[{q['id']}][{arm}] running ...", flush=True)
                rec = run_one(q["question"], arm, args.model, args.max_turns, args.timeout)
                rec = {"id": q["id"], "kind": q.get("kind"), **rec}
                out.write(json.dumps(rec) + "\n")
                out.flush()
                print(f"  turns={rec.get('num_turns')} "
                      f"wall={rec.get('wall_sec')}s out_tok={rec.get('output_tokens')}")

    # emit an empty labelling sheet for the human correctness pass
    with labels_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "arm", "verdict(correct|partial|incorrect|unsupported)", "note"])
        for q in questions:
            for arm in args.arms:
                w.writerow([q["id"], arm, "", ""])

    print(f"\nwrote {out_path}\nlabel answers in {labels_path}, then run score.py")


if __name__ == "__main__":
    main()
