# DGLink benchmark — KG-backed agent vs. raw-API agent

Measures whether giving an agent the **DGLink knowledge graph** (via MCP) beats giving it
**raw access to the PDC APIs** — on the same questions, in the same harness.

## Design: change exactly one variable

Both arms are the **same Claude Code agent** (`claude -p`, same model, same system prompt,
same turn budget). The *only* difference is the MCP toolset. **The primary target is the
General Commons (GC)** — it has real scale and DGLink genuinely extracts tabular file
content there (unlike the tiny 3-case PDC demo). A PDC arm is kept as an alternative.

| arm | tools | represents |
| --- | --- | --- |
| `dglink` | `neo4j` MCP over the merged CRDC graph (scoped to GC via a prompt suffix) | the DGLink KG |
| `gc_api` | raw GC GraphQL + schema introspection + Gen3/DRS file download/preview | a scientist who just has the GC APIs |
| `pdc_api` | raw PDC GraphQL + file download/preview | the PDC alternative |

The two GC APIs mirror the CRDC surface a scientist actually has:
GraphQL metadata (`general.datacommons.cancer.gov/v1/graphql/`) + the Gen3 / DRS data-pull
API (`nci-crdc.datacommons.io`, "the Swagger data pull" from the meeting notes).

### Why the baseline is "raw + schema", not curated tools
The baseline is intentionally **not** given DGLink's curated client methods
(`NciGeneralCommonsClient.get_diagnoses` / `get_study_files` / ...). Those encode the exact
integration work the benchmark is trying to measure — handing them over would be scoring
DGLink against itself. So the control gets only: run a GraphQL query, introspect the schema,
download a file, preview a file. It must discover the schema, write queries, find files, and
scan contents itself. (Reusing `Gen3Client` purely as the download *transport* is fair —
Gen3/DRS is public CRDC infrastructure, the analogue of giving the agent `requests`.)

### The fairness catch that makes the result defensible
The baseline **can download and read the tabular files** (`download_and_preview_file`).
DGLink's value is the *index over file content*, not that it alone can see files. So the
honest comparison is: both agents can reach the same files; DGLink already knows which
file/entity, the baseline must list → download → scan. If we removed file access from the
baseline we'd be strawmanning ("can read files" vs "can't") instead of measuring
"pre-indexed vs. search-at-query-time". Keep tool #4.

## Metrics

**Objective (no ground truth needed)** — from headless `--output-format json`: `num_turns`
(tool-call effort), input/output tokens, wall-clock, cost. These substantiate "slower and
more work" on their own; report them even where correctness is fuzzy.

**Correctness (human-labelled)** — the runner emits `labels_to_fill.csv`; label each
(question, arm) `correct|partial|incorrect|unsupported`. `score.py` reports per-arm
accuracy against these **curated** verdicts as the PRIMARY number.

**Agreement analysis (secondary, caveated)** — the "found by A but not B" view from the
meeting notes is also printed, but it treats the *union* of the two agents as ground truth,
so it ignores anything **both** miss and is circular for the headline claim. `score.py`
labels it SECONDARY and surfaces the "neither" bucket. Don't lead with it.

## Run it

```bash
# 0. prereqs: `claude` and `uvx` on PATH; venv at ./.venv (the mcp_configs point at
#    .venv/bin/python, relative to the repo root); neo4j up with the merged graph
#    loaded (dglink arm).
#    For GC file downloads: export GEN3_CREDENTIAL_FILE=/path/to/credentials.json
#    (free, from https://nci-crdc.datacommons.io/login). The mcp_configs read it from the
#    environment as ${GEN3_CREDENTIAL_FILE}; if your client does not expand env vars in
#    MCP config, put the absolute path in mcp_configs/{gc,crdc}_api.json instead.
pip install mcp polars requests gen3       # if not already in your env

# 1. (optional) smoke-test the baseline MCP server starts
python scripts/benchmark/gc_api_mcp_server.py    # ctrl-c; should sit waiting on stdio

# 2. run both arms over the CRDC question set (default arms: dglink + crdc_api)
python scripts/benchmark/run_benchmark.py --model sonnet --max-turns 30
#    -> results.jsonl + labels_to_fill.csv (both gitignored; regenerate per run)
#    (GC only:  --arms dglink gc_api  --questions scripts/benchmark/questions_gc.jsonl)
#    (PDC only: --arms dglink pdc_api --questions scripts/benchmark/questions_gc.jsonl)

# 3. label answers in labels_to_fill.csv, then
python scripts/benchmark/score.py
```

## Files
- `crdc_api_mcp_server.py` — baseline raw-API MCP server spanning GDC + GC + PDC. **Primary baseline.**
- `gc_api_mcp_server.py` — single-portal GC baseline (GraphQL + schema + Gen3 download).
- `pdc_api_mcp_server.py` — single-portal PDC baseline.
- `mcp_configs/{crdc_api,gc_api,pdc_api,dglink_neo4j}.json` — one MCP server per arm (`--mcp-config` format).
- `questions_crdc.jsonl` — cross-portal question set (default). `questions_gc.jsonl` — GC set.
  `questions_panel.jsonl` — the 20-gene panel task. `kind` tags file-content vs metadata.
- `gold_gene_panel.{json,tsv}` — independent gold standard for the panel task, built by
  `build_gold_table.py` (which caches per-file scans in `gold_scan_cache.jsonl`).
- `gen_panel_questions.py` — regenerates `questions_panel.jsonl` from the gold panel.
- `run_benchmark.py` — runs each question × arm headless, logs objective metrics.
- `score.py` — objective aggregates + curated correctness + caveated agreement analysis.

### Generated, not checked in
`results.jsonl`, `labels_to_fill.csv` and `score_results.txt` are run outputs and are
gitignored — rerun `run_benchmark.py` and `score.py` to regenerate them.

### Frozen snapshot behind the published numbers
- `results_published.jsonl` — the raw run (8 records) whose figures and verbatim agent
  answers are quoted on the DGLink Overview page's benchmark section.
- `score_results_published.txt` — `score.py` output for that run: the P/R/F1 numbers
  (dglink F1 0.92 vs crdc_api F1 0.44) shown on that page.

These are committed deliberately, so the public claims have in-repo provenance. They are
*not* the gitignored live filenames, so a rerun can never silently overwrite them. Both were
produced with `--model sonnet` (resolves to `claude-sonnet-5`); the model is not recorded in
the records themselves, which is worth fixing in `run_one` before the next run.

Caveat on the snapshot: the `panel_cananolab`/`dglink` arm appears three times because that
question was rerun during development, and `score.py` reports each. The published F1 refers
to the run whose `PRESENT:` line lists 12 genes.

## To verify / tune before a real run
- Confirm CLI flags against your version: `claude --help` (esp. `--mcp-config`,
  `--allowedTools`, `--append-system-prompt`, `--max-turns`, JSON fields `num_turns`/`usage`).
- Pin a single `--model` for every run so the arms are comparable.
- For exact tool-call counts (vs. the `num_turns` proxy), switch the runner to
  `--output-format stream-json --verbose` and count `tool_use` events.
- Grow `questions_crdc.jsonl` to ~10–15 and curate ground truth for as many as you can.
