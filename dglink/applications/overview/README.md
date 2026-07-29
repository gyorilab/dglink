# Overview Application — DGLink landing page

A single-page website that acts as the hub for the DGLink tool suite. It shows:

1. **About DGLink** — a short description of what DGLink does.
2. **Graph summary statistics** — node/edge counts, entity- and relation-type
   breakdowns, and per-portal coverage for the active knowledge graph (by
   default the merged CRDC graph, `dglink/applications/mcp/graph_crdc_merged`).
3. **Inside the merge** — the cross-portal story, computed live from the loaded
   graph: how many genes converge across all three CRDC portals, and a table
   breaking down *extracted-from-files* vs. *exposed-metadata* nodes for each
   portal-membership combination (each single portal, each overlap/"bridge",
   and the total). Inspired by `graph_crdc_merged/demo_crdc_merge.ipynb` (§8).
4. **Open in Neo4j Browser** — a one-click link to the Neo4j Browser web-view
   plus a copy-paste sample Cypher. The neo4j image ships with authentication
   disabled, so **no login is required** — just click *Connect*.
5. **About the MCP application** — what the Chat Assistant does and how it uses
   the Model Context Protocol to query Neo4j.
6. **Example MCP queries** — curated natural-language questions (from the CRDC
   benchmark) that click to load into the query box.
7. **A live query box** — run natural-language MCP queries directly from the
   page; the reply streams back in real time.

All statistics — including the "Inside the merge" numbers — are recomputed from
the graph TSVs at startup, so they always reflect whichever graph is loaded.

## Architecture

This is a single Flask frontend service (`frontend/`). It:

- reads the graph TSVs mounted at `/app/resources` (i.e. `${NEO4J_TARGET}/graph`)
  once at startup to compute the summary statistics — no database connection
  needed for stats; and
- proxies the live query box to the existing **MCP backend** `/chat` endpoint
  (the same service that powers the Chat Assistant), so it reuses the running
  LLM + Neo4j-over-MCP stack rather than duplicating it.

Because it depends on `mcp_backend`, the live query box needs the MCP backend
(and therefore Neo4j and an API key) to be up. The rest of the page — including
all statistics — renders without it.

## Running

The service is wired into the top-level compose stack
(`dglink/applications/compose.yaml`). From `dglink/applications/`:

```bash
docker compose up --build
```

Then open the Overview page at **http://localhost:5003**.

### Relevant environment variables (`.env`)

| Variable            | Purpose                                              |
| ------------------- | ---------------------------------------------------- |
| `NEO4J_TARGET`      | Graph whose TSVs are mounted for the statistics.     |
| `MCP_BACKEND_URL`   | Backend the query box proxies to (default `mcp_backend:8000`). |
| `GRAPH_NAME`        | Human-readable graph name shown in the stats header. |
| `NEO4J_BROWSER_URL` | Neo4j Browser web-view link (default `http://localhost:7474`, login-free). |
| `FONT_SIZE`         | Font scale for the query box (shared with the chat). |
| `NAV_*_URL`         | Cross-tool navigation links.                         |

## Ports

| Service             | Port |
| ------------------- | ---- |
| Overview            | 5003 |
| Chat Assistant      | 5000 |
| Query Builder       | 5001 |
| Sequence Search     | 5002 |
