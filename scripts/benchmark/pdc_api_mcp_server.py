"""
Baseline "raw PDC API" MCP server for the DGLink benchmark (control arm).

Exposes the PDC public APIs to an agent that has NO DGLink knowledge graph — just the
raw GraphQL endpoint (+ schema introspection) and the ability to download and preview
tabular data files. Same Claude Code harness as the DGLink arm; the only difference is
the toolset. The agent must discover the schema, write queries, find files, download
them, and scan their contents itself — i.e. do at query time what DGLink does at build
time.

Intentionally *not* exposed: any of DGLink's curated client methods
(``NciProteomicCommonsClient.get_*``). Those encode the very integration work the
benchmark measures, so handing them to the baseline would bias the result. Keep this
server "raw" on purpose.

Run standalone (Claude Code launches it via scripts/benchmark/mcp_configs/pdc_api.json):
    python scripts/benchmark/pdc_api_mcp_server.py
"""

import io
import os
import sys
import json
from urllib.parse import urlparse

import requests
import polars as pl
from mcp.server.fastmcp import FastMCP
# from fast

## make `dglink` importable no matter what cwd Claude Code launches us from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dglink.portals.nci.pdc.constants import PDC_GQL_ENDPOINT  # noqa: E402

mcp = FastMCP("pdc_api")

## host allowlist so the agent can't wander off CRDC / signed-download hosts
_ALLOWED_HOST_SUFFIXES = (
    "cancer.gov",
    "datacommons.cancer.gov",
    "cloudfront.net",  # PDC file downloads come back as cloudfront signed urls
    "amazonaws.com",
)
_MAX_CHARS = 20000  # bound a single tool result so one call can't blow the context


def _host_ok(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == s or host.endswith("." + s) for s in _ALLOWED_HOST_SUFFIXES)


def _truncate(text: str) -> str:
    if len(text) <= _MAX_CHARS:
        return text
    return text[:_MAX_CHARS] + f"\n...[truncated; {len(text)} chars total]"


@mcp.tool()
def pdc_graphql(query: str, variables: dict | None = None) -> str:
    """Run an arbitrary GraphQL query against the PDC public API
    (https://pdc.cancer.gov/graphql) and return the raw JSON response.

    You must construct the query yourself. Call `pdc_graphql_schema` first to discover
    the available entry points, types, and fields.
    """
    resp = requests.post(
        PDC_GQL_ENDPOINT, json={"query": query, "variables": variables or {}}, timeout=60
    )
    return _truncate(resp.text)


@mcp.tool()
def pdc_graphql_schema(type_name: str | None = None) -> str:
    """Introspect the PDC GraphQL schema.

    No argument -> the top-level query entry points (the fields you can start from).
    With `type_name` -> that type's fields and their types, so you can drill in.
    """
    if type_name:
        query = (
            "query($n:String!){ __type(name:$n){ name kind "
            "fields{ name type{ name kind ofType{ name kind } } } } }"
        )
        variables = {"n": type_name}
    else:
        query = (
            "{ __schema { queryType { name fields { name description "
            "args { name } type { name kind ofType { name } } } } } }"
        )
        variables = {}
    resp = requests.post(
        PDC_GQL_ENDPOINT, json={"query": query, "variables": variables}, timeout=60
    )
    return _truncate(resp.text)


@mcp.tool()
def http_get(url: str) -> str:
    """HTTP GET a URL on an allowed CRDC / signed-download host and return the response
    body (truncated). Use for any REST endpoint or to fetch a file's raw text."""
    if not _host_ok(url):
        return f"ERROR: host not in allowlist {_ALLOWED_HOST_SUFFIXES}"
    resp = requests.get(url, timeout=60)
    return _truncate(resp.text)


@mcp.tool()
def download_and_preview_file(url: str, max_rows: int = 20) -> str:
    """Download a tabular data file (CSV/TSV/XLSX) from an allowed host and return its
    column names + the first `max_rows` rows as JSON. This is how you inspect the
    *contents* of a study's files — the same file content DGLink pre-extracts. This is
    deliberately the slow path: you must first find the file's signed URL via GraphQL.
    """
    if not _host_ok(url):
        return f"ERROR: host not in allowlist {_ALLOWED_HOST_SUFFIXES}"
    content = requests.get(url, timeout=120).content
    lower = url.lower()
    try:
        if lower.endswith((".xlsx", ".xls")) or "xlsx" in lower:
            df = pl.read_excel(io.BytesIO(content))
        elif lower.endswith(".tsv") or b"\t" in content[:2000]:
            df = pl.read_csv(io.BytesIO(content), separator="\t")
        else:
            df = pl.read_csv(io.BytesIO(content))
    except Exception as e:  # noqa: BLE001
        return f"ERROR parsing file (first 500 bytes shown): {e}\n{content[:500]!r}"
    preview = {
        "columns": df.columns,
        "n_rows": df.height,
        "sample_rows": df.head(max_rows).to_dicts(),
    }
    return _truncate(json.dumps(preview, default=str))


if __name__ == "__main__":
    mcp.run()
