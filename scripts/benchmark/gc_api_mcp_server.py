"""
Baseline "raw General Commons (GC) API" MCP server for the DGLink benchmark (control arm).

Exposes the NCI CRDC General Commons public APIs to an agent that has NO DGLink knowledge
graph — just the raw GraphQL endpoint (+ schema introspection) and the Gen3 data-pull API
for downloading + previewing tabular files. Same Claude Code harness as the DGLink arm; the
only difference is the toolset. The agent must discover the schema, write queries, find a
study's files, download them, and scan their contents itself — i.e. do at query time what
DGLink does at build time.

The two APIs mirror the CRDC surface a scientist actually has:
  * GraphQL metadata API  -> https://general.datacommons.cancer.gov/v1/graphql/
  * Gen3 / DRS data-pull   -> https://nci-crdc.datacommons.io  (signed file download)

Intentionally *not* exposed: DGLink's curated client methods
(``NciGeneralCommonsClient.get_diagnoses`` / ``get_study_files`` / ...). Those encode the
integration work the benchmark measures, so handing them to the baseline would bias it.
Reusing ``Gen3Client`` purely as the download transport is fair — Gen3/DRS is public CRDC
infrastructure, the analogue of giving the agent `requests`.

Gen3 downloads need a (free) credential file from https://nci-crdc.datacommons.io/login —
point GEN3_CREDENTIAL_FILE at it. Without it, the metadata (GraphQL) tools still work.

Run standalone (Claude Code launches it via scripts/benchmark/mcp_configs/gc_api.json):
    python scripts/benchmark/gc_api_mcp_server.py
"""

import io
import os
import sys
import json
import tempfile
from urllib.parse import urlparse

import requests
import polars as pl
from mcp.server.fastmcp import FastMCP

## make `dglink` importable no matter what cwd Claude Code launches us from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dglink.portals.nci.gc.constants import (  # noqa: E402
    NCI_GQL_ENDPOINT,
    NCI_GEN3_ENDPOINT,
)

mcp = FastMCP("gc_api")

_ALLOWED_HOST_SUFFIXES = (
    "cancer.gov",
    "datacommons.cancer.gov",
    "datacommons.io",
    "cloudfront.net",
    "amazonaws.com",
)
_MAX_CHARS = 20000
_GEN3_CREDENTIAL_FILE = 'nci_general_commons_credentials'  # optional; needed for downloads


def _host_ok(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == s or host.endswith("." + s) for s in _ALLOWED_HOST_SUFFIXES)


def _truncate(text: str) -> str:
    if len(text) <= _MAX_CHARS:
        return text
    return text[:_MAX_CHARS] + f"\n...[truncated; {len(text)} chars total]"


def _post_gql(query: str, variables: dict) -> str:
    resp = requests.post(
        NCI_GQL_ENDPOINT, json={"query": query, "variables": variables or {}}, timeout=60
    )
    return _truncate(resp.text)


@mcp.tool()
def gc_graphql(query: str, variables: dict | None = None) -> str:
    """Run an arbitrary GraphQL query against the NCI General Commons API
    (https://general.datacommons.cancer.gov/v1/graphql/) and return the raw JSON response.

    You must construct the query yourself. Call `gc_graphql_schema` first to discover the
    available entry points (e.g. `studies`, `files`, `diagnoses`), types, and fields.
    """
    return _post_gql(query, variables or {})


@mcp.tool()
def gc_graphql_schema(type_name: str | None = None) -> str:
    """Introspect the GC GraphQL schema.

    No argument -> the top-level query entry points. With `type_name` -> that type's
    fields and their types, so you can drill in.
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
    return _post_gql(query, variables)


@mcp.tool()
def http_get(url: str) -> str:
    """HTTP GET a URL on an allowed CRDC / signed-download host and return the response
    body (truncated). Use for a REST endpoint or a directly-fetchable file URL."""
    if not _host_ok(url):
        return f"ERROR: host not in allowlist {_ALLOWED_HOST_SUFFIXES}"
    return _truncate(requests.get(url, timeout=60).text)


def _preview_path(path: str, max_rows: int) -> str:
    lower = path.lower()
    try:
        if lower.endswith((".xlsx", ".xls", ".ods")):
            df = pl.read_excel(path)
        elif lower.endswith((".tsv", ".txt")):
            df = pl.read_csv(path, separator="\t", infer_schema_length=1000)
        else:
            df = pl.read_csv(path, infer_schema_length=1000)
    except Exception as e:  # noqa: BLE001
        return f"ERROR parsing {os.path.basename(path)}: {e}"
    return json.dumps(
        {"file": os.path.basename(path), "columns": df.columns,
         "n_rows": df.height, "sample_rows": df.head(max_rows).to_dicts()},
        default=str,
    )


@mcp.tool()
def download_and_preview_file(file_id: str, max_rows: int = 20) -> str:
    """Download a GC data file by its Gen3 file_id (a DRS object id, discoverable via the
    `files` GraphQL query) and return its column names + first `max_rows` rows as JSON.

    This is how you inspect the *contents* of a study's files — the same content DGLink
    pre-extracts. It is deliberately the slow path: first find the file_id via GraphQL,
    then download here. Requires GEN3_CREDENTIAL_FILE to be set.
    """
    if not _GEN3_CREDENTIAL_FILE:
        return ("ERROR: set GEN3_CREDENTIAL_FILE to a Gen3 credentials.json "
                "(from https://nci-crdc.datacommons.io/login) to enable downloads.")
    try:
        # Fetch via the commons' fence presigned URL, NOT the gen3 DownloadManager. The
        # DownloadManager expands the `dg.4DFC` DataGUID prefix through dataguids.org, which is
        # sunset (404) — so it fails even though the object is open-access and resolvable
        # directly on nci-crdc. Fence (GET /user/data/download/<guid>) bypasses that resolver.
        from gen3.auth import Gen3Auth
        from gen3.file import Gen3File
        auth = Gen3Auth(endpoint=NCI_GEN3_ENDPOINT, refresh_file=_GEN3_CREDENTIAL_FILE)
        presigned = Gen3File(auth).get_presigned_url(file_id)
        url = presigned.get("url") if isinstance(presigned, dict) else None
        if not url:
            return f"ERROR: no presigned URL for file_id {file_id} (access? id valid? got: {presigned})"
        name = url.split("?")[0].rsplit("/", 1)[-1] or file_id
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, name)
            with open(path, "wb") as fh:
                fh.write(requests.get(url, timeout=120).content)
            return _truncate(_preview_path(path, max_rows))
    except Exception as e:  # noqa: BLE001
        return f"ERROR downloading {file_id}: {e}"


if __name__ == "__main__":
    mcp.run()
