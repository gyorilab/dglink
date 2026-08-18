"""
Unified "raw CRDC API" MCP server for the DGLink benchmark (single control arm).

Exposes the raw public APIs of all THREE NCI CRDC portals to an agent that has no DGLink
knowledge graph. One arm can therefore answer per-portal *and* cross-portal questions —
the latter is exactly where the baseline suffers, since it must query each portal
separately and join the results itself (the integration DGLink pre-computes).

Portals & their native APIs:
  * GDC  — REST      https://api.gdc.cancer.gov/<endpoint>   (cases, files, projects, genes, ssms, ...)
  * GC   — GraphQL   https://general.datacommons.cancer.gov/v1/graphql/  + Gen3/DRS downloads
  * PDC  — GraphQL   https://pdc.cancer.gov/graphql          + signed-URL file downloads

Deliberately NOT exposed: any of DGLink's curated client methods (the `get_*` on the
portal clients). Those encode the integration work the benchmark measures. The agent gets
only raw query + schema-introspection + file download/preview, and must do the rest.
Reusing `Gen3Client` purely as the GC download transport is fair (public CRDC infra).

GC downloads need a Gen3 credentials file (https://nci-crdc.datacommons.io/login) via
GEN3_CREDENTIAL_FILE. GDC/PDC metadata + GDC open-access file downloads need no auth.

Run standalone (Claude Code launches it via scripts/benchmark/mcp_configs/crdc_api.json):
    python scripts/benchmark/crdc_api_mcp_server.py
"""

import io
import os
import sys
import json
from urllib.parse import urlparse

import requests
import polars as pl
from mcp.server.fastmcp import FastMCP

## make `dglink` importable no matter what cwd Claude Code launches us from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dglink.portals.nci.gc.constants import NCI_GQL_ENDPOINT, NCI_GEN3_ENDPOINT  # noqa: E402
from dglink.portals.nci.pdc.constants import PDC_GQL_ENDPOINT  # noqa: E402
from dglink.portals.nci.gdc.constants import DATA_ENDPNT as GDC_DATA_ENDPNT  # noqa: E402

mcp = FastMCP("crdc_api")

GDC_API_BASE = "https://api.gdc.cancer.gov"
GQL_ENDPOINTS = {"gc": NCI_GQL_ENDPOINT, "pdc": PDC_GQL_ENDPOINT}

_ALLOWED_HOST_SUFFIXES = (
    "cancer.gov",        # api.gdc.cancer.gov, pdc.cancer.gov, general.datacommons.cancer.gov
    "datacommons.io",    # nci-crdc.datacommons.io (Gen3)
    "cloudfront.net",    # PDC signed download urls
    "amazonaws.com",
)
_MAX_CHARS = 20000
_GEN3_CREDENTIAL_FILE = os.environ.get("GEN3_CREDENTIAL_FILE", "nci_general_commons_credentials")


def _host_ok(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == s or host.endswith("." + s) for s in _ALLOWED_HOST_SUFFIXES)


def _truncate(text: str) -> str:
    if len(text) <= _MAX_CHARS:
        return text
    return text[:_MAX_CHARS] + f"\n...[truncated; {len(text)} chars total]"


# ----------------------------------------------------------------------------- GraphQL (GC, PDC)
def _post_gql(portal: str, query: str, variables: dict) -> str:
    if portal not in GQL_ENDPOINTS:
        return f"ERROR: portal must be one of {list(GQL_ENDPOINTS)} (GDC uses gdc_rest)."
    resp = requests.post(
        GQL_ENDPOINTS[portal], json={"query": query, "variables": variables or {}}, timeout=60
    )
    return _truncate(resp.text)


@mcp.tool()
def graphql(portal: str, query: str, variables: dict | None = None) -> str:
    """Run an arbitrary GraphQL query against a portal's public API.

    portal: "gc"  -> NCI General Commons (studies, files, participants, diagnoses, ...)
            "pdc" -> Proteomic Data Commons (allPrograms, biospecimenPerStudy, ...)
    Construct the query yourself; call `graphql_schema` first to discover types/fields.
    (GDC is NOT GraphQL — use `gdc_rest` for GDC.)
    """
    return _post_gql(portal, query, variables or {})


@mcp.tool()
def graphql_schema(portal: str, type_name: str | None = None) -> str:
    """Introspect a GraphQL portal's schema ("gc" or "pdc").

    No `type_name` -> the top-level query entry points. With `type_name` -> that type's
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
    return _post_gql(portal, query, variables)


# ----------------------------------------------------------------------------- REST (GDC)
@mcp.tool()
def gdc_rest(endpoint: str, params: dict | None = None) -> str:
    """GET the GDC REST API (https://api.gdc.cancer.gov/<endpoint>) and return raw JSON.

    endpoint: e.g. "cases", "files", "projects", "genes", "ssms", or "<type>/_mapping"
              to discover the available fields for a type (GDC's schema introspection).
    params:   GDC query params, e.g. {"filters": {...}, "fields": "case_id,project.project_id",
              "expand": "diagnoses,demographic", "size": 10}. `filters` may be a dict (it is
              JSON-encoded for you) or a JSON string. `format` defaults to JSON.
    """
    params = dict(params or {})
    params.setdefault("format", "JSON")
    if isinstance(params.get("filters"), (dict, list)):
        params["filters"] = json.dumps(params["filters"])
    url = f"{GDC_API_BASE}/{endpoint.lstrip('/')}"
    resp = requests.get(url, params=params, timeout=60)
    return _truncate(resp.text)


# ----------------------------------------------------------------------------- files / content
def _preview_bytes(content: bytes, name: str, max_rows: int) -> str:
    lower = name.lower()
    try:
        if lower.endswith((".xlsx", ".xls", ".ods")):
            df = pl.read_excel(io.BytesIO(content))
        elif lower.endswith((".tsv", ".txt", ".maf")) or b"\t" in content[:2000]:
            df = pl.read_csv(io.BytesIO(content), separator="\t", infer_schema_length=1000)
        else:
            df = pl.read_csv(io.BytesIO(content), infer_schema_length=1000)
    except Exception as e:  # noqa: BLE001
        return f"ERROR parsing {name}: {e}\nfirst 300 bytes: {content[:300]!r}"
    return json.dumps(
        {"file": name, "columns": df.columns, "n_rows": df.height,
         "sample_rows": df.head(max_rows).to_dicts()}, default=str,
    )


@mcp.tool()
def download_and_preview_file(portal: str, ref: str, max_rows: int = 20) -> str:
    """Download a data file and return its columns + first `max_rows` rows as JSON — the
    same file *content* DGLink pre-extracts. Deliberately the slow path: first find the
    file via the portal's API, then download here.

    portal / ref:
      "gdc" -> ref is a file UUID; downloaded from https://api.gdc.cancer.gov/data/<uuid>
               (open-access files need no auth).
      "pdc" -> ref is a signed https download URL obtained from a PDC GraphQL query.
      "gc"  -> ref is a Gen3 DRS file_id (e.g. "dg.4DFC/<uuid>") from the GC `files` query;
               requires GEN3_CREDENTIAL_FILE.
    """
    portal = portal.lower()
    try:
        if portal == "gdc":
            url = f"{GDC_DATA_ENDPNT}/{ref}"
            r = requests.get(url, timeout=120)
            name = ref
            cd = r.headers.get("content-disposition", "")
            if "filename=" in cd:
                name = cd.split("filename=")[-1].strip('"; ')
            return _truncate(_preview_bytes(r.content, name, max_rows))

        if portal == "pdc":
            if not _host_ok(ref):
                return f"ERROR: url host not in allowlist {_ALLOWED_HOST_SUFFIXES}"
            r = requests.get(ref, timeout=120)
            return _truncate(_preview_bytes(r.content, ref, max_rows))

        if portal == "gc":
            if not _GEN3_CREDENTIAL_FILE or not os.path.exists(_GEN3_CREDENTIAL_FILE):
                return ("ERROR: set GEN3_CREDENTIAL_FILE to a valid Gen3 credentials.json "
                        "(https://nci-crdc.datacommons.io/login) to download GC files.")
            # Fetch via the commons' fence presigned URL, NOT the gen3 DownloadManager. The
            # DownloadManager expands the `dg.4DFC` DataGUID *prefix* through dataguids.org,
            # which is now sunset (404) — so it fails ("unable to resolve dg.4DFC") even though
            # the object is open-access and resolvable directly on nci-crdc. Fence
            # (GET /user/data/download/<guid>) bypasses that dead resolver.
            from gen3.auth import Gen3Auth
            from gen3.file import Gen3File
            auth = Gen3Auth(endpoint=NCI_GEN3_ENDPOINT, refresh_file=_GEN3_CREDENTIAL_FILE)
            presigned = Gen3File(auth).get_presigned_url(ref)
            url = presigned.get("url") if isinstance(presigned, dict) else None
            if not url:
                return f"ERROR: no presigned URL for gc file_id {ref} (access? id valid? got: {presigned})"
            name = url.split("?")[0].rsplit("/", 1)[-1] or ref
            r = requests.get(url, timeout=120)
            return _truncate(_preview_bytes(r.content, name, max_rows))

        return 'ERROR: portal must be "gdc", "gc", or "pdc".'
    except Exception as e:  # noqa: BLE001
        return f"ERROR downloading {portal}:{ref}: {e}"


@mcp.tool()
def http_get(url: str) -> str:
    """HTTP GET a URL on an allowed CRDC / signed-download host; return the body (truncated).
    For raw text; use `download_and_preview_file` to parse tabular files."""
    if not _host_ok(url):
        return f"ERROR: host not in allowlist {_ALLOWED_HOST_SUFFIXES}"
    return _truncate(requests.get(url, timeout=60).text)


if __name__ == "__main__":
    mcp.run()
