"""DGLink Overview — a landing page for the DGLink tool suite.

Serves a single info page that describes DGLink, shows summary statistics for
the active knowledge graph (by default the merged CRDC graph mounted at
``/app/resources``), documents the MCP chat application, lists example MCP
queries, and exposes a box that runs those queries live against the MCP
backend.

The query box proxies to the same MCP backend ``/chat`` endpoint that the Chat
Assistant frontend uses, so answers stream back in real time.
"""

import csv
import json
import os
from collections import Counter, defaultdict
from urllib.parse import quote

from flask import Flask, render_template, request, jsonify, Response
import requests

app = Flask(__name__)

# Backend that runs the natural-language questions against Neo4j over MCP. This
# is the same service the Chat Assistant frontend talks to.
MCP_BACKEND_URL = os.getenv('MCP_BACKEND_URL', 'http://mcp_backend:8009')

# Graph TSVs. docker-compose mounts ${NEO4J_TARGET}/graph here, so the stats
# always reflect whichever graph the rest of the stack is serving.
GRAPH_DIR = os.getenv('GRAPH_DIR', '/app/resources')

# Shared DGLink navigation (see mcp/frontend/app.py). Defaults to the
# docker-compose localhost ports; override via env for other deployments.
NAV = {
    'overview': os.getenv('NAV_OVERVIEW_URL', 'http://localhost:5003/'),
    'chat': os.getenv('NAV_CHAT_URL', 'http://localhost:5005/'),
    'sequence': os.getenv('NAV_SEQUENCE_URL', 'http://localhost:5002/'),
    'query': os.getenv('NAV_QUERY_URL', 'http://localhost:5001/'),
}

# When false, the Sequence Search tab is hidden in the nav everywhere.
SHOW_SEQUENCE_SEARCH = os.getenv('SHOW_SEQUENCE_SEARCH', 'true').strip().lower() in ('1', 'true', 'yes', 'on')

# When false, every reference to the MCP chat interface is hidden: the Chat
# Assistant nav link, the chat write-up, and the live "Run an MCP query" box
# (which proxies to the same MCP backend). Disabled for deployments where the
# LLM-writes-Cypher chat interface should not be exposed.
SHOW_CHAT = os.getenv('SHOW_CHAT', 'true').strip().lower() in ('1', 'true', 'yes', 'on')

# Human-readable name of the graph the stats describe.
GRAPH_NAME = os.getenv('GRAPH_NAME', 'Merged CRDC graph (NCI GDC · GC · PDC)')

# Neo4j Browser web-view. The neo4j image ships with authentication disabled
# (the Dockerfile uncomments ``dbms.security.auth_enabled=false``), so this URL
# opens the graph with no login required — one "Connect" click. Override for
# non-localhost deployments.
NEO4J_BROWSER_URL = os.getenv('NEO4J_BROWSER_URL', 'http://localhost:7474')

# Bolt URL (with scheme) passed to the browser as ``connectURL`` to pre-fill the connection form.
NEO4J_BOLT_URL = os.getenv('NEO4J_BOLT_URL', 'bolt://localhost:7687')

# Browser link with the bolt address pre-filled via ``connectURL`` (encode so the ``+`` in bolt+s survives).
_sep = '&' if '?' in NEO4J_BROWSER_URL else '?'
NEO4J_BROWSER_CONNECT_URL = (
    f"{NEO4J_BROWSER_URL}{_sep}connectURL={quote(NEO4J_BOLT_URL, safe='')}"
    if NEO4J_BOLT_URL else NEO4J_BROWSER_URL
)

# Public MCP endpoint (the `mcp_http_server` service) that external clients connect to
# by URL alone. Shown verbatim in the connection instructions, so it must be the address
# as reachable from outside the container — override for a hosted deployment.
MCP_HTTP_URL = os.getenv('MCP_HTTP_URL', 'http://localhost:8009/mcp/')

# The three CRDC portals whose convergence is the point of the merge. Used to
# compute the cross-portal overlap and the "shared across all three" headline.
CRDC_PORTALS = ('gc', 'gdc', 'pdc')

# Display order for the portals across the page (summary cards, per-type tables and
# the coverage section). Portals not listed here fall back to size order after these.
PORTAL_ORDER = ('gc', 'gdc', 'pdc')

# How much of each portal DGLink actually processed. DGLink extracts from the
# data files a portal makes downloadable, so coverage differs by portal. The
# prose is editorial (demo framing); the counts beside it are computed live from
# the graph so they never drift. ``primary`` picks which metric leads the card.
COVERAGE_NOTES = {
    'gc': {
        'scope': 'Full open-access coverage',
        'variant': 'full',
        'primary': 'open',
        # Known number of studies in the NCI General Commons. The merged graph holds more
        # Study nodes for GC (controlled-access + null-access specimens carry study ids),
        # so the headline total is set here rather than counted from the graph.
        'total_studies': 52,
        'note': ("Every open-access study in the NCI General Commons was processed end to "
                 "end — DGLink extracted content from all of their data files. "
                 "Controlled-access studies appear as metadata only."),
    },
    'gdc': {
        'scope': 'Proof-of-concept sample',
        'variant': 'poc',
        'primary': 'studies',
        'note': ("A proof-of-concept subset of studies and cases was processed to demonstrate "
                 "genomic coverage — not the entire commons."),
    },
    'pdc': {
        'scope': 'Proof-of-concept sample',
        'variant': 'poc',
        'primary': 'studies',
        'note': ("A proof-of-concept subset of studies and cases was processed to demonstrate "
                 "proteomic coverage — not the entire commons."),
    },
}

# ``source`` values that mark a node as content DGLink *extracted from files*,
# as opposed to metadata the portals already expose. Mirrors the notebook
# (demo_crdc_merge.ipynb, §7-8).
EXTRACTED_SOURCES = {'tabular_data', 'experimental_data'}

# A copy-paste Cypher that renders the demo's headline in Neo4j Browser as a small,
# tidy star: the oncogene EGFR in the middle, with the first two records from each
# CRDC portal that report it fanning out around it — one gene, three commons. Taking
# two per portal keeps it balanced (PDC alone has ~30 studies for EGFR, which would
# otherwise swamp GC's two). GC + PDC contribute studies; GDC's gene links are
# per-case, so its two nodes are cases. Shown next to the browser link.
BROWSER_SAMPLE_CYPHER = (
    "// EGFR linked to the first 2 studies/cases from each CRDC portal (GC + GDC + PDC)\n"
    "MATCH (a)-[r:`biolink:related_to`]->(g:`biolink:Gene` {name: 'EGFR'})\n"
    "WITH a, g, r, a.portal[0] AS portal\n"
    "WITH portal, collect(r)[0..2] AS rels\n"
    "UNWIND rels AS r\n"
    "RETURN startNode(r) AS a, r, endNode(r) AS g\n"
)

# Curated natural-language MCP queries shown on the landing page. Ordered to lead
# with the cross-portal convergence that is the whole point of the merge, then dig
# into content DGLink extracted from data files (with provenance) rather than the
# trivial metadata the portals already expose. Each is verified to return a clean,
# bounded, non-degenerate answer on the merged CRDC graph.
EXAMPLE_QUERIES = [
    "How many genes are observed across all three CRDC portals (GDC, GC, and PDC) at once — and are landmark cancer genes like TP53, EGFR, and PTEN among them?",
    "Trace TP53 across the three portals: which studies and cases reference it, and in what role, based on the source data-file column each mention was extracted from?",
    "Which genes are called copy-number drivers in PDC proteogenomic studies and are also present in the Genomic Data Commons? Name a few, with the studies they came from.",
    "For glioblastoma, how many GDC genomic cases and PDC proteomic studies converge on the same disease concept?",
    "Which cancer genes appear in BOTH a General Commons (GC) study's data files AND a PDC proteomic study? List a few of the shared genes.",
    "Which genes have acetylation or glycosylation site data in PDC data files AND are also called copy-number drivers? List a few, with the columns the evidence came from.",
    "For TP53, show the exact source data file and column that each mention was extracted from, per portal.",
]

# Saved agent responses for the example queries above, so the page can show a real
# answer without a live LLM call (the CRDC demo runs with SHOW_CHAT off). Produced by
# running each question against the KG agent over the neo4j MCP server; keyed by the
# exact question text, so a question with no saved answer simply renders no dropdown.
EXAMPLE_ANSWERS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'example_answers.json'
)


def _load_example_answers():
    try:
        with open(EXAMPLE_ANSWERS_PATH) as f:
            answers = json.load(f)
    except (OSError, ValueError) as exc:
        print(f'No example answers loaded from {EXAMPLE_ANSWERS_PATH}: {exc}')
        return {}
    # The answer is markdown, emitted verbatim into a <script type="text/markdown">
    # block and parsed client-side, so it is injected with |safe (HTML-escaping it
    # would leak entities into the rendered markdown — script is a raw-text element).
    # A literal </script> in the text would therefore close the block early: neutralise
    # it here, which is the one sequence that can break out.
    for record in answers.values():
        answer = record.get('answer')
        if isinstance(answer, str):
            record['answer'] = answer.replace('</script', r'<\/script')
    print(f'Loaded {len(answers)} saved example answers.')
    return answers


EXAMPLE_ANSWERS = _load_example_answers()


def _read_tsv_header_and_rows(path):
    """Yield (header list, row iterator) for a large TSV without loading it all."""
    csv.field_size_limit(10 ** 7)  # graph cells (descriptions) can be long
    f = open(path, newline='')
    reader = csv.reader(f, delimiter='\t')
    header = next(reader)
    return header, reader, f


def _expand_portals(cell):
    """Portal cells look like ``"gc;gdc;pdc"`` (quoted, semicolon-joined)."""
    cell = (cell or '').strip().strip('"')
    return [p for p in cell.split(';') if p]


# ``source`` cells are quoted, semicolon-joined lists too (e.g.
# ``"experimental_data;tabular_data"``) — same shape as portals.
_split_list = _expand_portals


def compute_stats(graph_dir=GRAPH_DIR):
    """Summarize the graph TSVs into the numbers the landing page renders.

    Returns None if the graph files are not present, so the page can degrade
    gracefully rather than crash.
    """
    nodes_path = os.path.join(graph_dir, 'nodes.tsv')
    edges_path = os.path.join(graph_dir, 'edges.tsv')
    if not (os.path.exists(nodes_path) and os.path.exists(edges_path)):
        return None

    # --- nodes ---
    labels = Counter()
    node_portal_presence = Counter()   # nodes counted once per portal they touch
    # Per entity type: how many of its nodes touch each portal (notebook §1), and
    # how many are shared across >1 portal (the per-type cross-portal overlap).
    label_portal = defaultdict(Counter)
    label_shared = Counter()
    # Open-access studies processed, per portal (drives the coverage note for GC).
    open_studies = Counter()
    multi_portal_nodes = 0
    node_total = 0
    # The headline: genes present in all three CRDC portals (notebook §3).
    three_way_genes = 0
    # Provenance split by portal membership: for each combination of portals a
    # node belongs to (``gdc``, ``gdc + pdc``, all three, …), how many nodes are
    # content DGLink extracted from files vs. metadata the portals expose. This
    # is the notebook §8 breakdown. Keyed by combo string -> [extracted, metadata].
    combo_prov = {}
    # curie -> (label, [portals]) for Study/Case nodes only, so the edge pass can map
    # each extraction edge back to the portal(s) of the case/study it hangs off.
    study_case_portal = {}
    header, rows, f = _read_tsv_header_and_rows(nodes_path)
    ci = header.index('curie:ID')
    li = header.index(':LABEL')
    pi = header.index('portal:string[]') if 'portal:string[]' in header else None
    si = header.index('source:string[]') if 'source:string[]' in header else None
    ai = header.index('study_access') if 'study_access' in header else None
    for row in rows:
        node_total += 1
        label = row[li].replace('biolink:', '') if li < len(row) else ''
        if label:
            labels[label] += 1

        portals = _expand_portals(row[pi]) if (pi is not None and pi < len(row)) else []
        if label in ('Study', 'Case') and ci < len(row):
            study_case_portal[row[ci]] = (label, portals)
        is_open_study = (label == 'Study' and ai is not None and ai < len(row)
                         and row[ai].strip().strip('"').lower() == 'open')
        for p in portals:
            node_portal_presence[p] += 1
            if label:
                label_portal[label][p] += 1
            if is_open_study:
                open_studies[p] += 1
        if len(portals) > 1:
            multi_portal_nodes += 1
            if label:
                label_shared[label] += 1
        if label == 'Gene' and all(p in portals for p in CRDC_PORTALS):
            three_way_genes += 1

        # Only nodes carrying a portal contribute to the membership breakdown
        # (matches the notebook's ``WHERE n.portal IS NOT NULL``).
        if portals:
            is_extracted = bool(si is not None and si < len(row)
                                and set(_split_list(row[si])) & EXTRACTED_SOURCES)
            bucket = combo_prov.setdefault(' + '.join(sorted(portals)), [0, 0])
            bucket[0 if is_extracted else 1] += 1
    f.close()

    # --- edges ---
    edge_types = Counter()
    edge_portal = Counter()
    edge_type_portal = defaultdict(Counter)   # per relationship type, per portal
    edge_total = 0
    # What DGLink actually extracted content from, per portal. related_to is the sole
    # extraction predicate (source ``experimental_data;tabular_data``); it hangs off the
    # case (GDC) or study (GC/PDC) it was mined from and carries the source file ids.
    extracted_cases = defaultdict(set)    # portal -> {case curie}
    extracted_studies = defaultdict(set)  # portal -> {study curie}
    extracted_files = defaultdict(set)    # portal -> {file id}
    header, rows, f = _read_tsv_header_and_rows(edges_path)
    ti = header.index(':TYPE')
    pi = header.index('portal:string[]') if 'portal:string[]' in header else None
    starti = header.index(':START_ID')
    fi = header.index('file_id:string[]') if 'file_id:string[]' in header else None
    for row in rows:
        edge_total += 1
        etype = row[ti].replace('biolink:', '') if ti < len(row) else ''
        if etype:
            edge_types[etype] += 1
        if pi is not None and pi < len(row):
            for p in _expand_portals(row[pi]):
                edge_portal[p] += 1
                if etype:
                    edge_type_portal[etype][p] += 1
        # Extraction provenance: attribute the source node + its files to the portal(s)
        # of the case/study the related_to edge originates from.
        if etype == 'related_to' and starti < len(row):
            src = study_case_portal.get(row[starti])
            if src:
                src_label, src_portals = src
                files = _split_list(row[fi]) if (fi is not None and fi < len(row)) else []
                for p in src_portals:
                    if src_label == 'Case':
                        extracted_cases[p].add(row[starti])
                    elif src_label == 'Study':
                        extracted_studies[p].add(row[starti])
                    extracted_files[p].update(files)
    f.close()

    portal_labels = {'gc': 'General Commons', 'gdc': 'Genomic Data Commons', 'pdc': 'Proteomic Data Commons'}

    # PORTAL_ORDER first (GC, GDC, PDC), then any others by descending node count.
    def _portal_sort_key(p):
        return (PORTAL_ORDER.index(p), 0) if p in PORTAL_ORDER else (len(PORTAL_ORDER), -node_portal_presence[p])

    portals = [
        {
            'key': p,
            'label': portal_labels.get(p, p.upper()),
            'nodes': node_portal_presence.get(p, 0),
            'edges': edge_portal.get(p, 0),
        }
        for p in sorted(node_portal_presence, key=_portal_sort_key)
    ]

    # Extracted-vs-metadata breakdown, one row per portal-membership combo
    # (single portals and overlaps), sorted by size, plus an "all portals" total.
    def _pct(part, other):
        total = part + other
        return round(100 * part / total, 1) if total else 0

    prov_rows = []
    tot_extracted = tot_metadata = 0
    for combo, (extracted, metadata) in combo_prov.items():
        tot_extracted += extracted
        tot_metadata += metadata
        prov_rows.append({
            'combo': ' + '.join(part.upper() for part in combo.split(' + ')),
            'portals_count': len(combo.split(' + ')),
            'extracted': extracted,
            'metadata': metadata,
            'total': extracted + metadata,
            'pct_extracted': _pct(extracted, metadata),
        })
    prov_rows.sort(key=lambda r: -r['total'])

    provenance_breakdown = {
        'rows': prov_rows,
        'total': {
            'extracted': tot_extracted,
            'metadata': tot_metadata,
            'total': tot_extracted + tot_metadata,
            'pct_extracted': _pct(tot_extracted, tot_metadata),
        },
    }

    # Ordered portal keys (most-populated first) — the columns of the per-type
    # tables and the header labels shown for them.
    portal_keys = [p['key'] for p in portals]
    portal_col_labels = {p['key']: p['key'].upper() for p in portals}

    # Per entity type: total, per-portal presence, and cross-portal overlap
    # (nodes of this type shared by >1 portal). Sorted by total, descending.
    node_type_rows = [
        {
            'name': name,
            'total': total,
            'by_portal': {k: label_portal[name].get(k, 0) for k in portal_keys},
            'shared': label_shared.get(name, 0),
            'pct': round(100 * total / node_total, 1) if node_total else 0,
        }
        for name, total in labels.most_common()
    ]
    # Per relationship type: total and per-portal presence.
    edge_type_rows = [
        {
            'name': name,
            'total': total,
            'by_portal': {k: edge_type_portal[name].get(k, 0) for k in portal_keys},
            'pct': round(100 * total / edge_total, 1) if edge_total else 0,
        }
        for name, total in edge_types.most_common()
    ]

    # Processing coverage per portal: editorial scope + live counts of what was
    # actually processed (open-access studies for GC, sampled studies/cases for
    # the proof-of-concept portals).
    processing_coverage = []
    for k in portal_keys:
        note = COVERAGE_NOTES.get(k)
        if not note:
            continue
        if k == 'gc':
            # Study-centric: the General Commons is covered at the study level.
            metrics = [
                {'value': note.get('total_studies', label_portal['Study'].get(k, 0)),
                 'label': 'studies (total)'},
                {'value': open_studies.get(k, 0), 'label': 'open-access studies'},
                {'value': len(extracted_studies.get(k, ())), 'label': 'studies with extracted content'},
            ]
        else:
            # Case-centric (GDC, PDC): total cases + what we actually extracted from.
            # Extraction attaches at the case level in GDC and the study level in PDC, so
            # show whichever direct-extraction count applies (label matches the unit).
            if extracted_cases.get(k):
                ex_val, ex_label = len(extracted_cases[k]), 'cases with extracted content'
            else:
                ex_val, ex_label = len(extracted_studies.get(k, ())), 'studies with extracted content'
            metrics = [
                {'value': label_portal['Case'].get(k, 0), 'label': 'cases (total)'},
                {'value': ex_val, 'label': ex_label},
            ]
        processing_coverage.append({
            'key': k,
            'label': portal_labels.get(k, k.upper()),
            'scope': note['scope'],
            'variant': note['variant'],
            'note': note['note'],
            'metrics': metrics,
        })

    return {
        'node_total': node_total,
        'edge_total': edge_total,
        'label_count': len(labels),
        'edge_type_count': len(edge_types),
        'multi_portal_nodes': multi_portal_nodes,
        'portals': portals,
        'portal_keys': portal_keys,
        'portal_col_labels': portal_col_labels,
        'node_type_rows': node_type_rows,
        'edge_type_rows': edge_type_rows,
        'processing_coverage': processing_coverage,
        'three_way_genes': three_way_genes,
        'provenance_breakdown': provenance_breakdown,
    }


# Compute once at startup — the graph is static for the life of the container.
try:
    STATS = compute_stats()
except Exception as exc:  # pragma: no cover - defensive, don't crash the page
    print(f"Failed to compute graph stats: {exc}")
    STATS = None


@app.route('/')
def index():
    return render_template(
        'index.html',
        nav=NAV,
        active='overview',
        stats=STATS,
        graph_name=GRAPH_NAME,
        examples=EXAMPLE_QUERIES,
        example_answers=EXAMPLE_ANSWERS,
        show_sequence=SHOW_SEQUENCE_SEARCH,
        show_chat=SHOW_CHAT,
        neo4j_browser_url=NEO4J_BROWSER_CONNECT_URL,
        mcp_http_url=MCP_HTTP_URL,
        browser_sample_cypher=BROWSER_SAMPLE_CYPHER,
    )


@app.route('/run', methods=['POST'])
def run():
    """Proxy a natural-language MCP query to the backend and stream the reply."""
    try:
        data = request.json or {}
        response = requests.post(
            f'{MCP_BACKEND_URL}/chat',
            json={
                'message': data.get('message', ''),
                'provider': data.get('provider', 'anthropic'),
            },
            stream=True,
            timeout=300,
        )

        def generate():
            for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
                if chunk:
                    yield chunk

        return Response(generate(), mimetype='text/plain')
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'stats_loaded': STATS is not None,
        'mcp_backend': MCP_BACKEND_URL,
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)
