"""
Merge multiple single-portal knowledge graphs into one, tagging provenance.

Within a single-portal graph the portal is constant, so it is *not* stored on the
nodes/edges -- it would be pure redundancy. Portal provenance only becomes
informative once graphs are merged, so it is added here at merge time:

  * every node/edge gains a ``portal:string[]`` attribute recording which
    portal(s) it came from. Because it is a multivalued (``string[]``) attribute
    it set-unions across inputs, so a shared entity (e.g. an ``hgnc:`` gene found
    in both portals) ends up tagged with *every* portal that contributed it.
  * every ``file_id`` is prefixed with its portal (e.g. ``gdc:<uuid>``) so that a
    node reached through many files keeps an unambiguous file -> portal mapping
    even after the multivalued ``file_id`` sets are unioned together.

Shared nodes/edges merge with the NodeSet/EdgeSet set-union semantics
(``update_nodes`` / ``update_edges`` union the ``string[]`` columns). Scalar
attributes are first-writer-wins -- so graphs are folded in the order given -- and
any disagreement on a shared id (notably ``:LABEL`` / ``:TYPE``) is logged.

Usage:
    python -m scripts.merge_graphs           # merges the default NCI graphs
    # or import merge_graphs(...) and pass your own {dir: portal} mapping
"""

import logging

from dglink import NodeSet, EdgeSet, load_graph, write_graph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PORTAL_ATTR = "portal:string[]"
FILE_ID_ATTR = "file_id:string[]"


def _ordered_union(attribute_lists: list) -> list:
    """merges set of attrs maintaining order"""
    merged = []
    for attrs in attribute_lists:
        for a in attrs:
            if a not in merged:
                merged.append(a)
    return merged


def _tag_provenance(rep: dict, portal: str) -> dict:
    """stamp portal + portal-prefix the file ids on a (copied) node/edge rep."""
    rep = dict(rep)
    rep[PORTAL_ATTR] = {portal}
    if FILE_ID_ATTR in rep:
        file_ids = rep[FILE_ID_ATTR]
        if isinstance(file_ids, str):
            file_ids = {file_ids}
        rep[FILE_ID_ATTR] = {f"{portal}:{fid}" for fid in file_ids if fid}
    return rep


def _log_scalar_conflicts(
    existing: dict, incoming: dict, item_id: str, scalar_attrs: list, kind: str
) -> None:
    """warn when two portals disagree on a scalar attribute of the same id."""
    for attr in scalar_attrs:
        old = existing.get(attr, "")
        new = incoming.get(attr, "")
        if old and new and old != new:
            logger.warning(
                "Conflicting %s %s on id %s: kept %r, dropped incoming %r",
                kind,
                attr,
                item_id,
                old,
                new,
            )


def merge_graphs(inputs: dict, output_path: str = "merged_graph", write: bool = True):
    """
    Merge single-portal graphs into one portal-tagged graph.

    inputs: mapping of graph directory -> portal label, e.g.
        {"nci_gc_graph": "gc", "nci_gdc_graph": "gdc"}
    Graphs are folded in ``inputs`` order; on a scalar conflict the earlier
    graph wins (the conflict is logged). Each graph's schema is inferred from
    its own ``nodes.tsv`` / ``edges.tsv`` header, so heterogeneous schemas
    (e.g. GC's 41 columns vs GDC's 15) merge without extra configuration.
    """
    loaded = []
    for resource_path, portal in inputs.items():
        nodes, edges = load_graph(resource_path=resource_path)
        logger.info(
            "loaded %s as portal %r: %d nodes, %d edges",
            resource_path,
            portal,
            len(nodes),
            len(edges),
        )
        loaded.append((portal, nodes, edges))

    # merged schema = union of every input's columns, plus the portal facet
    node_attrs = _ordered_union([n.attributes for _, n, _ in loaded] + [[PORTAL_ATTR]])
    edge_attrs = _ordered_union([e.attributes for _, _, e in loaded] + [[PORTAL_ATTR]])
    node_scalars = [a for a in node_attrs if "string[]" not in a]
    edge_scalars = [a for a in edge_attrs if "string[]" not in a]

    merged_nodes = NodeSet(attributes=node_attrs)
    merged_edges = EdgeSet(attributes=edge_attrs)

    for portal, nodes, edges in loaded:
        for node_id, rep in nodes.nodes.items():
            rep = _tag_provenance(rep, portal)
            if node_id in merged_nodes.nodes:
                _log_scalar_conflicts(
                    merged_nodes.nodes[node_id], rep, node_id, node_scalars, "node"
                )
            # always update (never skip on collision) so provenance -- portal,
            # source, file_id -- set-unions across every graph that has this id
            merged_nodes.update_nodes(rep, node_id)

        for edge_id, rep in edges.edges.items():
            rep = _tag_provenance(rep, portal)
            if edge_id in merged_edges.edges:
                _log_scalar_conflicts(
                    merged_edges.edges[edge_id], rep, edge_id, edge_scalars, "edge"
                )
            merged_edges.update_edges(rep)

    logger.info(
        "merged graph: %d nodes, %d edges", len(merged_nodes), len(merged_edges)
    )
    if write:
        write_graph(
            node_set=merged_nodes, edge_set=merged_edges, resource_path=output_path
        )
        logger.info("wrote merged graph to %s", output_path)
    return merged_nodes, merged_edges


if __name__ == "__main__":
    merge_graphs(
        inputs={"nci_gc_graph": "gc", "nci_gdc_graph": "gdc", "nci_pdc_graph": "pdc"},
        output_path="merged_graph",
    )
