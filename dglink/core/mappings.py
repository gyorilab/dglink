"""Read typed cross-references between vocabularies from OBO files."""

import re
from typing import NamedTuple

import obonet
from bioregistry import normalize_curie

_SOURCE = re.compile(r'source="([^"]+)"')


class Mapping(NamedTuple):
    """A mapping from a term to a term in another vocabulary."""

    subject: str
    predicate: str
    object: str


def read_obo_mappings(path: str, qualifier_predicates: dict) -> list:
    """Return the cross-references in an OBO file whose qualifiers have a predicate.

    An OBO file can qualify each cross-reference with `source` annotations, for
    example `xref: SCTID:64572001 {source="MONDO:equivalentTo"}`.
    `qualifier_predicates` maps such annotations to a predicate, and
    cross-references with no annotation in it are skipped. CURIEs are normalized
    with Bioregistry, and ones it cannot normalize are skipped, as are obsolete
    terms.
    """
    graph = obonet.read_obo(path, include_clauses=True)
    mappings = []
    for node, data in graph.nodes(data=True):
        term = normalize_curie(node)
        ## Terms only referenced by others have no clauses
        for clause in data.get("_clauses", {}).get("xref", []):
            obj = normalize_curie(clause["value"])
            predicates = {
                qualifier_predicates[q]
                for q in _SOURCE.findall(clause["trailing_modifier"] or "")
                if q in qualifier_predicates
            }
            ## Conflicting qualifiers leave the mapping untyped
            if term and obj and len(predicates) == 1:
                mappings.append(Mapping(term, predicates.pop(), obj))
    return mappings
