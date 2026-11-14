"""Read typed cross-references between vocabularies from OBO files."""

import re
from typing import NamedTuple

from bioregistry import normalize_curie

_XREF = re.compile(r"^xref: (\S+)(?: \{(.*)\})?")
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
    with Bioregistry, and ones it cannot normalize are skipped.
    """
    mappings = []
    term = None
    with open(path) as fh:
        for line in fh:
            if line.startswith("["):
                term = None
            elif line.startswith("id: "):
                term = normalize_curie(line[4:].strip())
            elif term and (match := _XREF.match(line)):
                xref, qualifiers = match.groups()
                obj = normalize_curie(xref)
                predicates = {
                    qualifier_predicates[q]
                    for q in _SOURCE.findall(qualifiers or "")
                    if q in qualifier_predicates
                }
                ## Conflicting qualifiers leave the mapping untyped
                if obj and len(predicates) == 1:
                    mappings.append(Mapping(term, predicates.pop(), obj))
    return mappings
