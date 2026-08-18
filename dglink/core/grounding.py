"""Ground free-text biomedical terms (e.g. a primary diagnosis) to ontology identifiers.

Shared by every portal's diagnosis parser so grounded disease concepts merge across
portals. Uses `gilda.annotate` (not `ground`) so messy multi-part strings still resolve —
`ground` needs the whole string to match, so e.g. "Papillary transitional cell carcinoma,
non-invasive" fails, whereas `annotate` finds the grounded span within it. Among the
grounded spans we keep the one covering the *longest* substring (the most specific
concept, not a generic sub-token like "carcinoma") above a score threshold; otherwise the
caller falls back to a portal-local id.
"""

import re

import gilda
from bioregistry import normalize_curie, get_bioregistry_iri

GROUNDING_SCORE_THRESHOLD = 0.5


def slugify(name: str) -> str:
    """Lower-case, punctuation-collapsed slug of a name, for keying ungrounded nodes by
    name so duplicates that share a name (e.g. "Not Reported") collapse to one id."""
    return (
        re.sub(r"[^a-z0-9]+", "_", (name or "").strip().lower()).strip("_") or "unknown"
    )


def ground_term(text: str, score_threshold: float = GROUNDING_SCORE_THRESHOLD) -> tuple:
    """Return a (normalized_name, curie, iri) triple for `text`.

    First tries to ground the whole string with `gilda.ground` (the most specific match);
    if that fails, falls back to `gilda.annotate` and keeps the match on the longest span
    within the string. Either way only matches at/above `score_threshold` are accepted.
    curie/iri are None when `text` is empty or nothing grounds, so callers can detect
    failure and fall back to a portal-local id.
    """
    if not isinstance(text, str) or not text.strip():
        return text, None, None

    def _triple(match):
        term = match.term
        return (
            term.entry_name,
            normalize_curie(f"{term.db}:{term.id}"),
            get_bioregistry_iri(term.db, term.id),
        )

    ## 1) prefer a grounding of the entire string
    full = gilda.ground(text)
    if full and full[0].score >= score_threshold:
        return _triple(full[0])

    ## 2) fall back to the longest grounded span within the string
    best_match, best_span = None, -1
    for annotation in gilda.annotate(text):
        if not annotation.matches:
            continue
        top = annotation.matches[0]  # gilda returns matches best-score-first
        span = len(annotation.text)
        if top.score >= score_threshold and span > best_span:
            best_span, best_match = span, top
    if best_match is None:
        return text, None, None
    return _triple(best_match)
