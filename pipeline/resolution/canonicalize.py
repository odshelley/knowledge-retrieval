"""Deterministic concept-name canonicalization (match key only; node names keep their surface form).

canonical_key collapses Tier-A duplicates (case / unicode dash / curly quote / parenthetical acronym)
while NEVER producing a false merge: no plural/suffix stripping, no symbol removal, and a min-length
guard. Misses (variants it does not collapse) are intentional and fall through to the cosine/LLM tiers.
"""
from __future__ import annotations

import re
import unicodedata

_DASHES = {"–", "—", "−", "‐"}  # en, em, minus, hyphen
_QUOTE_MAP = {"‘": "'", "’": "'", "“": '"', "”": '"'}
_TRAILING_PAREN = re.compile(r"\s*\(([^()]*)\)\s*$")
_MIN_KEY_LEN = 3


def _unify_dashes_quotes(s: str) -> str:
    out = []
    for ch in s:
        if ch in _DASHES:
            out.append("-")
        else:
            out.append(_QUOTE_MAP.get(ch, ch))
    return "".join(out)


def _strip_acronym(s: str) -> str:
    """Drop a single trailing (ACR) iff ACR (letters-only, upper) == initials of preceding tokens.

    Tokens split on whitespace AND hyphens; every token contributes its first letter; no stop-word
    dropping. Returns s unchanged if the rule does not fire.
    """
    m = _TRAILING_PAREN.search(s)
    if not m:
        return s
    head = s[: m.start()]
    acr = "".join(ch for ch in m.group(1) if ch.isalpha()).upper()
    if not acr:
        return s
    tokens = [t for t in re.split(r"[\s-]+", head) if t]
    initials = "".join(t[0] for t in tokens if t[:1].isalpha()).upper()
    return head if acr == initials else s


def canonical_key(name: str) -> str:
    name = name.replace("\x00", "")  # NUL from tainted PDF text must never reach Postgres params
    casefolded_original = name.strip().casefold()
    s = unicodedata.normalize("NFKC", name)
    s = _unify_dashes_quotes(s)
    s = s.casefold()
    s = _strip_acronym(s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) < _MIN_KEY_LEN:
        return casefolded_original
    return s
