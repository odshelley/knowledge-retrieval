"""Deterministic result-label normalization for the post-extraction linking pass.
Handles both directions of the format mismatch: models write "Lemma 9.6", nodes are
named "9.6. Lemma."; prose references name theorems ("the Monotone-Convergence
Theorem"). Only unique matches resolve — ambiguity returns None (spec §5)."""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

_KINDS = {"theorem": "theorem", "thm": "theorem", "lemma": "lemma", "lem": "lemma",
          "proposition": "proposition", "prop": "proposition",
          "corollary": "corollary", "cor": "corollary"}
_TAG = re.compile(r"\b(\d+(?:\.\d+)+|\d+)\b")
_KIND = re.compile("|".join(_KINDS), re.I)
_WORD = re.compile(r"[a-z]+")


def parse_label(s: str) -> tuple[str | None, str | None, str]:
    """(kind, numeric tag, normalized residual phrase). Any part may be missing.

    All kind words are stripped from the phrase, not just the one consumed as the kind
    marker: a node name like "9.7. Theorem. Dominated-Convergence Theorem" contains
    "theorem" twice (the marker and part of the theorem's proper name), so a single
    removal leaves a residual "theorem" that a plain reference phrase ("Dominated-
    Convergence Theorem") would not have after its own marker is consumed. Filtering
    every kind word out of the phrase keeps both sides symmetric.
    """
    low = s.lower()
    km = _KIND.search(low)
    kind = _KINDS[km.group(0)] if km else None
    tm = _TAG.search(low)
    tag = tm.group(1) if tm else None
    residue = low
    if km:
        residue = residue.replace(km.group(0), " ", 1)
    if tm:
        residue = residue.replace(tm.group(1), " ", 1)
    phrase = " ".join(w for w in _WORD.findall(residue) if w not in _KINDS)
    return kind, tag, phrase


@dataclass
class LabelIndex:
    by_kind_tag: dict[tuple[str, str], list[str]] = field(default_factory=lambda: defaultdict(list))
    by_tag: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    by_phrase: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def resolve(self, ref: str) -> str | None:
        kind, tag, phrase = parse_label(ref)
        candidates: list[str] = []
        if kind and tag:
            candidates = self.by_kind_tag.get((kind, tag), [])
        if not candidates and tag:
            candidates = self.by_tag.get(tag, [])
        if not candidates and phrase and len(phrase.split()) >= 2:
            candidates = self.by_phrase.get(phrase, [])
        return candidates[0] if len(candidates) == 1 else None


def unique_label_map(rows: list[dict]) -> dict[str, str]:
    """name -> id, EXCLUDING empty and duplicate names (mirrors
    pipeline.assets.graph_write.result_name_index's collision-safety rationale).

    Keying a plain dict on name would let the LLM fuzzy fallback resolve an ambiguous
    reference to an arbitrary one of several same-named Results (last-wins). Dropping
    ambiguous names here means those references never enter the fuzzy candidate space —
    they stay unresolved and are logged+dropped, never guessed.
    """
    counts: dict[str, int] = {}
    for r in rows:
        if r["name"]:
            counts[r["name"]] = counts.get(r["name"], 0) + 1
    return {r["name"]: r["id"] for r in rows if r["name"] and counts[r["name"]] == 1}


def build_label_index(rows: list[dict]) -> LabelIndex:
    idx = LabelIndex()
    for row in rows:
        if not row.get("name"):
            continue
        kind, tag, phrase = parse_label(row["name"])
        kind = kind or row.get("kind")
        if tag:
            idx.by_tag[tag].append(row["id"])
            if kind:
                idx.by_kind_tag[(kind, tag)].append(row["id"])
        if phrase and len(phrase.split()) >= 2:
            idx.by_phrase[phrase].append(row["id"])
    return idx
