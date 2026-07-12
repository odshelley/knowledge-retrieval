"""Chapter role classification: heuristics first, one batched LLM tie-break for the
ambiguous residue (spec 2026-07-12 §3). Roles gate which chapters get extraction
partitions; misclassifying as content only wastes a few LLM calls, so every fallback
lands on content."""
from __future__ import annotations

import json
import re

EXTRACT_ROLES = frozenset({"content", "notation_guide", "exercises"})
ALL_ROLES = EXTRACT_ROLES | {"front_matter", "back_matter"}

_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(notation|symbols|nomenclature)\b", re.I), "notation_guide"),
    (re.compile(r"\bexercises?\b", re.I), "exercises"),
    (re.compile(r"^(title|copyright|colophon|contents|table of contents|preface|foreword|"
                r"acknowledg\w*|dedication|about the author|a question of terminology)\b",
                re.I), "front_matter"),
    (re.compile(r"^(index|references|bibliography|glossary|list of (figures|tables))\b",
                re.I), "back_matter"),
]


def _heuristic(title: str, page_start: int, page_end: int) -> str | None:
    t = title.strip()
    for pat, role in _RULES:
        if pat.search(t):
            return role
    if page_end - page_start >= 4:
        return "content"  # multi-page, no front/back-matter signal: real content
    return None  # short + unrecognized: ambiguous


def classify_roles(chapters: list[dict]) -> dict[int, str | None]:
    """number -> role, or None where the heuristics can't decide."""
    return {ch["number"]: _heuristic(ch["title"], ch["page_start"], ch["page_end"])
            for ch in chapters}


_TIEBREAK_PROMPT = (
    "You are classifying book chapters by role. Given the full chapter list of a book and "
    "a subset that needs classification, answer with a JSON object mapping chapter number "
    "(as a string) to one of: content, notation_guide, exercises, front_matter, back_matter. "
    "content = real subject matter worth extracting. Answer with the JSON object only.\n\n"
    "Full chapter list:\n{listing}\n\nClassify these chapter numbers: {pending}"
)


def resolve_ambiguous(client, model: str, chapters: list[dict],
                      pending: list[int], timeout: float = 60.0) -> dict[int, str]:
    """One batched call for the ambiguous residue. Any failure -> content (safe default)."""
    listing = "\n".join(f'{c["number"]}: "{c["title"]}" '
                        f'(pages {c["page_start"]}-{c["page_end"]})' for c in chapters)
    try:
        resp = client.messages.create(
            model=model, max_tokens=1024, timeout=timeout,
            messages=[{"role": "user", "content": _TIEBREAK_PROMPT.format(
                listing=listing, pending=pending)}])
        text = next(b.text for b in resp.content if b.type == "text")
        raw = json.loads(text[text.index("{"):text.rindex("}") + 1])
        out = {}
        for n in pending:
            role = raw.get(str(n), "content")
            out[n] = role if role in ALL_ROLES else "content"
        return out
    except Exception:  # noqa: BLE001 — classification must never sink an ingestion
        return {n: "content" for n in pending}
