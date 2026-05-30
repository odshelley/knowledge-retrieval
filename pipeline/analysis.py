"""Per-paper analysis matching the research skill's note template. Math kept as LaTeX."""
from __future__ import annotations

# Fixed standing brief replaces the skill's interactive per-paper learning goal (spec §15).
STANDING_BRIEF = (
    "Summarise for a STEM researcher whose work is rooted in mathematics, statistics, and "
    "AI / machine learning but who reads broadly across the sciences and engineering. "
    "Emphasise the core technical contributions and how the results connect."
)

ANALYSIS_FIELDS = [
    "summary", "key_contributions", "methodology", "key_findings",
    "important_references", "atomic_notes", "definitions", "results",
]

SYSTEM_PROMPT = (
    "Produce a structured analysis of this paper as STRICT JSON with keys: "
    + ", ".join(ANALYSIS_FIELDS) + ". "
    "summary: 2-3 paragraphs. key_contributions/key_findings/important_references/atomic_notes: "
    "arrays of strings. definitions/results: arrays of objects with statements in LaTeX. "
    f"Audience brief: {STANDING_BRIEF}"
)


def strip_to_json(text: str) -> str:
    """Claude may wrap JSON in prose/fences; return the substring from the first '{' to the
    last '}' (inclusive). If no braces are present, return the input stripped."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return text.strip()
    return text[start:end + 1]


def validate_analysis(obj: dict) -> dict:
    if not isinstance(obj, dict):
        raise ValueError("analysis must be a JSON object")
    missing = [f for f in ANALYSIS_FIELDS if f not in obj]
    if missing:
        raise ValueError(f"analysis missing fields: {missing}")
    expected = {
        "summary": str,
        "key_contributions": list,
        "methodology": str,
        "key_findings": list,
        "important_references": list,
        "atomic_notes": list,
        "definitions": list,
        "results": list,
    }
    wrong = [k for k, t in expected.items() if not isinstance(obj.get(k), t)]
    if wrong:
        raise ValueError(f"analysis fields with wrong types: {wrong}")
    return obj
