"""LLM extraction against the alethograph schema. Prompts ported from the research skill
+ spec/03-extraction-prompts.md scaffold; alethograph label vocabulary + few-shots.

The extraction targets are defined once, as Pydantic models with per-field guidance. Both the
OpenAI and Claude paths hand these models to the SDK's ``.parse()`` helper, which derives the
structured-output JSON schema, enforces it on the provider, and validates the response back into
these objects — so there is no separate hand-written schema to keep in sync. The Field
descriptions ARE the per-field instructions the model sees; keep them precise."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

from pipeline.text_norm import normalize_statement


class Concept(BaseModel):
    name: str = Field(
        description="The name of a *named* idea, object, framework, or algorithm/technique, as it "
        "would head a glossary entry — no surrounding prose. It must be a real concept name, never "
        "bare mathematical notation: a symbol like 'W_t', 'Π*', or 'ũ(x,t)' is NOT a concept, it is "
        "notation that denotes one. If the named concept is present in the text, use its name (e.g. "
        "'Brownian motion', not 'W_t'); if a symbol has no named concept behind it, emit no concept for it."
    )
    kind: Literal["concept", "method"] = Field(
        default="concept",
        description='"concept" for a theoretical idea, object, or framework; '
        '"method" for an implementable algorithm, technique, or procedure.',
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip()


class Definition(BaseModel):
    term: str = Field(
        description="The exact term being defined. If it contains mathematical notation, render it "
        "as LaTeX in $...$."
    )
    name: str = Field(
        default="",
        description='Label of the definition as printed, e.g. "Definition 2.14". '
        "Empty string if the text gives no label.",
    )
    statement: str = Field(
        description="The full formal definition as stated in the text. Render ALL mathematical "
        "notation as LaTeX: inline math in $...$, display equations in $$...$$. Convert any Unicode "
        "or plaintext math to LaTeX (e.g. σ -> \\sigma, ∇ -> \\nabla, sub/superscripts and fractions); "  # noqa: RUF001
        "never leave raw Unicode math."
    )
    defines: list[str] = Field(
        default_factory=list,
        description="Concept name(s), from the concepts you extract in this same response, "
        "that this definition introduces. Usually exactly one. Leave empty if unsure.",
    )

    @field_validator("term")
    @classmethod
    def _strip_term(cls, v: str) -> str:
        return v.strip()


class Result(BaseModel):
    name: str = Field(
        default="",
        description='Label of the result as it appears, e.g. "Theorem 3.2" or "Lemma 1". '
        "Empty string if the text gives no label.",
    )
    kind: Literal["theorem", "lemma", "proposition", "corollary"] = Field(
        description="The type of formal result."
    )
    statement: str = Field(
        description="The full statement of the result, excluding any proof. Render ALL mathematical "
        "notation as LaTeX: inline math in $...$, display equations in $$...$$. Convert any Unicode or "
        "plaintext math to LaTeX; never leave raw Unicode math."
    )
    uses: list[str] = Field(
        default_factory=list,
        description="Names of the concepts (from the concepts you extract in this same "
        "response) that this result invokes or relies on. Use the exact concept name "
        "strings. Leave empty if none or unsure.",
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description='Labels of OTHER results this result depends on, e.g. ["Lemma 2.4"]. '
        "Use the exact result labels as they appear. Leave empty if none or unsure.",
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip()


class ExtractionResult(BaseModel):
    concepts: list[Concept] = Field(
        default_factory=list,
        description="3-7 major theoretical ideas/objects/frameworks (kind=concept) or "
        "implementable algorithms/techniques (kind=method) present in the chunk. "
        "Each must be self-contained.",
    )
    definitions: list[Definition] = Field(
        default_factory=list,
        description="Formal definitions stated in the chunk.",
    )
    results: list[Result] = Field(
        default_factory=list,
        description="Theorems, lemmas, propositions, and corollaries stated in the chunk.",
    )


SYSTEM_PROMPT = """You are an information-extraction assistant for STEM research papers \
(most often rooted in mathematics, statistics, or AI / machine learning, but spanning the \
sciences and engineering broadly). From the chunk, populate the concepts, definitions, and \
results of the response schema, following each field's description. Emit nothing not asserted \
by the text. When filling a definition's `defines`, a result's `uses`, or a result's \
`depends_on`, reference ONLY names you have already produced in this same response; if \
unsure, leave the list empty.

Two rules govern every field:
1. CONCEPTS are named ideas/objects/frameworks/algorithms (glossary headwords). Bare mathematical \
notation is never a concept: from "Let W_t be a standard Brownian motion", the concept is \
"Brownian motion", NOT "W_t". If a symbol has no named concept behind it, emit no concept for it.
2. Render ALL mathematical notation as LaTeX — inline in $...$, display in $$...$$ — actively \
converting Unicode or plaintext math. For example, source text "ũ(x,t) = (σ²/2) ∇ ln ρ̃(x,t)" must \
be written as $\\tilde u(x,t) = \\tfrac{\\sigma^2}{2}\\,\\nabla \\ln \\tilde\\rho(x,t)$. Never leave \
raw Unicode math in any field."""  # noqa: RUF001


def parse_extraction(payload: dict) -> ExtractionResult:
    """Validate a raw JSON dict into an ExtractionResult.

    Retained for callers/tests that already hold a plain dict; the ``.parse()``-based extract
    paths get an ExtractionResult straight from the SDK and don't go through here. Raises
    ``pydantic.ValidationError`` (a ``ValueError`` subclass) on an unknown kind or missing field.
    """
    return ExtractionResult.model_validate(payload)


def extract_from_chunk(client, model: str, chunk: str, timeout: float = 60.0) -> ExtractionResult:
    resp = client.chat.completions.parse(
        model=model,
        messages=[{"role": "system", "content": SYSTEM_PROMPT},
                  {"role": "user", "content": chunk[:12000]}],
        response_format=ExtractionResult,
        timeout=timeout,
    )
    return resp.choices[0].message.parsed


def _extend_unique(dst: list[str], src: list[str]) -> None:
    """Append items from src not already in dst, preserving order. Mutates dst in place
    (and thus the kept model it belongs to); inputs are not read again after merge."""
    for item in src:
        if item not in dst:
            dst.append(item)


# Structural math markup / operators that mark a string as notation. Includes ASCII operators
# (/ + = < >) so 'x=y', 'a+b', 'p/q' are caught. Deliberately excludes hyphen and whitespace —
# they appear in real names ('σ-algebra', 'k-NN', 'state-of-the-art').
_MATH_SIGNAL_CHARS = set("_^*\\(){}|/+=<>")


def _has_three_letter_run(s: str) -> bool:
    """True if s has a run of >=3 consecutive Unicode-alphabetic letters (a word-like token)."""
    run = 0
    for ch in s:
        if ch.isalpha():
            run += 1
            if run >= 3:
                return True
        else:
            run = 0
    return False


def _has_math_signal(s: str) -> bool:
    """True if s contains math markup: structural chars, a digit, or a non-letter symbol.
    Greek/accented LETTERS (σ, Π, ũ) are letters, not signals; hyphen and whitespace are not signals."""
    for ch in s:
        if ch in _MATH_SIGNAL_CHARS or ch.isdigit():
            return True
        if not ch.isascii() and not ch.isalpha() and not ch.isspace():
            return True
    return False


def _is_notation_only(name: str) -> bool:
    """Conservative backstop: a concept name is notation-only (and should not be a Concept) iff it
    carries a math signal AND has no >=3-letter word. Errs toward keeping (real concept > stray symbol)."""
    s = name.replace("$", "")
    return _has_math_signal(s) and not _has_three_letter_run(s)


def merge_results(parts: list[ExtractionResult]) -> ExtractionResult:
    # Chunks overlap, so the same concept/definition/result is extracted from adjacent chunks.
    # Dedup all three by the same normalized key graph_write uses for ids, so overlap doesn't
    # mint duplicate nodes. (Near-duplicate *partial* statements from a result split across a
    # chunk boundary can still slip through — acceptable for v1, flagged in spec §14.)
    seen_c, concepts = set(), []
    for p in parts:
        for c in p.concepts:
            if _is_notation_only(c.name):
                continue  # bare notation is never a concept (backstop; primary fix is the prompt)
            if c.name.lower() not in seen_c:
                seen_c.add(c.name.lower())
                concepts.append(c)
    seen_d: dict[str, Definition] = {}
    definitions = []
    for p in parts:
        for d in p.definitions:
            k = normalize_statement(d.statement)
            kept = seen_d.get(k)
            if kept is None:
                seen_d[k] = d
                definitions.append(d)
            else:
                _extend_unique(kept.defines, d.defines)
    seen_r: dict[tuple[str, str], Result] = {}
    results = []
    for p in parts:
        for r in p.results:
            k = (r.kind, normalize_statement(r.statement))
            kept = seen_r.get(k)
            if kept is None:
                seen_r[k] = r
                results.append(r)
            else:
                _extend_unique(kept.uses, r.uses)
                _extend_unique(kept.depends_on, r.depends_on)
    return ExtractionResult(concepts=concepts, definitions=definitions, results=results)


def merge_results_with_provenance(
    parts: list[ExtractionResult], chunk_ids: list[str],
) -> tuple[ExtractionResult, dict]:
    """merge_results plus per-item source-chunk ids. `chunk_ids` aligns 1:1 with `parts`.
    Provenance keys mirror the dedup keys merge_results/graph_write use: lowercased concept
    name; normalize_statement(statement) for definitions; '<kind>|<normalized>' for results."""
    assert len(parts) == len(chunk_ids), "chunk_ids must align 1:1 with parts"
    merged = merge_results(parts)
    kept_c = {c.name.lower() for c in merged.concepts}
    kept_d = {normalize_statement(d.statement) for d in merged.definitions}
    kept_r = {f"{r.kind}|{normalize_statement(r.statement)}" for r in merged.results}
    prov: dict = {"concepts": {}, "definitions": {}, "results": {}}

    def _add(bucket: dict, key: str, cid: str) -> None:
        lst = bucket.setdefault(key, [])
        if cid not in lst:
            lst.append(cid)

    for part, cid in zip(parts, chunk_ids):
        for c in part.concepts:
            if c.name.lower() in kept_c:
                _add(prov["concepts"], c.name.lower(), cid)
        for d in part.definitions:
            k = normalize_statement(d.statement)
            if k in kept_d:
                _add(prov["definitions"], k, cid)
        for r in part.results:
            k = f"{r.kind}|{normalize_statement(r.statement)}"
            if k in kept_r:
                _add(prov["results"], k, cid)
    return merged, prov
