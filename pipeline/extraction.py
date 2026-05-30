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
        description="Short, self-contained name of the idea/object/framework or "
        "algorithm/technique, as it would head a glossary entry — no surrounding prose."
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
    term: str = Field(description="The exact term being defined.")
    statement: str = Field(
        description="The full formal definition as stated in the text, "
        "preserving LaTeX / math notation verbatim."
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
        description="The full statement of the result, preserving LaTeX / math notation "
        "verbatim. Exclude any proof."
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
by the text."""


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


def merge_results(parts: list[ExtractionResult]) -> ExtractionResult:
    # Chunks overlap, so the same concept/definition/result is extracted from adjacent chunks.
    # Dedup all three by the same normalized key graph_write uses for ids, so overlap doesn't
    # mint duplicate nodes. (Near-duplicate *partial* statements from a result split across a
    # chunk boundary can still slip through — acceptable for v1, flagged in spec §14.)
    seen_c, concepts = set(), []
    for p in parts:
        for c in p.concepts:
            if c.name.lower() not in seen_c:
                seen_c.add(c.name.lower())
                concepts.append(c)
    seen_d, definitions = set(), []
    for p in parts:
        for d in p.definitions:
            if normalize_statement(d.statement) not in seen_d:
                seen_d.add(normalize_statement(d.statement))
                definitions.append(d)
    seen_r, results = set(), []
    for p in parts:
        for r in p.results:
            key = (r.kind, normalize_statement(r.statement))
            if key not in seen_r:
                seen_r.add(key)
                results.append(r)
    return ExtractionResult(concepts=concepts, definitions=definitions, results=results)
