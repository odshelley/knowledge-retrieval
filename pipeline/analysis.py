"""Per-paper analysis matching the research skill's note template. Math kept as LaTeX.

The note template is defined once, as the PaperAnalysis Pydantic model with per-field guidance.
The paper_analysis asset hands it to the Anthropic SDK's ``.parse()`` helper, which derives the
structured-output JSON schema, enforces it, and validates the response back into PaperAnalysis —
so there is no separate prose schema or hand-written type check to keep in sync. The Field
descriptions ARE the per-field instructions the model sees; keep them precise."""
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

# Fixed standing brief replaces the skill's interactive per-paper learning goal (spec §15).
STANDING_BRIEF = (
    "Summarise for a STEM researcher whose work is rooted in mathematics, statistics, and "
    "AI / machine learning but who reads broadly across the sciences and engineering. "
    "Emphasise the core technical contributions and how the results connect."
)


class Definition(BaseModel):
    term: str = Field(description="The term being defined.")
    statement: str = Field(
        description="The definition, preserving LaTeX / math notation verbatim."
    )

    @field_validator("term")
    @classmethod
    def _strip_term(cls, v: str) -> str:
        return v.strip()


class Result(BaseModel):
    name: str = Field(
        default="",
        description='Label of the result if the paper gives one, e.g. "Theorem 3.2"; '
        "empty string otherwise.",
    )
    statement: str = Field(
        description="The result statement, preserving LaTeX / math notation verbatim."
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip()


class PaperAnalysis(BaseModel):
    summary: str = Field(description="2-3 paragraph prose summary of the paper.")
    key_contributions: list[str] = Field(
        description="The paper's main contributions, one per item."
    )
    methodology: str = Field(
        description="How the work is carried out — the methods/approach, as prose."
    )
    key_findings: list[str] = Field(
        description="Principal findings/results in plain language, one per item."
    )
    important_references: list[str] = Field(
        description="Key works the paper builds on or positions against, one per item."
    )
    atomic_notes: list[str] = Field(
        description="Standalone, self-contained notes — each a single idea capturable on its "
        "own (research-skill atomic-note style)."
    )
    definitions: list[Definition] = Field(
        description="Formal definitions introduced or relied on, with LaTeX preserved."
    )
    results: list[Result] = Field(
        description="Formal results (theorems, lemmas, etc.) stated in the paper, "
        "with LaTeX preserved."
    )


# Ordered note-template keys, single-sourced from the model (preserves declaration order).
ANALYSIS_FIELDS = list(PaperAnalysis.model_fields)

SYSTEM_PROMPT = (
    "Produce a structured analysis of this paper, populating every field of the response "
    "schema according to its description. Keep all mathematics as LaTeX. "
    f"Audience brief: {STANDING_BRIEF}"
)


def validate_analysis(obj: dict) -> dict:
    """Validate a raw analysis dict into the canonical note-template shape.

    Retained for callers/tests that hold a plain dict; the ``.parse()``-based asset path gets a
    PaperAnalysis straight from the SDK. Raises ``pydantic.ValidationError`` (a ``ValueError``
    subclass) on a missing field or wrong type.
    """
    return PaperAnalysis.model_validate(obj).model_dump()
