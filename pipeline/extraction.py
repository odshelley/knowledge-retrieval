"""LLM extraction against the alethograph schema. Prompts ported from the research skill
+ spec/03-extraction-prompts.md scaffold; alethograph label vocabulary + few-shots."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from pipeline.schema import PATTERNS

VALID_RESULT_KINDS = {"theorem", "lemma", "proposition", "corollary"}
VALID_CONCEPT_KINDS = {"concept", "method"}

SYSTEM_PROMPT = """You are an information-extraction assistant for academic papers in \
quantitative finance / stochastics. From the chunk, extract:
- concepts: 3-7 major theoretical ideas/objects/frameworks (kind="concept") or implementable \
algorithms/techniques (kind="method"). Each must be self-contained.
- definitions: formal definitions, with the term and the statement (preserve LaTeX).
- results: theorems/lemmas/propositions/corollaries, with name (e.g. "Theorem 3.2"), kind, \
and statement (preserve LaTeX).
Return STRICT JSON: {"concepts":[{"name","kind"}],"definitions":[{"term","statement"}],\
"results":[{"name","kind","statement"}]}. Emit nothing not asserted by the text."""


@dataclass
class Concept:
    name: str
    kind: str  # concept | method


@dataclass
class Definition:
    term: str
    statement: str


@dataclass
class Result:
    name: str
    kind: str  # theorem | lemma | proposition | corollary
    statement: str


@dataclass
class ExtractionResult:
    concepts: list[Concept] = field(default_factory=list)
    definitions: list[Definition] = field(default_factory=list)
    results: list[Result] = field(default_factory=list)


def validate_triples(triples: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    allowed = set(PATTERNS)
    return [t for t in triples if t in allowed]


def parse_extraction(payload: dict) -> ExtractionResult:
    concepts = []
    for c in payload.get("concepts", []):
        kind = c.get("kind", "concept")
        if kind not in VALID_CONCEPT_KINDS:
            raise ValueError(f"bad concept kind: {kind}")
        concepts.append(Concept(name=c["name"].strip(), kind=kind))
    definitions = [Definition(term=d["term"].strip(), statement=d["statement"])
                   for d in payload.get("definitions", [])]
    results = []
    for r in payload.get("results", []):
        if r.get("kind") not in VALID_RESULT_KINDS:
            raise ValueError(f"bad result kind: {r.get('kind')}")
        results.append(Result(name=r.get("name", ""), kind=r["kind"], statement=r["statement"]))
    return ExtractionResult(concepts=concepts, definitions=definitions, results=results)


def extract_from_chunk(client, model: str, chunk: str) -> ExtractionResult:
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": SYSTEM_PROMPT},
                  {"role": "user", "content": chunk[:12000]}],
        response_format={"type": "json_object"},
    )
    return parse_extraction(json.loads(resp.choices[0].message.content))


def _norm(s: str) -> str:
    """Match graph_write.normalize_statement so dedup keys here line up with node ids there."""
    return re.sub(r"\s+", " ", s.strip().lower())


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
            if _norm(d.statement) not in seen_d:
                seen_d.add(_norm(d.statement))
                definitions.append(d)
    seen_r, results = set(), []
    for p in parts:
        for r in p.results:
            key = (r.kind, _norm(r.statement))
            if key not in seen_r:
                seen_r.add(key)
                results.append(r)
    return ExtractionResult(concepts=concepts, definitions=definitions, results=results)
