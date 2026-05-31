"""Entity resolution: conservative thresholds, split-when-unsure, decisions recorded."""
from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel

from pipeline.canonicalize import canonical_key

EMBEDDING_DIM = 1536


class Decision(enum.Enum):
    MERGE = "merge"            # cosine ≥ high → confident same entity, no LLM call
    CREATE = "create"          # cosine < low → confident distinct entity, no LLM call
    ADJUDICATE = "adjudicate"  # ambiguous band → ask the LLM whether the names match


def decide(score: float, high: float = 0.90, low: float = 0.60) -> Decision:
    """Cheap cosine pre-filter. Only the ambiguous band escalates to an LLM (see `adjudicate`)."""
    if score >= high:
        return Decision.MERGE
    if score < low:
        return Decision.CREATE
    return Decision.ADJUDICATE


class Verdict(BaseModel):
    """LLM 3-way verdict on whether two concept names denote the same concept."""
    decision: Literal["SAME", "DIFFERENT", "UNSURE"]
    reason: str


_ADJUDICATE_SYSTEM = (
    "You judge whether two technical concept names, extracted from research papers, refer to the "
    "SAME underlying concept. Answer with exactly one decision:\n"
    "- SAME: they denote the same concept (acronym/expansion, pluralisation, or minor notational/"
    "spelling variant of one idea, e.g. 'Bridge Matching (BM)' vs 'Bridge Matching').\n"
    "- DIFFERENT: they are genuinely different ideas, even if closely related (e.g. 'Bridge Matching' "
    "vs 'Flow Matching').\n"
    "- UNSURE: you cannot tell from the names alone whether they are the same.\n"
    "Prefer UNSURE over guessing; do NOT collapse UNSURE into DIFFERENT. Always give a brief reason."
)


def adjudicate(client, model: str, candidate: str, canonical: str,
               timeout: float | None = None) -> Verdict:
    """LLM 3-way: do `candidate` and `canonical` name the same concept? Called only for the
    ambiguous cosine band on the single top-1 neighbour. The caller guards exceptions/None."""
    resp = client.chat.completions.parse(
        model=model,
        timeout=timeout,
        messages=[
            {"role": "system", "content": _ADJUDICATE_SYSTEM},
            {"role": "user",
             "content": f"Concept A: {candidate!r}\nConcept B: {canonical!r}\n\n"
                        "Do A and B refer to the same concept? Answer SAME, DIFFERENT, or UNSURE."},
        ],
        response_format=Verdict,
    )
    return resp.choices[0].message.parsed


def lookup_by_key(cur, label: str, key: str) -> tuple[str, str] | None:
    """Return (canonical, source) the canonical_key maps to in alias_map, or None.
    `alias` column stores canonical keys only (spec rev 2 §6)."""
    cur.execute(
        "SELECT canonical, source FROM alias_map WHERE label = %s AND alias = %s",
        (label, key),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def similarity_to(cur, label: str, canonical: str, embedding: list[float]) -> float | None:
    """Cosine similarity of `embedding` to a specific canonical's stored embedding, or None if absent.
    Used by the alias cosine-guard (spec §3 step 1)."""
    if len(embedding) != EMBEDDING_DIM:
        raise ValueError(f"embedding has {len(embedding)} dims, expected {EMBEDDING_DIM}")
    cur.execute(
        "SELECT 1 - (embedding <=> %s::vector) FROM entity_embeddings "
        "WHERE label = %s AND canonical = %s",
        (embedding, label, canonical),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_alias(cur, label: str, key: str, canonical: str, source: str) -> None:
    """Register canonical_key -> canonical (first-seen wins). Sole writer is graph_write (spec §7)."""
    cur.execute(
        "INSERT INTO alias_map (alias, label, canonical, source) VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (alias, label) DO NOTHING",
        (key, label, canonical, source),
    )


def nearest(cur, label: str, embedding: list[float]) -> tuple[str, float] | None:
    """Return (canonical_name, cosine_similarity) of the closest same-label entity, or None."""
    if len(embedding) != EMBEDDING_DIM:
        raise ValueError(
            f"embedding has {len(embedding)} dims, expected {EMBEDDING_DIM} "
            f"(does the embedding_model match the pgvector column?)"
        )
    cur.execute(
        "SELECT canonical, 1 - (embedding <=> %s::vector) AS sim "
        "FROM entity_embeddings WHERE label = %s ORDER BY embedding <=> %s::vector LIMIT 1",
        (embedding, label, embedding),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def record_decision(cur, candidate: str, matched_to: str | None, label: str,
                    score: float, action: str, run_id: str, note: str | None = None) -> None:
    cur.execute(
        "INSERT INTO resolution_decisions "
        "(candidate, matched_to, label, score, action, run_id, note) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (candidate, matched_to, label, score, action, run_id, note),
    )


def upsert_embedding(cur, canonical: str, label: str, embedding: list[float]) -> None:
    if len(embedding) != EMBEDDING_DIM:
        raise ValueError(
            f"embedding has {len(embedding)} dims, expected {EMBEDDING_DIM} "
            f"(does the embedding_model match the pgvector column?)"
        )
    cur.execute(
        "INSERT INTO entity_embeddings (canonical, label, embedding) VALUES (%s,%s,%s::vector) "
        "ON CONFLICT (canonical, label) DO UPDATE SET embedding = EXCLUDED.embedding",
        (canonical, label, embedding),
    )


@dataclass
class ConceptResolution:
    surface: str
    canonical: str
    kind: str
    action: str
    score: float
    matched_to: str | None
    note: str | None
    embedding: list[float]


@dataclass
class AliasRegistration:
    key: str
    canonical: str
    source: str  # "rule" | "cosine" | "llm"


_VALID_DECISIONS = {"SAME", "DIFFERENT", "UNSURE"}


def _resolve_one(name, key, vec, *, lookup_by_key, nearest, similarity_to, adjudicate, high, low):
    """Resolve a single representative. Returns (action, canonical, score, matched_to, note, alias|None)."""
    note = None
    hit = lookup_by_key("Concept", key)
    if hit is not None:
        canonical, source = hit
        if source == "human":
            return "merge_alias", canonical, 1.0, canonical, None, None
        sim = similarity_to("Concept", canonical, vec)
        if sim is not None and sim >= low:
            return "merge_alias", canonical, sim, canonical, None, None
        note = f"alias key collision: key={key!r} -> {canonical!r} sim={sim}"

    nn = nearest("Concept", vec)
    if nn is None:
        return "create", name, 0.0, None, note, AliasRegistration(key, name, "rule")
    matched, score = nn
    if score >= high:
        reg = AliasRegistration(key, matched, "cosine") if canonical_key(matched) != key else None
        return "merge", matched, score, matched, note, reg
    if score < low:
        return "create", name, score, None, note, AliasRegistration(key, name, "rule")

    # ambiguous band -> guarded LLM 3-way
    try:
        v = adjudicate(name, matched)
        decision = v.decision if (v is not None and v.decision in _VALID_DECISIONS) else "UNSURE"
        reason = v.reason if v is not None else "no verdict"
    except Exception as e:  # noqa: BLE001 - any failure becomes UNSURE per spec
        decision, reason = "UNSURE", f"adjudicate error: {e}"
    note = "; ".join(p for p in (note, reason) if p)
    if decision == "SAME":
        return "merge_llm", matched, score, matched, note, AliasRegistration(key, matched, "llm")
    if decision == "DIFFERENT":
        return "create_llm", name, score, None, note, AliasRegistration(key, name, "llm")
    return "create_flagged", name, score, None, note, None  # UNSURE: never cache


def resolve_concepts(concepts, embeddings, *, lookup_by_key, nearest, similarity_to, adjudicate,
                     high: float = 0.90, low: float = 0.60):
    """Pure per-partition resolution ladder. `concepts` is a list of {name,kind}; `embeddings`
    aligns 1:1. Returns (list[ConceptResolution] one-per-surface, list[AliasRegistration])."""
    assert len(embeddings) == len(concepts), "embeddings must align 1:1 with concepts"
    groups: dict[str, list[int]] = {}
    order: list[str] = []
    for i, c in enumerate(concepts):
        k = canonical_key(c["name"])
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(i)

    res: list = [None] * len(concepts)
    aliases: list = []
    for k in order:
        idxs = groups[k]
        rep = idxs[0]
        action, canonical, score, matched_to, note, reg = _resolve_one(
            concepts[rep]["name"], k, embeddings[rep],
            lookup_by_key=lookup_by_key, nearest=nearest, similarity_to=similarity_to,
            adjudicate=adjudicate, high=high, low=low)
        res[rep] = ConceptResolution(concepts[rep]["name"], canonical, concepts[rep]["kind"],
                                     action, score, matched_to, note, embeddings[rep])
        if reg is not None:
            aliases.append(reg)
        for j in idxs[1:]:
            res[j] = ConceptResolution(concepts[j]["name"], canonical, concepts[j]["kind"],
                                       "merge_local", 1.0, canonical, None, embeddings[j])
    return res, aliases
