"""Entity resolution: conservative thresholds, split-when-unsure, decisions recorded."""
from __future__ import annotations

import enum
from typing import Literal

from pydantic import BaseModel

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


def lookup_alias(cur, label: str, name: str) -> str | None:
    """Return the canonical name an alias maps to, or None. Consulted before NN search (spec §7)."""
    cur.execute(
        "SELECT canonical FROM alias_map WHERE label = %s AND alias = %s",
        (label, name),
    )
    row = cur.fetchone()
    return row[0] if row else None


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
                    score: float, action: str, run_id: str) -> None:
    cur.execute(
        "INSERT INTO resolution_decisions "
        "(candidate, matched_to, label, score, action, run_id) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (candidate, matched_to, label, score, action, run_id),
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
