"""Entity resolution: conservative thresholds, split-when-unsure, decisions recorded."""
from __future__ import annotations

import enum


class Decision(enum.Enum):
    MERGE = "merge"
    CREATE = "create"
    CREATE_FLAGGED = "create_flagged"  # ambiguous band → create new but flag for review


def decide(score: float, high: float = 0.90, low: float = 0.60) -> Decision:
    if score >= high:
        return Decision.MERGE
    if score < low:
        return Decision.CREATE
    return Decision.CREATE_FLAGGED


def nearest(cur, label: str, embedding: list[float]) -> tuple[str, float] | None:
    """Return (canonical_name, cosine_similarity) of the closest same-label entity, or None."""
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
    cur.execute(
        "INSERT INTO entity_embeddings (canonical, label, embedding) VALUES (%s,%s,%s::vector) "
        "ON CONFLICT (canonical, label) DO UPDATE SET embedding = EXCLUDED.embedding",
        (canonical, label, embedding),
    )
