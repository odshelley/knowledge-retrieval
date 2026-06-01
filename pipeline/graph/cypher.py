"""Reusable Cypher fragments."""
from __future__ import annotations


def batched_detach_delete(batch_size: int = 10000) -> str:
    """Delete every node in transaction batches so large graphs don't OOM."""
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return (
        "MATCH (n) "
        f"CALL {{ WITH n DETACH DELETE n }} IN TRANSACTIONS OF {batch_size} ROWS"
    )
