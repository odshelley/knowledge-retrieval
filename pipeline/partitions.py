from __future__ import annotations

import json
from pathlib import Path

from dagster import StaticPartitionsDefinition

PARTITIONS_FILE = Path(__file__).resolve().parent.parent / "data" / "partitions.json"


def load_partitions() -> list[dict]:
    if not PARTITIONS_FILE.exists():
        return []
    return json.loads(PARTITIONS_FILE.read_text())


def paper_ids() -> list[str]:
    return [p["paper_id"] for p in load_partitions()]


def partitions_def() -> StaticPartitionsDefinition:
    return StaticPartitionsDefinition(paper_ids())


def get_partition(paper_id: str) -> dict | None:
    for p in load_partitions():
        if p["paper_id"] == paper_id:
            return p
    return None
