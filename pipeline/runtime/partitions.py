"""Dynamic, content-hash-keyed partitions — one per ingested document."""
from __future__ import annotations

import hashlib

from dagster import DynamicPartitionsDefinition

DOCUMENTS_PARTITION = "documents"


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def documents_partitions_def() -> DynamicPartitionsDefinition:
    return DynamicPartitionsDefinition(name=DOCUMENTS_PARTITION)
