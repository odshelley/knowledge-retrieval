"""Dynamic, content-hash-keyed partitions — one per ingested document."""
from __future__ import annotations

import hashlib

from dagster import DynamicPartitionsDefinition

DOCUMENTS_PARTITION = "documents"


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def documents_partitions_def() -> DynamicPartitionsDefinition:
    return DynamicPartitionsDefinition(name=DOCUMENTS_PARTITION)


BOOKS_PARTITION = "books"
BOOK_CHAPTERS_PARTITION = "book_chapters"


def books_partitions_def() -> DynamicPartitionsDefinition:
    return DynamicPartitionsDefinition(name=BOOKS_PARTITION)


def book_chapters_partitions_def() -> DynamicPartitionsDefinition:
    return DynamicPartitionsDefinition(name=BOOK_CHAPTERS_PARTITION)


def chapter_partition_key(book_sha: str, n: int) -> str:
    return f"{book_sha}:ch{n:02d}"


def split_chapter_key(key: str) -> tuple[str, int]:
    sha, sep, ch = key.rpartition(":ch")
    if not sep or not ch.isdigit():
        raise ValueError(f"malformed chapter partition key: {key!r}")
    return sha, int(ch)
