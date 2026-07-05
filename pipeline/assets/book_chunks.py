"""book_chunks: section-aware chunking + embeddings → MinIO artifact. No Neo4j writes;
book_structure_write creates Chunk nodes from this artifact."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.books.chunking import section_chunk_rows
from pipeline.embedding import embed_texts
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import CHUNKS_BUCKET, PARSED_BUCKET, TRIAGE_BUCKET

_EMBED_BATCH = 128


@asset(partitions_def=books_partitions_def(), deps=["book_structure"],
       required_resource_keys={"minio", "openai"})
def book_chunks(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    pages = json.loads(
        s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json")["Body"].read())["pages"]
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.structure.json")["Body"].read())

    rows: list[dict] = []
    for chapter in structure["chapters"]:
        for section in chapter["sections"]:
            rows.extend(section_chunk_rows(key, chapter, section, pages))
    for i, row in enumerate(rows):  # global position across the book, stable ordering
        row["position"] = i

    cfg = context.resources.openai
    client = cfg.get_client()
    texts = [r["text"] for r in rows]
    vectors: list[list[float]] = []
    for start in range(0, len(texts), _EMBED_BATCH):
        vectors.extend(embed_texts(client, texts[start:start + _EMBED_BATCH],
                                   model=cfg.embedding_model, timeout=cfg.request_timeout))
    for row, vec in zip(rows, vectors, strict=True):
        row["embedding"] = vec

    s3.put_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.book.json",
                  Body=json.dumps(rows).encode("utf-8"))
    return MaterializeResult(metadata={"chunks": MetadataValue.int(len(rows))})
