"""book_structure_write: writes Book/Author/Document/Chapter/Section/Chunk (+embeddings).
After this asset, vector RAG covers the whole book — extraction has not started yet."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.books.write import (
    WRITE_BOOK, WRITE_BOOK_CHUNKS, WRITE_BOOK_DOCUMENT, WRITE_CHAPTERS, WRITE_SECTIONS,
    chapter_rows, section_rows,
)
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import CHUNKS_BUCKET, TRIAGE_BUCKET

_CHUNK_WRITE_BATCH = 200  # embeddings are ~12 KB each; keep bolt messages bounded


@asset(partitions_def=books_partitions_def(),
       deps=["book_metadata", "book_structure", "book_chunks"],
       required_resource_keys={"minio", "neo4j_new"})
def book_structure_write(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    meta = json.loads(s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.book.json")["Body"].read())
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.structure.json")["Body"].read())
    chunk_rows = json.loads(
        s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.book.json")["Body"].read())

    crows = chapter_rows(structure)
    srows = section_rows(structure)
    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        s.run(WRITE_BOOK, id=meta["book_id"], title=meta.get("title"), year=meta.get("year"),
              edition=meta.get("edition"), publisher=meta.get("publisher"),
              isbn=meta.get("isbn"), document_id=key, authors=meta.get("authors") or [])
        s.run(WRITE_BOOK_DOCUMENT, id=meta["book_id"], doc_id=key)
        s.run(WRITE_CHAPTERS, id=meta["book_id"], rows=crows)
        s.run(WRITE_SECTIONS, rows=srows)
        for start in range(0, len(chunk_rows), _CHUNK_WRITE_BATCH):
            s.run(WRITE_BOOK_CHUNKS, doc_id=key,
                  rows=chunk_rows[start:start + _CHUNK_WRITE_BATCH])
    return MaterializeResult(metadata={
        "book_id": meta["book_id"],
        "chapters": MetadataValue.int(len(crows)),
        "sections": MetadataValue.int(len(srows)),
        "chunks": MetadataValue.int(len(chunk_rows)),
    })
