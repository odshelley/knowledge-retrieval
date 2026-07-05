"""book_metadata: book identity from frontmatter (LLM + ISBN regex), duplicate check.
DECIDES identity only — Book/Author nodes are written by book_structure_write."""
from __future__ import annotations

import json

from dagster import MaterializeResult, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.metadata import book_record, extract_book_frontmatter, frontmatter_head
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import PARSED_BUCKET, TRIAGE_BUCKET

DUP_CHECK_BOOK = "MATCH (b:Book {id:$bid}) RETURN b.document_id AS doc"


@asset(partitions_def=books_partitions_def(), deps=["book_parsed"],
       required_resource_keys={"minio", "neo4j_new", "openai"})
def book_metadata(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    pages = json.loads(
        s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json")["Body"].read())["pages"]

    cfg = context.resources.openai
    client = cfg.get_client()
    try:
        fm = extract_book_frontmatter(client, cfg.extraction_model, frontmatter_head(pages),
                                      timeout=cfg.request_timeout)
        rec = book_record(fm, pages, document_id=key)
    except (json.JSONDecodeError, ValueError, KeyError, AttributeError) as exc:
        raise QuarantineError(f"{key}: book frontmatter extraction failed") from exc

    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(DUP_CHECK_BOOK, bid=rec["book_id"]).single()
        if row and row["doc"] and row["doc"] != key:
            raise QuarantineError(
                f"{key}: duplicate-book-different-bytes — book {rec['book_id']} already "
                f"ingested from document {row['doc']}")

    s3.put_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.book.json",
                  Body=json.dumps(rec).encode("utf-8"))
    return MaterializeResult(metadata={"book_id": rec["book_id"],
                                       "title": rec.get("title") or "",
                                       "isbn": rec.get("isbn") or ""})
