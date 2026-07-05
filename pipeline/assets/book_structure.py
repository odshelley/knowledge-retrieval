"""book_structure: chapter/section tree from outline (or heading fallback); registers one
book_chapters dynamic partition per chapter. Quarantine when no structure is recoverable."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.outline import NoStructureError, build_structure, choose_toc, structure_artifact
from pipeline.books.parsing import TocEntry
from pipeline.runtime.partitions import BOOK_CHAPTERS_PARTITION, books_partitions_def
from pipeline.runtime.storage import PARSED_BUCKET, TRIAGE_BUCKET


@asset(partitions_def=books_partitions_def(), deps=["book_parsed", "book_metadata"],
       required_resource_keys={"minio"})
def book_structure(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    parsed = json.loads(
        s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json")["Body"].read())
    meta = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.book.json")["Body"].read())

    toc = [TocEntry(**e) for e in parsed["toc"]]
    chosen = choose_toc(toc, parsed["pages"])
    try:
        chapters = build_structure(chosen, n_pages=len(parsed["pages"]))
    except NoStructureError as exc:
        raise QuarantineError(
            f"{key}: no-structure — outline had {len(toc)} entries, heading fallback "
            f"found {len(chosen)} chapters; cannot build chapter tree.") from exc

    artifact = structure_artifact(meta["book_id"], key, chapters)
    s3.put_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.structure.json",
                  Body=json.dumps(artifact).encode("utf-8"))

    chapter_keys = [ch["key"] for ch in artifact["chapters"]]
    context.instance.add_dynamic_partitions(BOOK_CHAPTERS_PARTITION, chapter_keys)
    return MaterializeResult(metadata={
        "book_id": meta["book_id"],
        "chapters": MetadataValue.int(len(artifact["chapters"])),
        "sections": MetadataValue.int(sum(len(c["sections"]) for c in artifact["chapters"])),
        "chapter_partitions": MetadataValue.text(", ".join(chapter_keys)),
    })
