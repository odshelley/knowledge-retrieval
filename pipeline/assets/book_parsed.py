"""book_parsed: per-page text + outline → MinIO. Quarantine scans and empty parses."""
from __future__ import annotations

import dataclasses
import json
import tempfile
from pathlib import Path

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.parsing import parse_book_pdf
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import PARSED_BUCKET, RAW_BUCKET


@asset(partitions_def=books_partitions_def(), deps=["book_raw_blob"],
       required_resource_keys={"minio"})
def book_parsed(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")
    with tempfile.TemporaryDirectory() as td:
        pdf_path = Path(td) / f"{key}.pdf"
        body = obj["Body"]
        try:
            with pdf_path.open("wb") as f:
                while chunk := body.read(1024 * 1024):
                    f.write(chunk)
        finally:
            body.close()
        parsed = parse_book_pdf(str(pdf_path))
    if parsed.mode == "vlm":
        raise QuarantineError(f"{key}: needs-ocr — no usable text layer (scanned book?).")
    if parsed.is_empty:
        raise QuarantineError(f"{key}: empty parse — corrupt or image-only PDF.")
    artifact = {"pages": parsed.pages,
                "toc": [dataclasses.asdict(e) for e in parsed.toc],
                "mode": parsed.mode}
    s3.put_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json",
                  Body=json.dumps(artifact).encode("utf-8"))
    return MaterializeResult(metadata={
        "key": f"{PARSED_BUCKET}/{key}.pages.json",
        "pages": MetadataValue.int(len(parsed.pages)),
        "toc_entries": MetadataValue.int(len(parsed.toc)),
    })
