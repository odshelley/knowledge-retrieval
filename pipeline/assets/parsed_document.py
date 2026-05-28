"""parsed_document: Docling → markdown+LaTeX in MinIO. Quarantine on empty parse."""
from __future__ import annotations

import tempfile
from pathlib import Path

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.parsing import parse_pdf
from pipeline.partitions import documents_partitions_def
from pipeline.storage import PARSED_BUCKET, RAW_BUCKET


class QuarantineError(Exception):
    """Raised when a document cannot be parsed to usable text."""


@asset(partitions_def=documents_partitions_def(), deps=["raw_blob"],
       required_resource_keys={"minio"})
def parsed_document(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")
    with tempfile.TemporaryDirectory() as td:
        pdf_path = Path(td) / f"{key}.pdf"
        pdf_path.write_bytes(obj["Body"].read())
        result = parse_pdf(str(pdf_path))
    if result.is_empty:
        raise QuarantineError(
            f"{key}: Docling produced empty output (likely image-only or corrupt). "
            "Surfaced, not skipped."
        )
    s3.put_object(Bucket=PARSED_BUCKET, Key=f"{key}.md", Body=result.markdown.encode("utf-8"))
    return MaterializeResult(metadata={
        "key": f"{PARSED_BUCKET}/{key}.md",
        "mode": result.mode,
        "chars": MetadataValue.int(len(result.markdown)),
    })
