"""book_raw_blob: ensure this book's PDF is in MinIO, keyed by content hash."""
from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.raw_blob import _upload_if_absent
from pipeline.ingest.source import books_source_dir, list_pdf_files
from pipeline.runtime.partitions import books_partitions_def, hash_bytes
from pipeline.runtime.storage import RAW_BUCKET


@asset(partitions_def=books_partitions_def(), required_resource_keys={"minio"})
def book_raw_blob(context) -> MaterializeResult:
    key = context.partition_key  # = content hash
    match, data = None, b""
    for p in list_pdf_files(books_source_dir()):
        candidate = p.read_bytes()
        if hash_bytes(candidate) == key:
            match, data = p, candidate
            break
    if match is None:
        raise ValueError(f"no source book PDF matches partition {key}")
    s3 = context.resources.minio.get_client()
    uploaded = _upload_if_absent(s3, RAW_BUCKET, f"{key}.pdf", data)
    return MaterializeResult(metadata={
        "key": f"{RAW_BUCKET}/{key}.pdf",
        "source_filename": match.name,
        "size_bytes": MetadataValue.int(len(data)),
        "uploaded": uploaded,
    })
