"""raw_blob: ensure this document's PDF is in MinIO, keyed by content hash."""
from __future__ import annotations

import botocore.exceptions
from dagster import MaterializeResult, MetadataValue, asset

from pipeline.partitions import documents_partitions_def
from pipeline.source import file_partition_key, list_pdf_files, source_dir
from pipeline.storage import RAW_BUCKET


def _upload_if_absent(s3, bucket: str, key: str, data: bytes) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return False
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    return True


@asset(partitions_def=documents_partitions_def(), required_resource_keys={"minio"})
def raw_blob(context) -> MaterializeResult:
    key = context.partition_key  # = content hash
    # Find the source file whose hash matches this partition.
    match = next((p for p in list_pdf_files(source_dir()) if file_partition_key(p) == key), None)
    if match is None:
        raise ValueError(f"no source PDF matches partition {key}")
    data = match.read_bytes()
    s3 = context.resources.minio.get_client()
    uploaded = _upload_if_absent(s3, RAW_BUCKET, f"{key}.pdf", data)
    return MaterializeResult(metadata={
        "key": f"{RAW_BUCKET}/{key}.pdf",
        "source_filename": match.name,
        "size_bytes": MetadataValue.int(len(data)),
        "uploaded": uploaded,
    })
