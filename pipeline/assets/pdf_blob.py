from __future__ import annotations

import hashlib

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.partitions import get_partition, partitions_def
from pipeline.resources import MinIOResource
from pipeline.storage import PDFS_BUCKET


def compute_pdf_metadata(s3_client, key: str) -> dict:
    obj = s3_client.get_object(Bucket=PDFS_BUCKET, Key=key)
    h = hashlib.sha256()
    size = 0
    for chunk in obj["Body"].iter_chunks(chunk_size=1 << 20):
        h.update(chunk)
        size += len(chunk)
    return {"sha256": h.hexdigest(), "size_bytes": size}


@asset(partitions_def=partitions_def(), required_resource_keys={"minio"})
def pdf_blob(context) -> MaterializeResult:
    """The PDF for this paper, sitting in MinIO. Asset value is metadata about the blob."""
    paper_id = context.partition_key
    part = get_partition(paper_id)
    if part is None:
        raise ValueError(f"unknown partition: {paper_id}")

    s3: MinIOResource = context.resources.minio
    meta = compute_pdf_metadata(s3.get_client(), f"{paper_id}.pdf")
    return MaterializeResult(
        metadata={
            "paper_id": paper_id,
            "title": part["title"],
            "key": f"{PDFS_BUCKET}/{paper_id}.pdf",
            "sha256": meta["sha256"],
            "size_bytes": MetadataValue.int(meta["size_bytes"]),
        },
    )
