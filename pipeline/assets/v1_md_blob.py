from __future__ import annotations

import hashlib

import botocore.exceptions
from dagster import MaterializeResult, MetadataValue, asset

from pipeline.partitions import get_partition, partitions_def


def fetch_md_metadata(s3_client, key: str) -> dict:
    try:
        obj = s3_client.get_object(Bucket="legacy-summaries", Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {"present": False}
        raise
    h = hashlib.sha256()
    size = 0
    for chunk in obj["Body"].iter_chunks(chunk_size=1 << 20):
        h.update(chunk)
        size += len(chunk)
    return {"present": True, "sha256": h.hexdigest(), "size_bytes": size}


@asset(partitions_def=partitions_def(), required_resource_keys={"minio"})
def v1_md_blob(context) -> MaterializeResult:
    paper_id = context.partition_key
    part = get_partition(paper_id)
    if part is None:
        raise ValueError(f"unknown partition: {paper_id}")

    s3 = context.resources.minio.get_client()
    meta = fetch_md_metadata(s3, f"{paper_id}.md")
    md = {"paper_id": paper_id, "present": meta["present"]}
    if meta["present"]:
        md["sha256"] = meta["sha256"]
        md["size_bytes"] = MetadataValue.int(meta["size_bytes"])
        md["key"] = f"legacy-summaries/{paper_id}.md"
    return MaterializeResult(metadata=md)
