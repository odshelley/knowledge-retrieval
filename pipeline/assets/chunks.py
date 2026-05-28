"""chunks: split parsed markdown + embed → MinIO artifact. No Neo4j write here;
graph_write (Task 14) creates the Chunk nodes from this artifact."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.chunking import split_markdown
from pipeline.embedding import embed_texts
from pipeline.partitions import documents_partitions_def
from pipeline.storage import CHUNKS_BUCKET, PARSED_BUCKET


@asset(partitions_def=documents_partitions_def(), deps=["parsed_document"],
       required_resource_keys={"minio", "openai"})
def chunks(context) -> MaterializeResult:
    key = context.partition_key  # document id = file SHA-256
    s3 = context.resources.minio.get_client()
    md = s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.md")["Body"].read().decode("utf-8")
    parts = split_markdown(md)

    cfg = context.resources.openai
    client = cfg.get_client()
    vectors = embed_texts(client, parts, model=cfg.embedding_model, timeout=cfg.request_timeout)

    artifact = [
        {"id": f"{key}:{i}", "position": i, "text": t, "embedding": v}
        for i, (t, v) in enumerate(zip(parts, vectors, strict=True))
    ]
    s3.put_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(artifact).encode("utf-8"))
    return MaterializeResult(metadata={"chunks": MetadataValue.int(len(artifact))})
