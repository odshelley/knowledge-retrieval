"""extracted_graph: run extraction over this paper's chunk artifact; emit candidate entities.
Reads the chunk artifact (not Neo4j — chunks are artifact-only now). Output stashed in MinIO
JSON feeds resolved_entities + graph_write."""
from __future__ import annotations

import json
from dataclasses import asdict

from dagster import MaterializeResult, MetadataValue, asset
from openai import OpenAI

from pipeline.extraction import extract_from_chunk, merge_results
from pipeline.partitions import documents_partitions_def
from pipeline.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET


@asset(partitions_def=documents_partitions_def(), deps=["chunks", "triage_metadata"],
       required_resource_keys={"minio", "openai"})
def extracted_graph(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    chunk_rows = json.loads(s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.json")["Body"].read())
    texts = [c["text"] for c in sorted(chunk_rows, key=lambda c: c["position"]) if c["text"]]

    cfg = context.resources.openai
    client = OpenAI(api_key=cfg.api_key)
    merged = merge_results([extract_from_chunk(client, cfg.extraction_model, t) for t in texts])

    payload = {
        "concepts": [asdict(c) for c in merged.concepts],
        "definitions": [asdict(d) for d in merged.definitions],
        "results": [asdict(r) for r in merged.results],
    }
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={
        "concepts": MetadataValue.int(len(merged.concepts)),
        "definitions": MetadataValue.int(len(merged.definitions)),
        "results": MetadataValue.int(len(merged.results)),
    })
