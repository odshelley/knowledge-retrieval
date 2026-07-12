"""extracted_graph: run extraction over this paper's chunk artifact; emit candidate entities.
Reads the chunk artifact (not Neo4j — chunks are artifact-only now). Output stashed in MinIO
JSON feeds resolved_entities + graph_write."""
from __future__ import annotations

import json
import os
import time

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.extraction.extraction import extract_from_chunk, merge_results_with_provenance
from pipeline.extraction.extraction_anthropic import extract_from_chunk_anthropic
from pipeline.runtime.partitions import documents_partitions_def
from pipeline.runtime.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET


@asset(partitions_def=documents_partitions_def(), deps=["chunks", "triage_metadata"],
       required_resource_keys={"minio", "openai", "anthropic"})
def extracted_graph(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    chunk_rows = json.loads(s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.json")["Body"].read())
    ordered = [c for c in sorted(chunk_rows, key=lambda c: c["position"]) if c["text"]]
    texts = [c["text"] for c in ordered]
    ids = [c["id"] for c in ordered]

    provider = os.environ.get("EXTRACTION_PROVIDER", "openai").lower()
    if provider == "anthropic":
        ar = context.resources.anthropic
        aclient = ar.get_client()
        def extract_one(t):
            return extract_from_chunk_anthropic(aclient, ar.extraction_model, t, timeout=ar.request_timeout)
        model_label = ar.extraction_model
    else:
        cfg = context.resources.openai
        oclient = cfg.get_client()
        def extract_one(t):
            return extract_from_chunk(oclient, cfg.extraction_model, t, timeout=cfg.request_timeout)
        model_label = cfg.extraction_model

    n = len(texts)
    context.log.info(f"extraction: {n} chunks via {provider}/{model_label} (sequential)")
    try:
        parts = []
        for i, t in enumerate(texts):
            t0 = time.monotonic()
            parts.append(extract_one(t))
            context.log.info(f"extraction: chunk {i + 1}/{n} done in {time.monotonic() - t0:.1f}s")
        merged, provenance = merge_results_with_provenance(parts, ids)
    except (json.JSONDecodeError, ValueError, KeyError, IndexError, AttributeError) as exc:
        raise QuarantineError(f"{key}: extraction returned unparseable/invalid JSON") from exc

    payload = {
        "concepts": [c.model_dump() for c in merged.concepts],
        "definitions": [d.model_dump() for d in merged.definitions],
        "results": [r.model_dump() for r in merged.results],
        "provenance": provenance,
    }
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={
        "concepts": MetadataValue.int(len(merged.concepts)),
        "definitions": MetadataValue.int(len(merged.definitions)),
        "results": MetadataValue.int(len(merged.results)),
        "provider": MetadataValue.text(provider),
        "model": MetadataValue.text(model_label),
    })
