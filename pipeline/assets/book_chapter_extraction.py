"""book_chapter_extraction: LLM extraction over one chapter's chunks, per section.
Same provider switch + progress logging as extracted_graph; ~20-40 chunks per run."""
from __future__ import annotations

import json
import os
import time

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.extraction import (
    attach_pages, chapter_payload, chunk_with_context, flatten_concepts,
)
from pipeline.extraction.extraction import extract_from_chunk, merge_results
from pipeline.extraction.extraction_anthropic import extract_from_chunk_anthropic
from pipeline.runtime.partitions import book_chapters_partitions_def, split_chapter_key
from pipeline.runtime.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET, TRIAGE_BUCKET


@asset(partitions_def=book_chapters_partitions_def(),
       required_resource_keys={"minio", "openai", "anthropic"})
def book_chapter_extraction(context) -> MaterializeResult:
    pkey = context.partition_key
    sha, ch_no = split_chapter_key(pkey)
    s3 = context.resources.minio.get_client()
    meta = json.loads(s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{sha}.book.json")["Body"].read())
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{sha}.structure.json")["Body"].read())
    all_chunks = json.loads(
        s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{sha}.book.json")["Body"].read())

    chapter = next(c for c in structure["chapters"] if c["number"] == ch_no)
    chunks = sorted((r for r in all_chunks if r["chapter_key"] == pkey),
                    key=lambda r: r["position"])

    provider = os.environ.get("EXTRACTION_PROVIDER", "openai").lower()
    if provider == "anthropic":
        ar = context.resources.anthropic
        aclient = ar.get_client()

        def extract_one(t):
            return extract_from_chunk_anthropic(aclient, ar.extraction_model, t,
                                                timeout=ar.request_timeout)
        model_label = ar.extraction_model
    else:
        cfg = context.resources.openai
        oclient = cfg.get_client()

        def extract_one(t):
            return extract_from_chunk(oclient, cfg.extraction_model, t,
                                      timeout=cfg.request_timeout)
        model_label = cfg.extraction_model

    n = len(chunks)
    context.log.info(f"extraction: {n} chunks via {provider}/{model_label} (chapter {ch_no})")
    sections_by_id = {s["id"]: s for c in structure["chapters"] for s in c["sections"]}
    per_section: dict[str, list[tuple]] = {}
    try:
        for i, row in enumerate(chunks):
            section = sections_by_id[row["section_id"]]
            t0 = time.monotonic()
            er = extract_one(chunk_with_context(meta.get("title") or meta["book_id"],
                                                chapter, section, row["text"]))
            context.log.info(
                f"extraction: chunk {i + 1}/{n} done in {time.monotonic() - t0:.1f}s")
            per_section.setdefault(row["section_id"], []).append(
                (er, row["page_start"], row["position"]))

        section_outputs, section_merges = [], []
        for sec_id, triples in per_section.items():
            merged = merge_results([er for er, _, _ in triples])
            section_merges.append(merged)
            defs, results, proof_rows = attach_pages(merged, triples)
            section_outputs.append({"section_id": sec_id,
                                    "definitions": defs, "results": results,
                                    "proof_chunks": proof_rows,
                                    "notations": [nt.model_dump()
                                                  for nt in merged.notations]})
    except (json.JSONDecodeError, ValueError, KeyError, IndexError, AttributeError) as exc:
        raise QuarantineError(f"{pkey}: extraction returned unparseable/invalid JSON") from exc

    payload = chapter_payload(structure["book_id"], chapter, section_outputs,
                              flatten_concepts(section_merges))
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={
        "chunks": MetadataValue.int(n),
        "concepts": MetadataValue.int(len(payload["concepts"])),
        "definitions": MetadataValue.int(sum(len(s["definitions"]) for s in section_outputs)),
        "results": MetadataValue.int(sum(len(s["results"]) for s in section_outputs)),
        "provider": MetadataValue.text(provider),
        "model": MetadataValue.text(model_label),
    })
