"""book_chapter_resolved: DECIDE ONLY — the same canonicalization+cosine+LLM ladder as
resolved_entities, against the same global pgvector tables, so a Lévy process in a book
resolves to the same Concept as a Lévy process in a paper. Writes no Neo4j/embeddings."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.resolved_entities import resolved_concept_row
from pipeline.embedding import embed_texts
from pipeline.resolution.resolver import (
    adjudicate,
    lookup_by_key,
    nearest,
    record_decision,
    resolve_concepts,
    similarity_to,
)
from pipeline.runtime.partitions import book_chapters_partitions_def
from pipeline.runtime.storage import EXTRACTED_BUCKET


def passthrough_payload(payload: dict, resolved_rows: list[dict],
                        alias_rows: list[dict]) -> dict:
    return {**payload, "concepts": resolved_rows, "alias_registrations": alias_rows}


@asset(partitions_def=book_chapters_partitions_def(), deps=["book_chapter_extraction"],
       required_resource_keys={"minio", "openai", "postgres"})
def book_chapter_resolved(context) -> MaterializeResult:
    pkey = context.partition_key
    s3 = context.resources.minio.get_client()
    payload = json.loads(
        s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.json")["Body"].read())

    cfg = context.resources.openai
    client = cfg.get_client()
    concepts = payload.get("concepts", [])
    names = [c["name"] for c in concepts]
    vecs = embed_texts(client, names, model=cfg.embedding_model, timeout=cfg.request_timeout)

    counts: dict[str, int] = {}
    with context.resources.postgres.connect() as conn:
        with conn.cursor() as cur:
            resolutions, aliases = resolve_concepts(
                concepts, vecs,
                lookup_by_key=lambda label, k: lookup_by_key(cur, label, k),
                nearest=lambda label, emb: nearest(cur, label, emb),
                similarity_to=lambda label, canon, emb: similarity_to(cur, label, canon, emb),
                adjudicate=lambda cand, canon: adjudicate(
                    client, cfg.effective_adjudication_model, cand, canon,
                    timeout=cfg.request_timeout),
            )
            for r in resolutions:
                counts[r.action] = counts.get(r.action, 0) + 1
                record_decision(cur, r.surface, r.matched_to, "Concept", r.score,
                                r.action, context.run_id, note=r.note)
        conn.commit()  # decision rows ONLY — graph_write-side owns embeddings + alias_map

    out = passthrough_payload(
        payload,
        resolved_rows=[resolved_concept_row(r.surface, r.canonical, r.kind, r.action,
                                            r.embedding) for r in resolutions],
        alias_rows=[{"key": a.key, "canonical": a.canonical, "source": a.source}
                    for a in aliases])
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.resolved.json",
                  Body=json.dumps(out).encode("utf-8"))
    return MaterializeResult(metadata={k: MetadataValue.int(v) for k, v in counts.items()})
