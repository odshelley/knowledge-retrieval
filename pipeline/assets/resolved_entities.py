"""resolved_entities: DECIDE ONLY. Runs the canonicalization+cosine+LLM ladder (pipeline.resolution.resolver.
resolve_concepts), records decision rows, and emits resolved concepts + alias registrations for
graph_write. Writes no Neo4j, no embeddings, no alias_map (graph_write owns those — spec rev 2 §7)."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.embedding import embed_texts
from pipeline.runtime.partitions import documents_partitions_def
from pipeline.resolution.resolver import (
    adjudicate,
    lookup_by_key,
    nearest,
    record_decision,
    resolve_concepts,
    similarity_to,
)
from pipeline.runtime.storage import EXTRACTED_BUCKET


def resolved_concept_row(surface: str, canonical: str, kind: str, action: str,
                         embedding: list[float]) -> dict:
    """One resolved-concept record (one per original surface). `surface` is the extracted name used by
    graph_write to attach defines/uses edges; `name` is the canonical node key; `embedding` is the
    vector graph_write upserts keyed on the canonical name — non-None only on rows that CREATE a
    canonical, None on merges (the resolver guarantees one vector per canonical, see resolve_concepts)."""
    return {"surface": surface, "name": canonical, "kind": kind,
            "action": action, "embedding": embedding}


@asset(partitions_def=documents_partitions_def(), deps=["extracted_graph"],
       required_resource_keys={"minio", "openai", "postgres"})
def resolved_entities(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    payload = json.loads(s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.json")["Body"].read())

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
                    client, cfg.effective_adjudication_model, cand, canon, timeout=cfg.request_timeout),
            )
            for r in resolutions:
                counts[r.action] = counts.get(r.action, 0) + 1
                record_decision(cur, r.surface, r.matched_to, "Concept", r.score,
                                r.action, context.run_id, note=r.note)
        conn.commit()  # decision rows ONLY — no Neo4j, no embeddings, no alias_map (graph_write owns).

    payload["concepts"] = [
        resolved_concept_row(r.surface, r.canonical, r.kind, r.action, r.embedding)
        for r in resolutions
    ]
    payload["alias_registrations"] = [
        {"key": a.key, "canonical": a.canonical, "source": a.source} for a in aliases
    ]
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={k: MetadataValue.int(v) for k, v in counts.items()})
