"""resolved_entities: DECIDE ONLY. For each candidate Concept, NN-query pgvector and record the
decision row in Postgres. Writes no Neo4j and upserts no embedding — graph_write owns both
(single-writer, spec §7). Emits resolved concepts (with embeddings) for graph_write."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.embedding import embed_texts
from pipeline.partitions import documents_partitions_def
from pipeline.resolver import Decision, decide, lookup_alias, nearest, record_decision
from pipeline.storage import EXTRACTED_BUCKET


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

    resolved = []
    counts = {"merge": 0, "create": 0, "create_flagged": 0}
    with context.resources.postgres.connect() as conn:
        with conn.cursor() as cur:
            for c, v in zip(concepts, vecs, strict=True):
                # Consult alias map first (spec §7) — definitive MERGE, skip NN query.
                alias_canonical = lookup_alias(cur, "Concept", c["name"])
                if alias_canonical is not None:
                    action, canonical, score = Decision.MERGE, alias_canonical, 1.0
                else:
                    hit = nearest(cur, "Concept", v)
                    if hit is None:
                        action, canonical, score = Decision.CREATE, c["name"], 0.0
                    else:
                        matched, score = hit
                        action = decide(score)
                        canonical = matched if action == Decision.MERGE else c["name"]
                counts[action.value] += 1
                record_decision(cur, c["name"], canonical if action == Decision.MERGE else None,
                                "Concept", score, action.value, context.run_id)
                resolved.append({
                    "name": canonical, "kind": c["kind"], "action": action.value,
                    # graph_write upserts this embedding keyed by canonical name on every run
                    # (idempotent overwrite) so a Concept node can never lack a pgvector row.
                    "embedding": v,
                })
        conn.commit()  # ONLY decision rows are written here — no Neo4j, no embedding upsert.

    payload["concepts"] = resolved
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={k: MetadataValue.int(v) for k, v in counts.items()})
