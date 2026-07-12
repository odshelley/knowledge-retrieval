"""graph_write: the SOLE writer of the derived graph (spec §5.9). Reads chunk/resolved/triage
artifacts and writes, all idempotent MERGE: Chunk nodes (+emb), Concept nodes (+ pgvector
embedding upsert for new canonicals, as one unit), Definition/Result nodes (paper-local
content-hash ids), and CITES (forward + backward backfill via pending_citations)."""
from __future__ import annotations

import hashlib
import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.embedding import embed_texts
from pipeline.runtime.partitions import documents_partitions_def
from pipeline.resolution.resolver import upsert_embedding, upsert_alias
from pipeline.runtime.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET, TRIAGE_BUCKET
from pipeline.text_norm import normalize_statement


def _hash12(s: str) -> str:
    return hashlib.sha1(normalize_statement(s).encode("utf-8")).hexdigest()[:12]


def def_id(paper_id: str, statement: str) -> str:
    return f"{paper_id}:def:{_hash12(statement)}"


def result_id(paper_id: str, kind: str, statement: str) -> str:
    return f"{paper_id}:{kind}:{_hash12(statement)}"


def concept_rows(concepts: list[dict]) -> list[dict]:
    return [{"name": c["name"], "tags": [c["kind"]],
             "description": c.get("description", "")} for c in concepts]


def definition_rows(paper_id: str, defs: list[dict]) -> list[dict]:
    return [{"id": def_id(paper_id, d["statement"]), "term": d["term"],
             "statement": d["statement"]} for d in defs]


def result_rows(paper_id: str, results: list[dict]) -> list[dict]:
    return [{"id": result_id(paper_id, r["kind"], r["statement"]), "name": r.get("name", ""),
             "kind": r["kind"], "statement": r["statement"]} for r in results]


def result_name_index(rrows: list[dict]) -> dict[str, str]:
    """Map result label -> result id, EXCLUDING empty and ambiguous (duplicate) labels.

    Result identity is (kind, normalized statement), NOT name, so two distinct results can
    share a label. Keying a plain dict on name would let a depends_on reference resolve to the
    wrong Result (last-wins). Dropping ambiguous labels makes those references skip+count
    instead of fabricating a wrong edge.
    """
    counts: dict[str, int] = {}
    for r in rrows:
        if r["name"]:
            counts[r["name"]] = counts.get(r["name"], 0) + 1
    return {r["name"]: r["id"] for r in rrows if r["name"] and counts[r["name"]] == 1}


def defines_edge_rows(paper_id: str, definitions: list[dict],
                      surface_to_canon: dict[str, str]) -> tuple[list[dict], int]:
    """Definition -> Concept rows. `surface_to_canon` is keyed on LOWERCASED surface names
    (concepts are deduped case-insensitively upstream). Skips names with no canonical."""
    rows, skipped = [], 0
    for d in definitions:
        did = def_id(paper_id, d["statement"])
        for name in d.get("defines", []):
            canon = surface_to_canon.get(name.lower())
            if canon is None:
                skipped += 1
                continue
            rows.append({"def_id": did, "canonical": canon})
    return rows, skipped


def uses_edge_rows(paper_id: str, results: list[dict],
                   surface_to_canon: dict[str, str]) -> tuple[list[dict], int]:
    """Result -> Concept rows. `surface_to_canon` is keyed on LOWERCASED surface names.
    Skips names with no canonical."""
    rows, skipped = [], 0
    for r in results:
        rid = result_id(paper_id, r["kind"], r["statement"])
        for name in r.get("uses", []):
            canon = surface_to_canon.get(name.lower())
            if canon is None:
                skipped += 1
                continue
            rows.append({"res_id": rid, "canonical": canon})
    return rows, skipped


def depends_on_edge_rows(paper_id: str, results: list[dict],
                         name_to_result_id: dict[str, str]) -> tuple[list[dict], int]:
    """Result -> Result rows. Skips unknown/ambiguous result names and self-dependencies.

    `name_to_result_id` must be collision-safe (e.g. from `result_name_index`) — ambiguous
    labels are expected to be filtered upstream, not here.
    """
    rows, skipped = [], 0
    for r in results:
        rid = result_id(paper_id, r["kind"], r["statement"])
        for dep_name in r.get("depends_on", []):
            dep = name_to_result_id.get(dep_name)
            if dep is None or dep == rid:
                skipped += 1
                continue
            rows.append({"res_id": rid, "dep_id": dep})
    return rows, skipped


def mention_rows(provenance: dict, surface_to_canon: dict[str, str]) -> tuple[list[dict], int]:
    """Chunk-MENTIONS->Concept rows from extraction provenance, mapped through the resolver's
    surface->canonical table (lowercased keys). Skips surfaces with no canonical."""
    rows, skipped = [], 0
    for surface_l, cids in provenance.get("concepts", {}).items():
        canon = surface_to_canon.get(surface_l)
        if canon is None:
            skipped += 1
            continue
        rows.extend({"chunk_id": cid, "canonical": canon} for cid in cids)
    return rows, skipped


def extracted_from_rows(paper_id: str, raw_defs: list[dict], raw_results: list[dict],
                        provenance: dict) -> tuple[list[dict], list[dict]]:
    """Definition/Result -EXTRACTED_FROM-> Chunk rows; node ids recomputed with the same
    content-hash scheme the node writers use, provenance looked up by the same keys."""
    drows = []
    for d in raw_defs:
        k = normalize_statement(d["statement"])
        drows.extend({"node_id": def_id(paper_id, d["statement"]), "chunk_id": cid}
                     for cid in provenance.get("definitions", {}).get(k, []))
    rrows = []
    for r in raw_results:
        k = f"{r['kind']}|{normalize_statement(r['statement'])}"
        rrows.extend({"node_id": result_id(paper_id, r["kind"], r["statement"]), "chunk_id": cid}
                     for cid in provenance.get("results", {}).get(k, []))
    return drows, rrows


# --- Cypher -------------------------------------------------------------------
WRITE_CHUNKS = """
MERGE (d:Document {id:$doc_id}) SET d.paper_id = $paper_id
WITH d
// Link the Paper to its Document so Paper->Document->Chunk is traversable. Guarded by
// OPTIONAL MATCH + FOREACH: if the Paper node is somehow absent, chunk writing below must
// still proceed rather than the whole query returning no rows.
OPTIONAL MATCH (p:Paper {id:$paper_id})
FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END |
  MERGE (p)-[:HAS_DOCUMENT]->(d))
WITH d
UNWIND $rows AS row
  MERGE (c:Chunk {id: row.id})
  SET c.text = row.text, c.position = row.position, c.embedding = row.embedding
  MERGE (c)-[:BELONGS_TO]->(d)
"""

WRITE_CONCEPTS = """
MATCH (p:Paper {id:$paper_id})
UNWIND $rows AS row
  MERGE (c:Concept {name: row.name})
  SET c.tags = row.tags
  SET c.description = coalesce(c.description,
        CASE WHEN row.description = '' THEN NULL ELSE row.description END)
  MERGE (p)-[:DISCUSSES]->(c)
  MERGE (c)-[:DERIVED_FROM]->(p)
"""

WRITE_DEFINITIONS = """
MATCH (p:Paper {id:$paper_id})
UNWIND $rows AS row
  MERGE (d:Definition {id: row.id})
  SET d.term = row.term, d.statement = row.statement
  MERGE (p)-[:STATES]->(d)
"""

WRITE_RESULTS = """
MATCH (p:Paper {id:$paper_id})
UNWIND $rows AS row
  MERGE (r:Result {id: row.id})
  SET r.name = row.name, r.kind = row.kind, r.statement = row.statement
  MERGE (p)-[:STATES]->(r)
"""

FIND_CITED = """
MATCH (cited:Paper)
  WHERE ($s2 IS NOT NULL AND cited.s2_id=$s2)
     OR ($doi IS NOT NULL AND cited.doi=$doi)
     OR ($arxiv IS NOT NULL AND cited.arxiv_id=$arxiv)
RETURN cited.id AS id LIMIT 1
"""
MERGE_CITES = "MATCH (a:Paper {id:$citing}),(b:Paper {id:$cited}) MERGE (a)-[:CITES]->(b)"

WRITE_DEFINES = """
UNWIND $rows AS row
  MATCH (d:Definition {id: row.def_id})
  MATCH (c:Concept {name: row.canonical})
  MERGE (d)-[:DEFINES]->(c)
"""

WRITE_RESULT_USES = """
UNWIND $rows AS row
  MATCH (r:Result {id: row.res_id})
  MATCH (c:Concept {name: row.canonical})
  MERGE (r)-[:USES]->(c)
"""

WRITE_RESULT_DEPENDS = """
UNWIND $rows AS row
  MATCH (r1:Result {id: row.res_id})
  MATCH (r2:Result {id: row.dep_id})
  MERGE (r1)-[:DEPENDS_ON]->(r2)
"""

# Provenance edges are keyed to positional chunk ids ({doc}:{i}). On re-materialization after a
# re-chunk, WRITE_CHUNKS overwrites a reused chunk id's text in place, but MERGE-only provenance
# writes would leave the previous run's MENTIONS/EXTRACTED_FROM attached to that now-different
# text. Clear this document's provenance edges first so each run's writes are authoritative.
CLEAR_DOC_PROVENANCE = """
MATCH (d:Document {id: $doc_id})<-[:BELONGS_TO]-(ch:Chunk)
CALL {
  WITH ch
  OPTIONAL MATCH (ch)-[m:MENTIONS]->()
  OPTIONAL MATCH ()-[e:EXTRACTED_FROM]->(ch)
  DELETE m, e
}
"""

WRITE_MENTIONS = """
UNWIND $rows AS row
  MATCH (ch:Chunk {id: row.chunk_id})
  MATCH (c:Concept {name: row.canonical})
  MERGE (ch)-[:MENTIONS]->(c)
"""

WRITE_DEF_PROVENANCE = """
UNWIND $rows AS row
  MATCH (d:Definition {id: row.node_id})
  MATCH (ch:Chunk {id: row.chunk_id})
  MERGE (d)-[:EXTRACTED_FROM]->(ch)
"""

WRITE_RESULT_PROVENANCE = """
UNWIND $rows AS row
  MATCH (r:Result {id: row.node_id})
  MATCH (ch:Chunk {id: row.chunk_id})
  MERGE (r)-[:EXTRACTED_FROM]->(ch)
"""

CONCEPTS_NEEDING_EMBEDDING = """
UNWIND $names AS name
MATCH (c:Concept {name: name})
WHERE c.embedding IS NULL AND c.description IS NOT NULL
RETURN c.name AS name, c.description AS description
"""

SET_CONCEPT_EMBEDDINGS = """
UNWIND $rows AS row
MATCH (c:Concept {name: row.name})
CALL db.create.setNodeVectorProperty(c, 'embedding', row.embedding)
"""

_MATCH_PENDING = ("ref_s2_id=%s OR ref_doi=%s OR ref_arxiv_id=%s OR ref_title_norm=%s")


@asset(partitions_def=documents_partitions_def(),
       deps=["resolved_entities", "chunks", "triage_metadata"],
       required_resource_keys={"minio", "neo4j_new", "postgres", "openai"})
def graph_write(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    resolved = json.loads(s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json")["Body"].read())
    chunks = json.loads(s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.json")["Body"].read())
    triage = json.loads(s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.json")["Body"].read())

    paper_id = triage["paper_id"]
    ids = triage.get("identifiers", {})
    concepts = resolved.get("concepts", [])
    raw_defs = resolved.get("definitions", [])
    raw_results = resolved.get("results", [])
    crows = concept_rows(concepts)
    drows = definition_rows(paper_id, raw_defs)
    rrows = result_rows(paper_id, raw_results)

    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        s.run(WRITE_CHUNKS, doc_id=key, paper_id=paper_id, rows=chunks)
        s.run(WRITE_CONCEPTS, paper_id=paper_id, rows=crows)
        s.run(WRITE_DEFINITIONS, paper_id=paper_id, rows=drows)
        s.run(WRITE_RESULTS, paper_id=paper_id, rows=rrows)

        # Lowercased keys: concepts are deduped case-insensitively upstream, so link names
        # (which may differ in case from the kept concept) must resolve case-insensitively.
        surface_to_canon = {c.get("surface", c["name"]).lower(): c["name"] for c in concepts}
        name_to_result_id = result_name_index(rrows)  # collision-safe (drops ambiguous labels)
        def_edges, sk_def = defines_edge_rows(paper_id, raw_defs, surface_to_canon)
        use_edges, sk_use = uses_edge_rows(paper_id, raw_results, surface_to_canon)
        dep_edges, sk_dep = depends_on_edge_rows(paper_id, raw_results, name_to_result_id)
        s.run(WRITE_DEFINES, rows=def_edges)
        s.run(WRITE_RESULT_USES, rows=use_edges)
        s.run(WRITE_RESULT_DEPENDS, rows=dep_edges)

        provenance = resolved.get("provenance", {})  # absent on pre-change artifacts
        m_rows, sk_mention = mention_rows(provenance, surface_to_canon)
        dprov_rows, rprov_rows = extracted_from_rows(paper_id, raw_defs, raw_results, provenance)
        # Drop this document's stale provenance edges before rewriting (re-materialization safety).
        s.run(CLEAR_DOC_PROVENANCE, doc_id=key)
        s.run(WRITE_MENTIONS, rows=m_rows)
        s.run(WRITE_DEF_PROVENANCE, rows=dprov_rows)
        s.run(WRITE_RESULT_PROVENANCE, rows=rprov_rows)

        # Retrieval embedding: name+description, only for concepts that don't have one yet
        # (first-wins, mirrors the description coalesce; keeps re-runs idempotent).
        # Dedup names: crows carry one row per surface, so two surfaces resolving to the same
        # canonical would otherwise embed the identical text twice.
        need = s.run(CONCEPTS_NEEDING_EMBEDDING,
                     names=sorted({c["name"] for c in crows})).data()
        if need:
            cfg = context.resources.openai
            vecs = embed_texts(cfg.get_client(),
                               [f"{r['name']}: {r['description']}" for r in need],
                               model=cfg.embedding_model, timeout=cfg.request_timeout)
            s.run(SET_CONCEPT_EMBEDDINGS,
                  rows=[{"name": r["name"], "embedding": v} for r, v in zip(need, vecs)])

        with context.resources.postgres.connect() as conn:
            with conn.cursor() as cur:
                # pgvector embedding upsert for newly-created Concepts (one unit with the node).
                # Only rows that CREATE a canonical carry an embedding (resolver sets it None on
                # merges); merged rows reuse the canonical's already-stored vector. This keeps the
                # upsert deterministic — exactly one vector per canonical, not last-write-wins.
                for c in concepts:
                    if c.get("embedding") is not None:
                        upsert_embedding(cur, c["name"], "Concept", c["embedding"])
                # Sole writer of alias_map (spec rev 2 §7): register canonical_key -> canonical,
                # co-located with the Concept node + embedding so an alias never precedes its node.
                for reg in resolved.get("alias_registrations", []):
                    upsert_alias(cur, "Concept", reg["key"], reg["canonical"], reg["source"])
                # forward: this paper → its references
                for ref in triage.get("references", []):
                    found = s.run(FIND_CITED, s2=ref.get("s2_id"), doi=ref.get("doi"),
                                  arxiv=ref.get("arxiv_id")).single()
                    if found:
                        s.run(MERGE_CITES, citing=paper_id, cited=found["id"])
                    else:
                        cur.execute(
                            "INSERT INTO pending_citations (citing_paper_id, ref_doi, "
                            "ref_arxiv_id, ref_title_norm, ref_s2_id, influential_count) "
                            "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (paper_id, ref.get("doi"), ref.get("arxiv_id"),
                             ref.get("title_norm"), ref.get("s2_id"),
                             ref.get("influential_count", 0)))
                # backward: prior pending refs that point at THIS paper
                params = (ids.get("s2_id"), ids.get("doi"), ids.get("arxiv_id"), ids.get("title_norm"))
                cur.execute(
                    f"SELECT citing_paper_id FROM pending_citations WHERE NOT resolved AND ({_MATCH_PENDING})",
                    params)
                for (citing_id,) in cur.fetchall():
                    s.run(MERGE_CITES, citing=citing_id, cited=paper_id)
                cur.execute(
                    f"UPDATE pending_citations SET resolved=true WHERE NOT resolved AND ({_MATCH_PENDING})",
                    params)
            conn.commit()
    return MaterializeResult(metadata={
        "chunks": MetadataValue.int(len(chunks)),
        "concepts": MetadataValue.int(len(crows)),
        "definitions": MetadataValue.int(len(drows)),
        "results": MetadataValue.int(len(rrows)),
        "defines": MetadataValue.int(len(def_edges)),
        "uses": MetadataValue.int(len(use_edges)),
        "depends_on": MetadataValue.int(len(dep_edges)),
        "skipped_refs": MetadataValue.int(sk_def + sk_use + sk_dep),
        "skipped_def_concepts": MetadataValue.int(sk_def),
        "skipped_use_concepts": MetadataValue.int(sk_use),
        "skipped_dep_results": MetadataValue.int(sk_dep),
        "mentions": MetadataValue.int(len(m_rows)),
        "skipped_mention_surfaces": MetadataValue.int(sk_mention),
        "extracted_from": MetadataValue.int(len(dprov_rows) + len(rprov_rows)),
        "concept_embeddings": MetadataValue.int(len(need)),
    })
