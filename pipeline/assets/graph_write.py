"""graph_write: the SOLE writer of the derived graph (spec §5.9). Reads chunk/resolved/triage
artifacts and writes, all idempotent MERGE: Chunk nodes (+emb), Concept nodes (+ pgvector
embedding upsert for new canonicals, as one unit), Definition/Result nodes (paper-local
content-hash ids), and CITES (forward + backward backfill via pending_citations)."""
from __future__ import annotations

import hashlib
import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.runtime.partitions import documents_partitions_def
from pipeline.resolver import upsert_embedding, upsert_alias
from pipeline.runtime.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET, TRIAGE_BUCKET
from pipeline.text_norm import normalize_statement


def _hash12(s: str) -> str:
    return hashlib.sha1(normalize_statement(s).encode("utf-8")).hexdigest()[:12]


def def_id(paper_id: str, statement: str) -> str:
    return f"{paper_id}:def:{_hash12(statement)}"


def result_id(paper_id: str, kind: str, statement: str) -> str:
    return f"{paper_id}:{kind}:{_hash12(statement)}"


def concept_rows(concepts: list[dict]) -> list[dict]:
    return [{"name": c["name"], "tags": [c["kind"]]} for c in concepts]


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


# --- Cypher -------------------------------------------------------------------
WRITE_CHUNKS = """
MERGE (d:Document {id:$doc_id}) SET d.paper_id = $paper_id
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

_MATCH_PENDING = ("ref_s2_id=%s OR ref_doi=%s OR ref_arxiv_id=%s OR ref_title_norm=%s")


@asset(partitions_def=documents_partitions_def(),
       deps=["resolved_entities", "chunks", "triage_metadata"],
       required_resource_keys={"minio", "neo4j_new", "postgres"})
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
    })
