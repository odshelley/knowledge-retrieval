"""book_chapter_graph_write: writes Concepts (COVERS/COVERED_IN), Definitions/Results
(Section-STATES, chapter-local ids, printed labels + pages), DEFINES/USES edges, and
DEPENDS_ON with cross-chapter back-reference lookup by (book prefix, label). Owns the
pgvector embedding + alias_map upserts for this chapter, mirroring graph_write."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.graph_write import (
    WRITE_DEFINES, WRITE_RESULT_DEPENDS, WRITE_RESULT_USES,
    concept_rows, defines_edge_rows, result_name_index, uses_edge_rows,
)
from pipeline.books.write import (
    FIND_BOOK_RESULT_BY_LABEL, WRITE_BOOK_CONCEPTS, WRITE_BOOK_DEFINITIONS, WRITE_BOOK_RESULTS,
    book_definition_rows, book_result_rows, split_depends_on,
)
from pipeline.resolution.resolver import upsert_alias, upsert_embedding
from pipeline.runtime.partitions import book_chapters_partitions_def
from pipeline.runtime.storage import EXTRACTED_BUCKET


@asset(partitions_def=book_chapters_partitions_def(), deps=["book_chapter_resolved"],
       required_resource_keys={"minio", "neo4j_new", "postgres"})
def book_chapter_graph_write(context) -> MaterializeResult:
    pkey = context.partition_key
    s3 = context.resources.minio.get_client()
    payload = json.loads(
        s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.resolved.json")["Body"].read())

    book_id = payload["book_id"]
    owner = payload["chapter_id"]
    concepts = payload.get("concepts", [])
    crows = concept_rows(concepts)
    surface_to_canon = {c.get("surface", c["name"]).lower(): c["name"] for c in concepts}

    drows, rrows, raw_results = [], [], []
    sk_def = sk_use = 0
    def_edges, use_edges = [], []
    for sec in payload.get("sections", []):
        sid = sec["section_id"]
        drows.extend(book_definition_rows(owner, sid, sec.get("definitions", [])))
        rrows.extend(book_result_rows(owner, sid, sec.get("results", [])))
        raw_results.extend(sec.get("results", []))
        de, sd = defines_edge_rows(owner, sec.get("definitions", []), surface_to_canon)
        ue, su = uses_edge_rows(owner, sec.get("results", []), surface_to_canon)
        def_edges.extend(de)
        use_edges.extend(ue)
        sk_def += sd
        sk_use += su

    name_index = result_name_index(rrows)
    dep_edges, unresolved = split_depends_on(owner, raw_results, name_index)

    new = context.resources.neo4j_new
    cross_linked = cross_skipped = 0
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        s.run(WRITE_BOOK_CONCEPTS, book_id=book_id, rows=crows)
        s.run(WRITE_BOOK_DEFINITIONS, rows=drows)
        s.run(WRITE_BOOK_RESULTS, rows=rrows)
        s.run(WRITE_DEFINES, rows=def_edges)
        s.run(WRITE_RESULT_USES, rows=use_edges)
        s.run(WRITE_RESULT_DEPENDS, rows=dep_edges)
        # cross-chapter back-references: label lookup scoped to this book's Result ids
        cross_rows = []
        for u in unresolved:
            hits = [rec["id"] for rec in s.run(FIND_BOOK_RESULT_BY_LABEL,
                                               book_prefix=book_id + ":", label=u["label"])]
            if len(hits) == 1 and hits[0] != u["res_id"]:
                cross_rows.append({"res_id": u["res_id"], "dep_id": hits[0]})
                cross_linked += 1
            else:
                cross_skipped += 1
                context.log.info(
                    f"depends_on skipped: {u['label']!r} → {len(hits)} matches in {book_id} "
                    "(forward reference or ambiguous label)")
        s.run(WRITE_RESULT_DEPENDS, rows=cross_rows)

        with context.resources.postgres.connect() as conn:
            with conn.cursor() as cur:
                for c in concepts:
                    if c.get("embedding") is not None:
                        upsert_embedding(cur, c["name"], "Concept", c["embedding"])
                for reg in payload.get("alias_registrations", []):
                    upsert_alias(cur, "Concept", reg["key"], reg["canonical"], reg["source"])
            conn.commit()

    return MaterializeResult(metadata={
        "concepts": MetadataValue.int(len(crows)),
        "definitions": MetadataValue.int(len(drows)),
        "results": MetadataValue.int(len(rrows)),
        "defines": MetadataValue.int(len(def_edges)),
        "uses": MetadataValue.int(len(use_edges)),
        "depends_on": MetadataValue.int(len(dep_edges) + cross_linked),
        "depends_on_cross_chapter": MetadataValue.int(cross_linked),
        "skipped_refs": MetadataValue.int(sk_def + sk_use + cross_skipped),
    })
