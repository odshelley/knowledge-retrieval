"""Per-partition structural sync: copy this paper/book's legacy connections into the new DB.

Runs once per partition. Faithful to the legacy schema — same relationship type
names and directions as the legacy alethograph DB:

    (Author)-[:AUTHORED]->(Paper|Book)
    (Paper|Book)-[:HAS_TOPIC]->(Topic)
    (Paper)-[:CITES]->(Paper)

The bulk ``legacy_graph_mirror`` asset already mirrors the entire curated graph
(including these edges); this asset is the per-partition incremental version
that runs as part of the per-paper pipeline so a single newly-added paper picks
up its connections without re-running the full mirror.
"""
from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.partitions import get_partition, partitions_def

LEGACY_PAPER_QUERY = """
MATCH (p:Paper {id: $paper_id})
OPTIONAL MATCH (p)<-[:AUTHORED]-(a:Author)
OPTIONAL MATCH (p)-[:HAS_TOPIC]->(t:Topic)
OPTIONAL MATCH (p)-[:CITES]->(cited:Paper)
RETURN
  collect(DISTINCT a.name) AS authors,
  collect(DISTINCT t.name) AS topics,
  collect(DISTINCT cited.id) AS citations
"""

LEGACY_BOOK_QUERY = """
MATCH (b:Book {id: $paper_id})
OPTIONAL MATCH (b)<-[:AUTHORED]-(a:Author)
OPTIONAL MATCH (b)-[:HAS_TOPIC]->(t:Topic)
RETURN
  collect(DISTINCT a.name) AS authors,
  collect(DISTINCT t.name) AS topics,
  [] AS citations
"""


def build_overlay_payload(legacy_session, paper_id: str, kind: str) -> dict:
    query = LEGACY_BOOK_QUERY if kind == "book" else LEGACY_PAPER_QUERY
    rows = list(legacy_session.run(query, paper_id=paper_id))
    if not rows:
        return {"authors": [], "topics": [], "citations": []}
    row = rows[0]
    return {
        "authors":   list(row["authors"]) if row["authors"] else [],
        "topics":    list(row["topics"]) if row["topics"] else [],
        "citations": list(row["citations"]) if row["citations"] else [],
    }


WRITE_PAPER_OVERLAY = """
MATCH (p:Paper {id: $paper_id})

WITH p
UNWIND $authors AS author_name
MERGE (a:Author {name: author_name})
MERGE (a)-[:AUTHORED]->(p)

WITH p, $topics AS topics
UNWIND topics AS topic_name
MERGE (t:Topic {name: topic_name})
MERGE (p)-[:HAS_TOPIC]->(t)

WITH p, $citations AS cited_ids
UNWIND cited_ids AS cited_id
OPTIONAL MATCH (cited:Paper {id: cited_id})
WITH p, cited WHERE cited IS NOT NULL
MERGE (p)-[:CITES]->(cited)
"""

WRITE_BOOK_OVERLAY = """
MATCH (b:Book {id: $paper_id})

WITH b
UNWIND $authors AS author_name
MERGE (a:Author {name: author_name})
MERGE (a)-[:AUTHORED]->(b)

WITH b, $topics AS topics
UNWIND topics AS topic_name
MERGE (t:Topic {name: topic_name})
MERGE (b)-[:HAS_TOPIC]->(t)
"""


@asset(
    partitions_def=partitions_def(),
    deps=["kg_extracted"],
    required_resource_keys={"neo4j_new", "neo4j_legacy"},
)
def structural_overlay(context) -> MaterializeResult:
    paper_id = context.partition_key
    part = get_partition(paper_id)
    if part is None:
        raise ValueError(f"unknown partition: {paper_id}")
    kind = part.get("kind", "paper")

    legacy = context.resources.neo4j_legacy
    new = context.resources.neo4j_new

    with legacy.get_driver().session(database=legacy.database) as s:
        payload = build_overlay_payload(s, paper_id, kind)

    write_query = WRITE_BOOK_OVERLAY if kind == "book" else WRITE_PAPER_OVERLAY
    params = {"paper_id": paper_id, "authors": payload["authors"], "topics": payload["topics"]}
    if kind != "book":
        params["citations"] = payload["citations"]

    with new.get_driver().session(database=new.database) as s:
        s.run(write_query, **params)

    return MaterializeResult(
        metadata={
            "paper_id": paper_id,
            "kind": kind,
            "authors": MetadataValue.int(len(payload["authors"])),
            "topics": MetadataValue.json(payload["topics"]),
            "citations_present": MetadataValue.int(len(payload["citations"])),
        },
    )
