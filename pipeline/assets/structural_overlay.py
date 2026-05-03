from __future__ import annotations

from dagster import AssetIn, MaterializeResult, MetadataValue, asset

from pipeline.partitions import partitions_def

LEGACY_QUERY = """
MATCH (p:Paper {id: $paper_id})
OPTIONAL MATCH (p)-[:AUTHORED]-(a:Author)
OPTIONAL MATCH (p)-[:HAS_TOPIC]->(t:Topic)
OPTIONAL MATCH (p)-[:CITES]->(cited:Paper)
RETURN
  collect(DISTINCT a.name) AS authors,
  collect(DISTINCT t.name) AS topics,
  collect(DISTINCT cited.id) AS citations
"""


def build_overlay_payload(legacy_session, paper_id: str) -> dict:
    rows = list(legacy_session.run(LEGACY_QUERY, paper_id=paper_id))
    if not rows:
        return {"authors": [], "topics": [], "citations": []}
    row = rows[0]
    return {
        "authors": list(row["authors"]) if row["authors"] else [],
        "topics": list(row["topics"]) if row["topics"] else [],
        "citations": list(row["citations"]) if row["citations"] else [],
    }


WRITE_OVERLAY = """
MATCH (p:Paper {id: $paper_id})

WITH p
UNWIND $authors AS author_name
MERGE (a:Author {name: author_name})
MERGE (p)-[:AUTHORED_BY]->(a)

WITH p, $topics AS topics
UNWIND topics AS topic_name
MERGE (t:Topic {name: topic_name})
MERGE (p)-[:IN_TOPIC]->(t)

WITH p, $citations AS cited_ids
UNWIND cited_ids AS cited_id
OPTIONAL MATCH (cited:Paper {id: cited_id})
WITH p, cited WHERE cited IS NOT NULL
MERGE (p)-[:CITES]->(cited)
"""


@asset(
    partitions_def=partitions_def(),
    ins={"kg_extracted": AssetIn()},
    required_resource_keys={"neo4j_new", "neo4j_legacy"},
)
def structural_overlay(context, kg_extracted) -> MaterializeResult:
    paper_id = context.partition_key
    legacy = context.resources.neo4j_legacy
    new = context.resources.neo4j_new

    with legacy.get_driver().session(database=legacy.database) as s:
        payload = build_overlay_payload(s, paper_id)

    with new.get_driver().session(database=new.database) as s:
        s.run(
            WRITE_OVERLAY,
            paper_id=paper_id,
            authors=payload["authors"],
            topics=payload["topics"],
            citations=payload["citations"],
        )

    return MaterializeResult(
        metadata={
            "paper_id": paper_id,
            "authors": MetadataValue.int(len(payload["authors"])),
            "topics": MetadataValue.json(payload["topics"]),
            "citations_present": MetadataValue.int(len(payload["citations"])),
        },
    )
