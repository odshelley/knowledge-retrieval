"""One-off backfill: Chunk-MENTIONS->Concept for papers ingested before provenance existed.
Approximate by design: a chunk MENTIONS a concept iff the concept's name appears verbatim
(case-insensitive) in the chunk text of a paper that DISCUSSES it. Definitions/Results are
NOT backfilled (statement matching is unreliable); they gain EXTRACTED_FROM on new ingests.

WRITES TO NEO4J. Run manually while the Dagster schedule is idle (it is idempotent MERGE,
but stay within the single-writer convention). Usage: uv run python scripts/backfill_mentions.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

PAPERS = "MATCH (p:Paper) RETURN p.id AS id ORDER BY p.id"

BACKFILL_ONE = """
MATCH (p:Paper {id: $paper_id})-[:DISCUSSES]->(c:Concept)
WHERE size(c.name) >= 4
MATCH (p)-[:HAS_DOCUMENT]->(:Document)<-[:BELONGS_TO]-(ch:Chunk)
WHERE toLower(ch.text) CONTAINS toLower(c.name)
MERGE (ch)-[:MENTIONS]->(c)
RETURN count(*) AS edges
"""


def main() -> None:
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    with driver.session(database=db) as s:
        papers = [r["id"] for r in s.run(PAPERS)]
        total = 0
        for i, pid in enumerate(papers, 1):
            edges = s.run(BACKFILL_ONE, paper_id=pid).single()["edges"]
            total += edges
            print(f"[{i}/{len(papers)}] {pid}: {edges} MENTIONS")
    driver.close()
    print(f"done: {total} MENTIONS edges")


if __name__ == "__main__":
    main()
