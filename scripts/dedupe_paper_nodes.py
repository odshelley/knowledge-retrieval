"""One-off repair: collapse Paper nodes that share a document_id.

Two documents in the corpus have two Paper nodes each, from pre-existing ingest bugs:

  f59d9ba8...  arxiv:1206.2459  vs  title:renyi divergence and kullback-leibler divergence
      The same PDF was ingested twice; the frontmatter LLM extracted the arXiv id on one
      pass and missed it on the other, so compute_paper_id produced a different id each time.

  b78eb3ec...  arxiv:2305.11089  vs  arxiv:arxiv:2305.11089
      A raw "arXiv:2305.11089v1" string reached compute_paper_id without its prefix or
      version stripped, yielding a double-prefixed id. scripts/backfill_citations.py's
      _arxiv_of() already documents this exact corruption.

Why it matters now: pipeline/zotero/push.py's PAPER_FOR_PUSH matches on document_id and the
callers use .single(), which RAISES on more than one row. Both documents would crash the
Zotero push and its backfill.

SURVIVOR CHOICE IS BY CONTENT, NOT BY ID SHAPE. In the first pair the node with the *worse*
id holds everything real (a Document, a Summary, 39 Definitions, 102 Results, 99 Concepts)
while the node with the tidy arxiv: id is nearly empty. Deleting "the ugly one" would have
destroyed 141 extracted statements. So: the richest node survives, and a node is only ever
deleted if it has NO Document, Summary, Definition or Result of its own.

Paper.id is deliberately NOT rewritten. Definition.id, Result.id and Summary.id are all
derived from paper_id (see pipeline/assets/graph_write.py and paper_analysis.py), so
relabelling a surviving node would strand every id derived from it — the same hazard that
got the paper-id migration dropped from the Zotero plan after review. Only the arxiv_id
*property* is repaired, which no other id derives from.

Dry-run by default. Run: uv run python scripts/dedupe_paper_nodes.py [--apply]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.graph.research_port import strip_arxiv_version

DUPLICATE_GROUPS = """
MATCH (p:Paper)
WITH p.document_id AS doc, count(*) AS n
WHERE n > 1 AND doc IS NOT NULL
RETURN doc ORDER BY doc
"""

NODES_IN_GROUP = """
MATCH (p:Paper {document_id: $doc})
OPTIONAL MATCH (p)-[:HAS_DOCUMENT]->(d)
OPTIONAL MATCH (p)-[:HAS_SUMMARY]->(sm)
OPTIONAL MATCH (p)-[:STATES]->(st)
OPTIONAL MATCH (p)-[:DISCUSSES]->(c)
RETURN p.id AS id, p.arxiv_id AS arxiv_id, p.doi AS doi, p.title AS title,
       count(DISTINCT d) AS docs, count(DISTINCT sm) AS summaries,
       count(DISTINCT st) AS statements, count(DISTINCT c) AS concepts
ORDER BY id
"""

# Relationships the loser may hold that the survivor needs. Authors are already shared in
# practice, but MERGE makes re-pointing them idempotent either way.
MIGRATE_CITES_IN = """
MATCH (c:Paper)-[r:CITES]->(loser:Paper {id: $loser})
MATCH (keep:Paper {id: $keep})
WHERE c.id <> $keep
MERGE (c)-[:CITES]->(keep)
DELETE r
RETURN count(*) AS n
"""

MIGRATE_CITES_OUT = """
MATCH (loser:Paper {id: $loser})-[r:CITES]->(t:Paper)
MATCH (keep:Paper {id: $keep})
WHERE t.id <> $keep
MERGE (keep)-[:CITES]->(t)
DELETE r
RETURN count(*) AS n
"""

SET_ARXIV = "MATCH (p:Paper {id:$id}) SET p.arxiv_id = $arxiv_id"
DELETE_LOSER = "MATCH (p:Paper {id:$id}) DETACH DELETE p"


def richness(row: dict) -> tuple:
    """Sort key: the node carrying real extracted content wins, id shape is irrelevant."""
    return (row["docs"], row["summaries"], row["statements"], row["concepts"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    args = ap.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    database = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")

    collapsed = refused = 0
    with driver, driver.session(database=database) as s:
        docs = [r["doc"] for r in s.run(DUPLICATE_GROUPS)]
        print(f"{len(docs)} document_id(s) with more than one Paper node\n")

        for doc in docs:
            rows = [dict(r) for r in s.run(NODES_IN_GROUP, doc=doc)]
            rows.sort(key=richness, reverse=True)
            keep, losers = rows[0], rows[1:]

            print(f"document_id {doc[:16]}...")
            for r in rows:
                tag = "KEEP  " if r is keep else "DELETE"
                print(f"  {tag} {r['id']}")
                print(f"         docs={r['docs']} summaries={r['summaries']} "
                      f"statements={r['statements']} concepts={r['concepts']} "
                      f"arxiv_id={r['arxiv_id']!r}")

            # Safety invariant: never delete a node that carries real content.
            unsafe = [r for r in losers
                      if r["docs"] or r["summaries"] or r["statements"] or r["concepts"]]
            if unsafe:
                refused += 1
                print("  REFUSED: a node marked for deletion carries content of its own; "
                      "these must be merged by hand.\n")
                continue

            # Recover a usable arxiv_id from any node in the group if the survivor lacks one.
            if not keep["arxiv_id"]:
                donor = next((r["arxiv_id"] for r in rows if r["arxiv_id"]), None)
                if donor:
                    clean = strip_arxiv_version(donor.split(":")[-1].strip())
                    print(f"  set arxiv_id on survivor: {clean!r} (from {donor!r})")
                    if args.apply:
                        s.run(SET_ARXIV, id=keep["id"], arxiv_id=clean)

            for loser in losers:
                if args.apply:
                    a = s.run(MIGRATE_CITES_IN, loser=loser["id"],
                              keep=keep["id"]).single()["n"]
                    b = s.run(MIGRATE_CITES_OUT, loser=loser["id"],
                              keep=keep["id"]).single()["n"]
                    if a or b:
                        print(f"  migrated CITES: {a} incoming, {b} outgoing")
                    s.run(DELETE_LOSER, id=loser["id"])
                else:
                    a = s.run("MATCH (c:Paper)-[:CITES]->(p:Paper {id:$id}) "
                              "RETURN count(c) AS n", id=loser["id"]).single()["n"]
                    b = s.run("MATCH (p:Paper {id:$id})-[:CITES]->(t) "
                              "RETURN count(t) AS n", id=loser["id"]).single()["n"]
                    if a or b:
                        print(f"  would migrate CITES: {a} incoming, {b} outgoing")
            collapsed += 1
            print()

    verb = "collapsed" if args.apply else "would collapse"
    print(f"{verb}: {collapsed}   refused (need manual merge): {refused}")
    return 1 if refused else 0


if __name__ == "__main__":
    raise SystemExit(main())
