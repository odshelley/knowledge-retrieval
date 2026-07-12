"""Wipe one book's entire subtree from the Aura graph. Scoped deletes only:
- the Book node + Document, Chapters, Sections, Chunks (subtree)
- Definitions / Results / Proofs / Notations whose id carries the book prefix
- Concepts left with NO remaining relationships (shared concepts survive)

Usage:
    uv run python scripts/wipe_book.py --book-id "title:probability with martingales" --dry-run
    uv run python scripts/wipe_book.py --book-id "title:probability with martingales"
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

DELETE_SUBTREE = """
MATCH (b:Book {id: $book_id})
OPTIONAL MATCH (b)-[:HAS_CHAPTER]->(ch:Chapter)
OPTIONAL MATCH (ch)-[:HAS_SECTION]->(s:Section)
OPTIONAL MATCH (c:Chunk)-[:PART_OF]->(s)
OPTIONAL MATCH (b)-[:HAS_DOCUMENT]->(d:Document)
DETACH DELETE c, s, ch, d, b
"""

DELETE_SCOPED_STATEMENTS = """
MATCH (n)
WHERE (n:Definition OR n:Result OR n:Proof OR n:Notation)
  AND n.id STARTS WITH $prefix
DETACH DELETE n
"""

DELETE_ORPHAN_CONCEPTS = """
MATCH (c:Concept) WHERE NOT (c)--() DELETE c
"""

COUNT_SUBTREE = """
MATCH (b:Book {id: $book_id})
OPTIONAL MATCH (b)-[:HAS_CHAPTER]->(ch:Chapter)
OPTIONAL MATCH (ch)-[:HAS_SECTION]->(s:Section)
OPTIONAL MATCH (c:Chunk)-[:PART_OF]->(s)
RETURN count(DISTINCT b) AS books, count(DISTINCT ch) AS chapters,
       count(DISTINCT s) AS sections, count(DISTINCT c) AS chunks
"""

COUNT_SCOPED_STATEMENTS = """
MATCH (n)
WHERE (n:Definition OR n:Result OR n:Proof OR n:Notation)
  AND n.id STARTS WITH $prefix
RETURN labels(n)[0] AS label, count(n) AS n ORDER BY n DESC
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--book-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    prefix = args.book_id + ":"
    with driver.session(database=db) as s:
        counts = s.run(COUNT_SUBTREE, book_id=args.book_id).single()
        scoped = s.run(COUNT_SCOPED_STATEMENTS, prefix=prefix).data()
        print(f"subtree: {dict(counts)}")
        scoped_summary = ", ".join(f"{row['label']}={row['n']}" for row in scoped) or "none"
        print(f"scoped statements under {prefix!r}: {scoped_summary}")
        if args.dry_run:
            print("dry-run: nothing deleted")
            return
        if counts["books"] == 0:
            print("book not found — nothing to do")
            return
        s.run(DELETE_SCOPED_STATEMENTS, prefix=prefix)
        s.run(DELETE_SUBTREE, book_id=args.book_id)
        orphans = s.run(DELETE_ORPHAN_CONCEPTS).consume().counters.nodes_deleted
        print(f"deleted book subtree + scoped statements; {orphans} orphan concepts removed")
    driver.close()


if __name__ == "__main__":
    main()
