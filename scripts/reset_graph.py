"""Wipe the new Neo4j DB and re-assert schema. Requires a manual Aura snapshot first."""
from __future__ import annotations

import os
import sys

from neo4j import GraphDatabase

from pipeline.cypher import batched_detach_delete
from pipeline.schema import iter_init_statements


def main() -> None:
    if "--yes" not in sys.argv:
        print("Refusing to wipe without --yes. Take an Aura snapshot first.")
        sys.exit(1)
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]),
    )
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    with driver.session(database=db) as s:
        before = s.run("MATCH (n) RETURN count(n) AS n").single()["n"]
        print(f"deleting {before} nodes...")
        # IN TRANSACTIONS must be auto-committed: use a top-level run, not execute_write.
        s.run(batched_detach_delete())
        after = s.run("MATCH (n) RETURN count(n) AS n").single()["n"]
        print(f"node count now: {after}")
        for stmt in iter_init_statements():
            s.run(stmt)
        print(f"re-applied {len(iter_init_statements())} schema statements to {db}")


if __name__ == "__main__":
    main()
