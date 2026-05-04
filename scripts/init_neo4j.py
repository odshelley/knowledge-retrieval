"""Apply schema constraints + vector index to the new Aura DB.

Idempotent — safe to re-run. Run once after Task 4, and again after any schema change.

Usage:
    uv run python scripts/init_neo4j.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.schema import iter_init_statements

load_dotenv()


def main() -> None:
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]),
    )
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    with driver.session(database=db) as s:
        for stmt in iter_init_statements():
            print(f"executing: {stmt[:60]}...")
            s.run(stmt)
    print(f"applied {len(iter_init_statements())} schema statements to {db}")


if __name__ == "__main__":
    main()
