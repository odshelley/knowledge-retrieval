"""One-off backfill: generate a one-sentence description (+ embedding) for Concepts that
predate Task 5a. Grounding = the concept's definitions and up to 3 MENTIONS chunk excerpts.
Resumable: only touches WHERE c.description IS NULL. WRITES TO NEO4J — run while the
Dagster schedule is idle. Cost: one gpt-5-nano call + one embedding per concept.
Usage: uv run python scripts/backfill_concept_descriptions.py [--limit N]"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from openai import OpenAI
from pydantic import BaseModel, Field

from pipeline.embedding import embed_texts

load_dotenv()

EMBED_MODEL = "text-embedding-3-small"  # pinned corpus-wide (1536 dims)
DESCRIBE_MODEL = os.environ.get("EXTRACTION_MODEL", "gpt-5-nano")

MISSING = """
MATCH (c:Concept) WHERE c.description IS NULL
OPTIONAL MATCH (d:Definition)-[:DEFINES]->(c)
WITH c, collect(d.statement)[..3] AS defs
OPTIONAL MATCH (ch:Chunk)-[:MENTIONS]->(c)
WITH c, defs, collect(left(ch.text, 900))[..3] AS excerpts
OPTIONAL MATCH (p:Paper)-[:DISCUSSES]->(c)
RETURN c.name AS name, defs, excerpts, collect(p.title)[..5] AS papers
LIMIT $limit
"""

SET_ONE = """
MATCH (c:Concept {name: $name})
SET c.description = $description
WITH c
CALL db.create.setNodeVectorProperty(c, 'embedding', $embedding)
"""


class Description(BaseModel):
    description: str = Field(description="One sentence, at most ~40 words, saying what the "
                             "concept IS. Grounded only in the provided material; LaTeX for math.")


def describe(client, model: str, name: str, defs, excerpts, papers) -> str:
    material = "\n".join(
        ["Definitions:", *defs, "Text excerpts:", *excerpts, "Papers:", *papers])
    resp = client.chat.completions.parse(
        model=model,
        messages=[{"role": "system",
                   "content": "Write a one-sentence description of the given research concept "
                              "using ONLY the provided material."},
                  {"role": "user", "content": f"Concept: {name}\n\n{material[:12000]}"}],
        response_format=Description, timeout=60)
    return resp.choices[0].message.parsed.description


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10000)
    args = ap.parse_args()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    client = OpenAI()  # OPENAI_API_KEY from .env
    processed = 0
    skipped = 0
    with driver.session(database=db) as s:
        rows = s.run(MISSING, limit=args.limit).data()
        print(f"{len(rows)} concepts missing descriptions")
        for i, row in enumerate(rows, 1):
            try:
                desc = describe(client, DESCRIBE_MODEL, row["name"],
                                row["defs"], row["excerpts"], row["papers"])
                vec = embed_texts(client, [f"{row['name']}: {desc}"], model=EMBED_MODEL)[0]
                s.run(SET_ONE, name=row["name"], description=desc, embedding=vec)
                processed += 1
                print(f"[{i}/{len(rows)}] {row['name']}: {desc[:70]}")
            except Exception as e:
                skipped += 1
                print(f"SKIP {row['name']}: {e}")
    driver.close()
    print(f"processed={processed} skipped={skipped}")


if __name__ == "__main__":
    main()
