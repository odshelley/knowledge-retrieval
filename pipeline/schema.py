"""Knowledge-graph schema for the new Aura DB.

Used by:
- SimpleKGPipeline (NODE_TYPES, RELATIONSHIP_TYPES, PATTERNS)
- scripts/init_neo4j.py (INIT_CYPHER) to apply constraints + vector index

Topic nodes are seeded by structural_overlay (not LLM extraction) — see spec §6.
"""
from __future__ import annotations

NODE_TYPES = [
    "Paper",
    "Author",
    "Concept",
    "Method",
    "Theorem",
    "Definition",
    "Topic",
]

RELATIONSHIP_TYPES = [
    "AUTHORED_BY",
    "CITES",
    "INTRODUCES",
    "USES",
    "BUILDS_ON",
    "IN_TOPIC",
]

PATTERNS: list[tuple[str, str, str]] = [
    ("Paper", "AUTHORED_BY", "Author"),
    ("Paper", "CITES", "Paper"),
    ("Paper", "INTRODUCES", "Concept"),
    ("Paper", "INTRODUCES", "Method"),
    ("Paper", "INTRODUCES", "Theorem"),
    ("Paper", "INTRODUCES", "Definition"),
    ("Paper", "USES", "Method"),
    ("Paper", "USES", "Concept"),
    ("Method", "BUILDS_ON", "Method"),
    ("Concept", "BUILDS_ON", "Concept"),
    ("Paper", "IN_TOPIC", "Topic"),
]

INIT_CYPHER = """
CREATE CONSTRAINT paper_id IF NOT EXISTS
  FOR (p:Paper) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT chunk_id IF NOT EXISTS
  FOR (c:Chunk) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT document_id IF NOT EXISTS
  FOR (d:Document) REQUIRE d.id IS UNIQUE;

CREATE CONSTRAINT author_name IF NOT EXISTS
  FOR (a:Author) REQUIRE a.name IS UNIQUE;

CREATE CONSTRAINT topic_name IF NOT EXISTS
  FOR (t:Topic) REQUIRE t.name IS UNIQUE;

CREATE INDEX paper_arxiv IF NOT EXISTS
  FOR (p:Paper) ON (p.arxiv_id);

CREATE INDEX paper_doi IF NOT EXISTS
  FOR (p:Paper) ON (p.doi);

CREATE VECTOR INDEX chunk_embedding IF NOT EXISTS
  FOR (c:Chunk) ON c.embedding
  OPTIONS {
    indexConfig: {
      `vector.dimensions`: 1536,
      `vector.similarity_function`: 'cosine'
    }
  };
"""


def iter_init_statements() -> list[str]:
    """Split INIT_CYPHER into individual statements (Aura's bolt API requires one stmt at a time)."""
    return [s.strip() for s in INIT_CYPHER.split(";") if s.strip()]
