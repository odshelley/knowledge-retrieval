"""Knowledge-graph schema for the alethograph Aura DB.

NODE_TYPES, RELATIONSHIP_TYPES, and PATTERNS define the graph shape used by the bespoke
builder pipeline. INIT_CYPHER (and iter_init_statements) set up Neo4j constraints and the
chunk vector index; consumed by scripts/init_neo4j.py and scripts/reset_graph.py.
PATTERNS documents the allowed (start_label, relationship, end_label) triples; extraction
validates extracted triples against this set.
"""
from __future__ import annotations

NODE_TYPES = [
    "Paper",
    "Book",
    "Author",
    "Concept",
    "Topic",
    "Researcher",
    "Idea",
    "Definition",
    "Result",
    "Summary",
]

# Verbatim from legacy DB. Verbs are subject-first
# (e.g. "Author AUTHORED Paper", "Paper HAS_TOPIC Topic").
RELATIONSHIP_TYPES = [
    "AUTHORED",
    "CITES",
    "HAS_TOPIC",
    "BROADER_THAN",
    "RELATED_TO",
    "BELONGS_TO",
    "DERIVED_FROM",
    "DISCUSSES",
    "COVERED_IN",
    "COVERS",
    "COVERS_TOPIC",
    "REFERENCES",
    "STUDIED_FOR",
    "STUDIES",
    "KNOWS",
    "PROPOSED",
    "USES_BOOK",
    "INVOLVES",
    "EVIDENCED_BY",
    "STATES",
    "DEFINES",
    "USES",
    "DEPENDS_ON",
    "HAS_SUMMARY",
]

# Verbatim patterns from the legacy DB (start, rel, end).
PATTERNS: list[tuple[str, str, str]] = [
    ("Author",     "AUTHORED",     "Paper"),
    ("Author",     "AUTHORED",     "Book"),
    ("Paper",      "CITES",        "Paper"),
    ("Paper",      "HAS_TOPIC",    "Topic"),
    ("Paper",      "DISCUSSES",    "Concept"),
    ("Paper",      "STUDIES",      "Topic"),
    ("Book",       "HAS_TOPIC",    "Topic"),
    ("Book",       "COVERS",       "Concept"),
    ("Book",       "COVERS_TOPIC", "Topic"),
    ("Book",       "STUDIED_FOR",  "Topic"),
    ("Book",       "REFERENCES",   "Concept"),
    ("Concept",    "DERIVED_FROM", "Paper"),
    ("Concept",    "DERIVED_FROM", "Topic"),
    ("Concept",    "RELATED_TO",   "Concept"),
    ("Concept",    "BELONGS_TO",   "Topic"),
    ("Concept",    "HAS_TOPIC",    "Topic"),
    ("Concept",    "COVERED_IN",   "Book"),
    ("Topic",      "BROADER_THAN", "Topic"),
    ("Topic",      "RELATED_TO",   "Topic"),
    ("Researcher", "KNOWS",        "Concept"),
    ("Researcher", "STUDIES",      "Paper"),
    ("Researcher", "PROPOSED",     "Idea"),
    ("Researcher", "USES_BOOK",    "Book"),
    ("Researcher", "HAS_TOPIC",    "Topic"),
    ("Idea",       "INVOLVES",     "Concept"),
    ("Idea",       "EVIDENCED_BY", "Paper"),
    ("Idea",       "STUDIES",      "Topic"),
    ("Idea",       "HAS_TOPIC",    "Topic"),
    ("Paper",      "STATES",       "Definition"),
    ("Paper",      "STATES",       "Result"),
    ("Definition", "DEFINES",      "Concept"),
    ("Result",     "USES",         "Concept"),
    ("Result",     "DEPENDS_ON",   "Result"),
    ("Paper",      "HAS_SUMMARY",  "Summary"),
]

INIT_CYPHER = """
CREATE CONSTRAINT paper_id IF NOT EXISTS
  FOR (p:Paper) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT book_id IF NOT EXISTS
  FOR (b:Book) REQUIRE b.id IS UNIQUE;

CREATE CONSTRAINT concept_name IF NOT EXISTS
  FOR (c:Concept) REQUIRE c.name IS UNIQUE;

CREATE CONSTRAINT idea_id IF NOT EXISTS
  FOR (i:Idea) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT researcher_name IF NOT EXISTS
  FOR (r:Researcher) REQUIRE r.name IS UNIQUE;

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

CREATE CONSTRAINT definition_id IF NOT EXISTS
  FOR (d:Definition) REQUIRE d.id IS UNIQUE;

CREATE CONSTRAINT result_id IF NOT EXISTS
  FOR (r:Result) REQUIRE r.id IS UNIQUE;

CREATE CONSTRAINT summary_id IF NOT EXISTS
  FOR (s:Summary) REQUIRE s.id IS UNIQUE;
"""


def iter_init_statements() -> list[str]:
    """Split INIT_CYPHER into individual statements (Aura's bolt API requires one stmt at a time)."""
    return [s.strip() for s in INIT_CYPHER.split(";") if s.strip()]
