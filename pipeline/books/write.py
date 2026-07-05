"""All book-pipeline Cypher + row builders. Structure writes here; statement (Definition/
Result/Concept) writes appended for book_chapter_graph_write. Everything MERGE/idempotent."""
from __future__ import annotations

WRITE_BOOK = """
MERGE (b:Book {id: $id})
SET b.title=$title, b.year=$year, b.edition=$edition, b.publisher=$publisher,
    b.isbn=$isbn, b.document_id=$document_id
WITH b
UNWIND $authors AS author
  MERGE (a:Author {name: author})
  MERGE (a)-[:AUTHORED]->(b)
"""

WRITE_BOOK_DOCUMENT = """
MATCH (b:Book {id: $id})
MERGE (d:Document {id:$doc_id}) SET d.book_id = $id
MERGE (b)-[:HAS_DOCUMENT]->(d)
"""

WRITE_CHAPTERS = """
MATCH (b:Book {id: $id})
UNWIND $rows AS row
  MERGE (ch:Chapter {id: row.id})
  SET ch.number = row.number, ch.title = row.title,
      ch.page_start = row.page_start, ch.page_end = row.page_end
  MERGE (b)-[:HAS_CHAPTER {order: row.order}]->(ch)
"""

WRITE_SECTIONS = """
UNWIND $rows AS row
  MATCH (ch:Chapter {id: row.chapter_id})
  MERGE (s:Section {id: row.id})
  SET s.number = row.number, s.title = row.title,
      s.page_start = row.page_start, s.page_end = row.page_end
  MERGE (ch)-[:HAS_SECTION {order: row.order}]->(s)
"""

WRITE_BOOK_CHUNKS = """
MATCH (d:Document {id: $doc_id})
UNWIND $rows AS row
  MATCH (s:Section {id: row.section_id})
  MERGE (c:Chunk {id: row.id})
  SET c.text = row.text, c.position = row.position, c.embedding = row.embedding,
      c.page_start = row.page_start, c.page_end = row.page_end
  MERGE (c)-[:BELONGS_TO]->(d)
  MERGE (c)-[:PART_OF]->(s)
"""


def chapter_rows(structure: dict) -> list[dict]:
    return [{"id": ch["id"], "number": ch["number"], "title": ch["title"],
             "page_start": ch["page_start"], "page_end": ch["page_end"],
             "order": ch["number"]} for ch in structure["chapters"]]


def section_rows(structure: dict) -> list[dict]:
    rows = []
    for ch in structure["chapters"]:
        for i, s in enumerate(ch["sections"], start=1):
            rows.append({"id": s["id"], "chapter_id": ch["id"], "number": s["number"],
                         "title": s["title"], "page_start": s["page_start"],
                         "page_end": s["page_end"], "order": i})
    return rows
