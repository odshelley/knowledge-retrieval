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


from pipeline.assets.graph_write import def_id, result_id  # noqa: E402  (chapter-local ids)

WRITE_BOOK_CONCEPTS = """
MATCH (b:Book {id:$book_id})
UNWIND $rows AS row
  MERGE (c:Concept {name: row.name})
  SET c.tags = row.tags
  MERGE (b)-[:COVERS]->(c)
  MERGE (c)-[:COVERED_IN]->(b)
"""

WRITE_BOOK_DEFINITIONS = """
UNWIND $rows AS row
  MATCH (s:Section {id: row.section_id})
  MERGE (d:Definition {id: row.id})
  SET d.term = row.term, d.statement = row.statement, d.label = row.label,
      d.name = row.label, d.page = row.page
  MERGE (s)-[:STATES]->(d)
"""

WRITE_BOOK_RESULTS = """
UNWIND $rows AS row
  MATCH (s:Section {id: row.section_id})
  MERGE (r:Result {id: row.id})
  SET r.name = row.name, r.label = row.label, r.kind = row.kind,
      r.statement = row.statement, r.page = row.page
  MERGE (s)-[:STATES]->(r)
"""

FIND_BOOK_RESULT_BY_LABEL = """
MATCH (r:Result) WHERE r.id STARTS WITH $book_prefix AND r.name = $label
RETURN r.id AS id LIMIT 2
"""


def book_definition_rows(owner: str, section_id: str, defs: list[dict]) -> list[dict]:
    return [{"id": def_id(owner, d["statement"]), "term": d["term"],
             "statement": d["statement"], "label": d.get("name", ""),
             "page": d.get("page"), "section_id": section_id} for d in defs]


def book_result_rows(owner: str, section_id: str, results: list[dict]) -> list[dict]:
    return [{"id": result_id(owner, r["kind"], r["statement"]), "name": r.get("name", ""),
             "label": r.get("name", ""), "kind": r["kind"], "statement": r["statement"],
             "page": r.get("page"), "section_id": section_id} for r in results]


def split_depends_on(owner: str, results: list[dict],
                     name_index: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """Within-chapter DEPENDS_ON via the collision-safe name index; anything not found is
    returned as an unresolved (res_id, label) for the cross-chapter Cypher lookup."""
    resolved, unresolved = [], []
    for r in results:
        rid = result_id(owner, r["kind"], r["statement"])
        for dep_label in r.get("depends_on", []):
            dep = name_index.get(dep_label)
            if dep == rid:
                continue  # self-reference
            if dep is not None:
                resolved.append({"res_id": rid, "dep_id": dep})
            else:
                unresolved.append({"res_id": rid, "label": dep_label})
    return resolved, unresolved
