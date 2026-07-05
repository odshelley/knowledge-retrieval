from pipeline.books.write import (
    WRITE_BOOK, WRITE_BOOK_CHUNKS, WRITE_BOOK_DOCUMENT, WRITE_CHAPTERS, WRITE_SECTIONS,
    chapter_rows, section_rows,
)

STRUCTURE = {
    "book_id": "isbn:9783161484100",
    "chapters": [
        {"id": "isbn:9783161484100:ch01", "key": "f" * 64 + ":ch01", "number": 1,
         "title": "Chapter 1", "page_start": 2, "page_end": 3,
         "sections": [{"id": "isbn:9783161484100:ch01:s01", "number": "1.1",
                       "title": "1.1 Defs", "page_start": 2, "page_end": 2}]},
        {"id": "isbn:9783161484100:ch02", "key": "f" * 64 + ":ch02", "number": 2,
         "title": "Chapter 2", "page_start": 4, "page_end": 5, "sections": []},
    ],
}


def test_chapter_rows_flatten_with_order():
    rows = chapter_rows(STRUCTURE)
    assert rows[0]["id"] == "isbn:9783161484100:ch01" and rows[0]["order"] == 1
    assert rows[1]["order"] == 2 and rows[1]["page_end"] == 5


def test_section_rows_carry_chapter_id_and_order():
    rows = section_rows(STRUCTURE)
    assert rows == [{"id": "isbn:9783161484100:ch01:s01", "chapter_id": "isbn:9783161484100:ch01",
                     "number": "1.1", "title": "1.1 Defs", "page_start": 2, "page_end": 2,
                     "order": 1}]


def test_write_book_merges_authors_and_sets_document_id():
    c = " ".join(WRITE_BOOK.split())
    assert "MERGE (b:Book {id: $id})" in c
    assert "b.document_id=$document_id" in c.replace(" =", "=").replace("= ", "=")
    assert "MERGE (a:Author {name: author})" in c
    assert "MERGE (a)-[:AUTHORED]->(b)" in c


def test_write_book_document_links_has_document():
    c = " ".join(WRITE_BOOK_DOCUMENT.split())
    assert "MERGE (d:Document {id:$doc_id})" in c
    assert "MERGE (b)-[:HAS_DOCUMENT]->(d)" in c


def test_write_chapters_and_sections_hierarchy_edges():
    ch = " ".join(WRITE_CHAPTERS.split())
    assert "MERGE (ch:Chapter {id: row.id})" in ch
    assert "MERGE (b)-[:HAS_CHAPTER {order: row.order}]->(ch)" in ch
    se = " ".join(WRITE_SECTIONS.split())
    assert "MATCH (ch:Chapter {id: row.chapter_id})" in se
    assert "MERGE (ch)-[:HAS_SECTION {order: row.order}]->(s)" in se


def test_write_book_chunks_belongs_to_and_part_of():
    c = " ".join(WRITE_BOOK_CHUNKS.split())
    assert "MERGE (c:Chunk {id: row.id})" in c
    assert "c.page_start = row.page_start" in c
    assert "MERGE (c)-[:BELONGS_TO]->(d)" in c
    assert "MATCH (s:Section {id: row.section_id})" in c
    assert "MERGE (c)-[:PART_OF]->(s)" in c
