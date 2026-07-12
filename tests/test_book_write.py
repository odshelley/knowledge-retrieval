from pipeline.assets.graph_write import def_id, result_id, result_name_index
from pipeline.books.identity import notation_node_id
from pipeline.books.write import (
    WRITE_BOOK, WRITE_BOOK_CHUNKS, WRITE_BOOK_CONCEPTS,
    WRITE_BOOK_DEFINITIONS, WRITE_BOOK_DOCUMENT, WRITE_BOOK_RESULTS, WRITE_CHAPTERS,
    WRITE_SECTIONS, book_definition_rows, book_notation_rows, book_proof_rows, book_result_rows,
    chapter_rows, section_rows, split_depends_on,
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


OWNER = "isbn:9783161484100:ch01"
SEC = "isbn:9783161484100:ch01:s01"


def test_book_definition_rows_carry_label_page_section():
    rows = book_definition_rows(OWNER, SEC, [
        {"term": "Levy process", "statement": "$X$ has independent increments.",
         "name": "Definition 1.1", "page": 12, "defines": ["Levy process"]}])
    assert rows[0]["id"] == def_id(OWNER, "$X$ has independent increments.")
    assert rows[0]["id"].startswith(OWNER + ":def:")
    assert rows[0]["label"] == "Definition 1.1"
    assert rows[0]["page"] == 12 and rows[0]["section_id"] == SEC


def test_book_result_rows_ids_are_chapter_local():
    rows = book_result_rows(OWNER, SEC, [
        {"name": "Theorem 1.2", "kind": "theorem", "statement": "$x=y$", "page": 13}])
    assert rows[0]["id"] == result_id(OWNER, "theorem", "$x=y$")
    assert rows[0]["label"] == "Theorem 1.2" and rows[0]["name"] == "Theorem 1.2"


def test_split_depends_on_within_chapter_then_unresolved():
    results = [
        {"name": "Theorem 1.2", "kind": "theorem", "statement": "$a$",
         "depends_on": ["Definition 1.1", "Theorem 0.9", "Theorem 1.2"]},
        {"name": "Definition 1.1", "kind": "lemma", "statement": "$b$", "depends_on": []},
    ]
    rrows = book_result_rows(OWNER, SEC, results)
    idx = result_name_index(rrows)
    resolved, unresolved = split_depends_on(OWNER, results, idx)
    rid = result_id(OWNER, "theorem", "$a$")
    assert resolved == [{"res_id": rid, "dep_id": result_id(OWNER, "lemma", "$b$")}]
    assert unresolved == [{"res_id": rid, "label": "Theorem 0.9"}]  # cross-chapter candidate


def test_book_statement_cypher_anchors_on_section_with_label_and_page():
    d = " ".join(WRITE_BOOK_DEFINITIONS.split())
    assert "MATCH (s:Section {id: row.section_id})" in d
    assert "MERGE (s)-[:STATES]->" in d
    assert "label" in d and "page" in d
    r = " ".join(WRITE_BOOK_RESULTS.split())
    assert "MATCH (s:Section {id: row.section_id})" in r
    assert "MERGE (s)-[:STATES]->" in r


def test_book_concepts_cypher_covers_both_directions():
    c = " ".join(WRITE_BOOK_CONCEPTS.split())
    assert "MERGE (b)-[:COVERS]->(c)" in c
    assert "MERGE (c)-[:COVERED_IN]->(b)" in c


def test_notation_id_is_per_book_and_symbol_normalized():
    a = notation_node_id("title:probability with martingales", "$W_t$")
    b = notation_node_id("title:probability with martingales", "$w_T$")
    c = notation_node_id("title:another book", "$W_t$")
    assert a == b          # case/whitespace-insensitive within a book
    assert a != c          # never collides across books
    assert a.split(":not:")[0] == "title:probability with martingales"


def test_book_notation_rows_resolve_concept_via_canon_map():
    rows = book_notation_rows(
        "title:b", "sec1",
        [{"symbol_latex": "$W_t$", "meaning": "Brownian motion", "concept": "brownian motion"},
         {"symbol_latex": "a.e.", "meaning": "almost everywhere", "concept": ""}],
        {"brownian motion": "Brownian motion"})
    assert rows[0]["concept"] == "Brownian motion"
    assert rows[1]["concept"] is None
    assert rows[0]["section_id"] == "sec1"


def test_book_proof_rows_only_for_results_with_sketch():
    results = [
        {"kind": "theorem", "statement": "S1", "proof": {"sketch": "sk", "technique": "t"}},
        {"kind": "lemma", "statement": "S2", "proof": None},
    ]
    rows = book_proof_rows("ch1", "sec1", results)
    assert len(rows) == 1
    assert rows[0]["sketch"] == "sk"
    assert rows[0]["id"].endswith(":proof")
