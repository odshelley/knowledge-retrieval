from pipeline.assets.graph_write import (
    concept_rows, definition_rows, result_rows, normalize_statement, def_id, result_id,
    defines_edge_rows, uses_edge_rows, depends_on_edge_rows, result_name_index,
    WRITE_CHUNKS,
)


def test_write_chunks_links_paper_to_document():
    # Regression: the Paper must be joined to its Document by an EDGE, not just the
    # d.paper_id property — otherwise Paper->Document->Chunk is untraversable and every
    # Paper reports zero chunks. Guarded so a missing Paper never drops chunk writes.
    cypher = " ".join(WRITE_CHUNKS.split())
    assert "OPTIONAL MATCH (p:Paper {id:$paper_id})" in cypher
    assert "MERGE (p)-[:HAS_DOCUMENT]->(d)" in cypher

def test_concept_rows_carry_kind_tag():
    rows = concept_rows([{"name": "WWR", "kind": "method", "action": "create", "embedding": [0.1]}])
    assert rows[0]["name"] == "WWR" and rows[0]["tags"] == ["method"]

def test_normalize_statement_collapses_and_lowercases():
    assert normalize_statement("  Let  $X$\n be ") == "let $x$ be"

def test_def_id_is_deterministic_and_paper_local():
    a = def_id("paper1", "Let $X$ be a martingale.")
    b = def_id("paper1", "let   $x$   be a martingale. ")
    assert a == b and a.startswith("paper1:def:")

def test_result_rows_use_kind_in_id():
    rows = result_rows("p1", [{"name": "Thm 1", "kind": "theorem", "statement": "$x=y$"}])
    assert rows[0]["id"].startswith("p1:theorem:")


def test_definition_rows_id_is_deterministic_and_paper_local():
    rows = definition_rows("p1", [{"term": "WWR", "statement": "Let $X$ be a martingale."}])
    assert rows[0]["id"] == def_id("p1", "Let $X$ be a martingale.")
    assert rows[0]["id"].startswith("p1:def:")
    assert rows[0]["term"] == "WWR"


# NOTE: The graph_write asset body's CITES backfill (forward path via MERGE_CITES) is covered
# by tests/integration/test_end_to_end.py::test_citation_backfill_b_then_a. Mocking the
# nested neo4j driver/session context-managers + postgres connect/cursor within a single asset
# body is too brittle to maintain here; the integration test provides that coverage.


# Keys are LOWERCASED surface names — concepts are deduped case-insensitively upstream,
# so link resolution must match case-insensitively too.
_SURFACE_TO_CANON = {"bsde": "Backward SDE", "feynman-kac": "Nonlinear Feynman-Kac"}


def test_defines_edge_rows_is_case_insensitive_and_skips_unknown():
    # "BSDE" (upper) must resolve against the lowercased "bsde" key; "Ghost Concept" is unknown.
    defs = [{"term": "BSDE", "statement": "$s$", "defines": ["BSDE", "Ghost Concept"]}]
    rows, skipped = defines_edge_rows("p1", defs, _SURFACE_TO_CANON)
    assert rows == [{"def_id": def_id("p1", "$s$"), "canonical": "Backward SDE"}]
    assert skipped == 1


def test_uses_edge_rows_is_case_insensitive_and_skips_unknown():
    results = [{"name": "T1", "kind": "theorem", "statement": "$x=y$",
                "uses": ["BSDE", "Feynman-Kac", "Nope"]}]
    rows, skipped = uses_edge_rows("p1", results, _SURFACE_TO_CANON)
    rid = result_id("p1", "theorem", "$x=y$")
    assert rows == [{"res_id": rid, "canonical": "Backward SDE"},
                    {"res_id": rid, "canonical": "Nonlinear Feynman-Kac"}]
    assert skipped == 1


def test_result_name_index_drops_empty_and_ambiguous_labels():
    rrows = [
        {"name": "Theorem 1", "id": "p1:theorem:aaa"},
        {"name": "Theorem 1", "id": "p1:theorem:bbb"},   # duplicate label → both dropped
        {"name": "Lemma 2.4", "id": "p1:lemma:ccc"},
        {"name": "", "id": "p1:theorem:ddd"},            # empty label → dropped
    ]
    assert result_name_index(rrows) == {"Lemma 2.4": "p1:lemma:ccc"}


def test_depends_on_edge_rows_maps_names_and_skips_self_and_unknown():
    results = [
        {"name": "Theorem 1", "kind": "theorem", "statement": "$a$",
         "depends_on": ["Lemma 2.4", "Theorem 1", "Missing"]},
        {"name": "Lemma 2.4", "kind": "lemma", "statement": "$b$", "depends_on": []},
    ]
    # Build the map exactly as the asset does (collision-safe), so the test proves real behavior.
    name_to_id = result_name_index(
        [{"name": r["name"], "id": result_id("p1", r["kind"], r["statement"])} for r in results]
    )
    rows, skipped = depends_on_edge_rows("p1", results, name_to_id)
    assert rows == [{"res_id": result_id("p1", "theorem", "$a$"),
                     "dep_id": result_id("p1", "lemma", "$b$")}]
    assert skipped == 2   # self-reference "Theorem 1" + unknown "Missing"


def test_mention_rows_maps_surface_to_canonical():
    from pipeline.assets.graph_write import mention_rows
    prov = {"concepts": {"bm": ["d:0", "d:2"], "unknown thing": ["d:1"]}}
    rows, skipped = mention_rows(prov, {"bm": "Brownian motion"})
    assert rows == [{"chunk_id": "d:0", "canonical": "Brownian motion"},
                    {"chunk_id": "d:2", "canonical": "Brownian motion"}]
    assert skipped == 1


def test_extracted_from_rows_recomputes_ids():
    from pipeline.assets.graph_write import def_id, extracted_from_rows, result_id
    defs = [{"term": "BM", "statement": "A process with...", "defines": []}]
    results = [{"kind": "theorem", "statement": "Every martingale...", "name": "", "uses": [], "depends_on": []}]
    from pipeline.text_norm import normalize_statement
    prov = {"definitions": {normalize_statement("A process with..."): ["d:0"]},
            "results": {"theorem|" + normalize_statement("Every martingale..."): ["d:3"]}}
    drows, rrows = extracted_from_rows("paper1", defs, results, prov)
    assert drows == [{"node_id": def_id("paper1", "A process with..."), "chunk_id": "d:0"}]
    assert rrows == [{"node_id": result_id("paper1", "theorem", "Every martingale..."), "chunk_id": "d:3"}]


def test_concept_rows_carry_description():
    from pipeline.assets.graph_write import concept_rows
    rows = concept_rows([{"name": "Rectified flow", "kind": "method",
                          "description": "Straightens transport paths."}])
    assert rows == [{"name": "Rectified flow", "tags": ["method"],
                     "description": "Straightens transport paths."}]
