from pipeline.assets.graph_write import (
    concept_rows, definition_rows, result_rows, normalize_statement, def_id,
)

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
