from pipeline.assets.graph_write import result_id
from pipeline.books.labels import LabelIndex, build_label_index, parse_label, unique_label_map
from pipeline.text_norm import normalize_statement


def test_parse_label_extracts_kind_and_tag():
    assert parse_label("Lemma 9.6") == ("lemma", "9.6", "")
    assert parse_label("9.6. Lemma.") == ("lemma", "9.6", "")
    assert parse_label("Theorem 3.12. Skorokod representation of a random variable "
                       "with prescribed distribution function.") == (
        "theorem", "3.12",
        "skorokod representation of a random variable with prescribed distribution function")
    assert parse_label("5.3. MON") == (None, "5.3", "mon")
    assert parse_label("the Monotone-Convergence Theorem") == (
        "theorem", None, "the monotone convergence")


NODES = [
    {"id": "b:ch9:lemma:aaa", "name": "9.6. Lemma.", "kind": "lemma"},
    {"id": "b:ch9:theorem:bbb", "name": "9.7. Theorem. Dominated-Convergence Theorem",
     "kind": "theorem"},
    {"id": "b:ch5:theorem:ccc", "name": "5.3. MON", "kind": "theorem"},
    {"id": "b:ch5:theorem:ddd", "name": "5.3. MON", "kind": "theorem"},  # dup label
]


def test_index_resolves_by_kind_and_tag():
    idx = build_label_index(NODES)
    assert idx.resolve("Lemma 9.6") == "b:ch9:lemma:aaa"
    assert idx.resolve("Theorem 9.7") == "b:ch9:theorem:bbb"


def test_index_resolves_named_theorem_phrase():
    idx = build_label_index(NODES)
    assert idx.resolve("Dominated-Convergence Theorem") == "b:ch9:theorem:bbb"


def test_index_refuses_ambiguous():
    idx = build_label_index(NODES)
    assert idx.resolve("5.3. MON") is None      # two nodes share the tag — never guess
    assert idx.resolve("Lemma 99.9") is None    # no match


def test_unique_label_map_drops_duplicates_and_empty_names():
    rows = [
        {"id": "b:ch9:lemma:aaa", "name": "9.6. Lemma."},
        {"id": "b:ch9:theorem:bbb", "name": "9.7. Theorem. Dominated-Convergence Theorem"},
        {"id": "b:ch5:theorem:ccc", "name": "5.3. MON"},
        {"id": "b:ch5:theorem:ddd", "name": "5.3. MON"},  # dup label
        {"id": "b:ch1:lemma:eee", "name": ""},            # empty label
        {"id": "b:ch1:lemma:fff", "name": None},          # missing label
    ]
    result = unique_label_map(rows)
    assert result == {
        "9.6. Lemma.": "b:ch9:lemma:aaa",
        "9.7. Theorem. Dominated-Convergence Theorem": "b:ch9:theorem:bbb",
    }
    assert "5.3. MON" not in result


def test_result_id_is_stable_under_pre_normalized_statement():
    # PROVED_IN rows are keyed on an already-normalized statement (proof_chunks'
    # result_key); result_id must produce the same id whether given the raw or the
    # pre-normalized statement, since it normalizes internally and normalization is
    # idempotent.
    raw = "A  Statement"
    assert result_id("ch", "theorem", raw) == result_id(
        "ch", "theorem", normalize_statement(raw))
