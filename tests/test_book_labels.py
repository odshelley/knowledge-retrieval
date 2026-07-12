from pipeline.books.labels import LabelIndex, build_label_index, parse_label


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
