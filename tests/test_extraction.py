import pytest

from pipeline.extraction import (
    ExtractionResult, parse_extraction, merge_results,
    Concept, Definition, Result,
)

def test_parse_extraction_reads_concepts_with_kind():
    payload = {
        "concepts": [{"name": "Wrong-Way Risk", "kind": "concept"},
                     {"name": "Deep BSDE Solver", "kind": "method"}],
        "definitions": [{"term": "WWR", "statement": "$P(\\tau)$ ..."}],
        "results": [{"name": "Thm 1", "kind": "theorem", "statement": "$x=y$"}],
    }
    r = parse_extraction(payload)
    assert isinstance(r, ExtractionResult)
    assert ("Wrong-Way Risk", "concept") in [(c.name, c.kind) for c in r.concepts]
    assert r.results[0].kind == "theorem"

def test_parse_extraction_rejects_unknown_result_kind():
    import pytest
    with pytest.raises(ValueError):
        parse_extraction({"results": [{"name": "x", "kind": "conjecture", "statement": "y"}]})

def test_merge_results_dedupes_definitions_and_results_across_overlapping_chunks():
    p1 = ExtractionResult(
        concepts=[Concept(name="WWR", kind="concept")],
        definitions=[Definition(term="WWR", statement="Let $X$ be a martingale.")],
        results=[Result(name="Thm 1", kind="theorem", statement="$x = y$")],
    )
    p2 = ExtractionResult(
        concepts=[Concept(name="wwr", kind="concept")],  # same name, different case
        definitions=[Definition(term="WWR", statement="let   $X$   be a martingale. ")],
        results=[Result(name="Theorem 1", kind="theorem", statement="$x = y$")],
    )
    merged = merge_results([p1, p2])
    assert len(merged.concepts) == 1
    assert len(merged.definitions) == 1
    assert len(merged.results) == 1


def test_parse_extraction_reads_link_fields():
    payload = {
        "concepts": [{"name": "BSDE", "kind": "concept"}],
        "definitions": [{"term": "BSDE", "statement": "$dY=...$", "defines": ["BSDE"]}],
        "results": [{"name": "Thm 1", "kind": "theorem", "statement": "$x=y$",
                     "uses": ["BSDE"], "depends_on": ["Lemma 2.4"]}],
    }
    r = parse_extraction(payload)
    assert r.definitions[0].defines == ["BSDE"]
    assert r.results[0].uses == ["BSDE"]
    assert r.results[0].depends_on == ["Lemma 2.4"]


def test_parse_extraction_defaults_link_fields_to_empty():
    payload = {
        "concepts": [],
        "definitions": [{"term": "X", "statement": "s"}],
        "results": [{"name": "T", "kind": "lemma", "statement": "s"}],
    }
    r = parse_extraction(payload)
    assert r.definitions[0].defines == []
    assert r.results[0].uses == []
    assert r.results[0].depends_on == []


def test_merge_results_keeps_distinct_results_of_different_kind():
    p = ExtractionResult(results=[
        Result(name="A", kind="theorem", statement="$x=y$"),
        Result(name="B", kind="lemma", statement="$x=y$"),  # same text, different kind ⇒ distinct
    ])
    assert len(merge_results([p]).results) == 2


def test_merge_results_unions_link_lists_across_overlapping_chunks():
    p1 = ExtractionResult(
        definitions=[Definition(term="BSDE", statement="$s$", defines=["BSDE"])],
        results=[Result(name="T1", kind="theorem", statement="$x=y$",
                        uses=["BSDE"], depends_on=["Lemma 2.4"])],
    )
    p2 = ExtractionResult(
        definitions=[Definition(term="BSDE", statement="$s$", defines=["Backward SDE"])],
        results=[Result(name="T1", kind="theorem", statement="$x=y$",
                        uses=["Feynman-Kac"], depends_on=["Lemma 2.4"])],
    )
    merged = merge_results([p1, p2])
    assert len(merged.definitions) == 1
    assert merged.definitions[0].defines == ["BSDE", "Backward SDE"]   # unioned, order-preserved
    assert len(merged.results) == 1
    assert merged.results[0].uses == ["BSDE", "Feynman-Kac"]
    assert merged.results[0].depends_on == ["Lemma 2.4"]              # deduped, not doubled


@pytest.mark.parametrize("name", [
    "W_t", "X_t", "Π*", "ũ(x,t)", "p_σ(x̃)", r"$\Pi^*$", "∇ρ",
])
def test_is_notation_only_drops_bare_notation(name):
    from pipeline.extraction import _is_notation_only
    assert _is_notation_only(name) is True


@pytest.mark.parametrize("name", [
    "Brownian motion", "Schrödinger bridge", "Markovian projection",
    "OT", "SB", "ELBO", "SDE", "BSDE", "WWR",
    "σ-algebra", "L² space", "k-NN", "GPT-4", "2-Wasserstein distance",
])
def test_is_notation_only_keeps_real_concepts(name):
    from pipeline.extraction import _is_notation_only
    assert _is_notation_only(name) is False


def test_merge_results_drops_notation_only_concepts_keeps_real():
    part = ExtractionResult(concepts=[
        Concept(name="Brownian motion", kind="concept"),
        Concept(name="W_t", kind="concept"),              # notation -> dropped
        Concept(name="brownian motion", kind="concept"),  # case dup -> deduped
        Concept(name="Π*", kind="concept"),               # notation -> dropped
        Concept(name="OT", kind="concept"),               # acronym -> kept
    ])
    merged = merge_results([part])
    names = [c.name for c in merged.concepts]
    assert names == ["Brownian motion", "OT"]
