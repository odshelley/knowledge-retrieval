import pytest

from pipeline.extraction.extraction import (
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
    "x=y", "a+b", "p/q", "x<y", "f>g",   # ASCII operators are math signals too
])
def test_is_notation_only_drops_bare_notation(name):
    from pipeline.extraction.extraction import _is_notation_only
    assert _is_notation_only(name) is True


@pytest.mark.parametrize("name", [
    "Brownian motion", "Schrödinger bridge", "Markovian projection",
    "OT", "SB", "ELBO", "SDE", "BSDE", "WWR",
    "σ-algebra", "L² space", "k-NN", "GPT-4", "2-Wasserstein distance",
])
def test_is_notation_only_keeps_real_concepts(name):
    from pipeline.extraction.extraction import _is_notation_only
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


def test_statement_field_descriptions_require_latex():
    for model in (Definition, Result):
        desc = model.model_fields["statement"].description
        assert "$" in desc
        assert "LaTeX" in desc


def test_concept_name_description_forbids_bare_notation():
    desc = Concept.model_fields["name"].description
    assert "notation" in desc.lower()


def test_system_prompt_states_both_rules():
    from pipeline.extraction.extraction import SYSTEM_PROMPT
    assert "LaTeX" in SYSTEM_PROMPT
    assert "Brownian motion" in SYSTEM_PROMPT  # the concept-vs-notation few-shot


from pipeline.extraction.extraction import (
    ExtractionResult, Notation, ProofSketch, parse_extraction,
)


def test_v1_payload_still_validates():
    er = parse_extraction({
        "concepts": [{"name": "Brownian motion", "kind": "concept"}],
        "definitions": [{"term": "martingale", "statement": "A process such that..."}],
        "results": [{"kind": "theorem", "statement": "Every $L^2$ martingale converges."}],
    })
    assert er.notations == []
    assert er.results[0].proof is None
    assert er.results[0].proof_present is False
    assert er.results[0].statement_complete is True
    assert er.definitions[0].uses == []


def test_notation_and_proof_roundtrip():
    er = ExtractionResult(
        notations=[Notation(symbol_latex="$W_t$", meaning="standard Brownian motion",
                            concept="Brownian motion")],
        results=[{
            "kind": "theorem", "name": "9.7. Theorem.",
            "statement": "If $X_n \\to X$ a.s. and $|X_n| \\le Y$...",
            "proof": {"sketch": "Apply Fatou to $Y \\pm X_n$.", "technique": "Fatou's lemma"},
            "proof_present": True, "statement_complete": True,
        }],
    )
    dumped = er.model_dump()
    assert dumped["notations"][0]["symbol_latex"] == "$W_t$"
    back = ExtractionResult.model_validate(dumped)
    assert isinstance(back.results[0].proof, ProofSketch)
    assert back.results[0].proof.technique == "Fatou's lemma"


def test_notation_symbol_stripped():
    n = Notation(symbol_latex="  $\\mu$ ", meaning="a measure")
    assert n.symbol_latex == "$\\mu$"


def _er(**kw):
    return ExtractionResult(**kw)


def test_merge_prefers_complete_statement_on_same_label():
    truncated = _er(results=[{"kind": "lemma", "name": "3.4. Composition Lemma.",
                              "statement": "Composition Lemma.",
                              "statement_complete": False, "depends_on": ["Lemma 3.3"]}])
    full = _er(results=[{"kind": "lemma", "name": "3.4. Composition Lemma.",
                         "statement": "If $f$ is measurable and $g$ is Borel, then "
                                      "$g \\circ f$ is measurable.",
                         "statement_complete": True,
                         "proof": {"sketch": "Preimages compose.", "technique": ""}}])
    merged = merge_results([truncated, full])
    assert len(merged.results) == 1
    r = merged.results[0]
    assert r.statement_complete is True
    assert "Borel" in r.statement
    assert r.depends_on == ["Lemma 3.3"]      # carried from the discarded variant
    assert r.proof is not None                # carried from the kept variant


def test_merge_keeps_distinct_unlabeled_results():
    a = _er(results=[{"kind": "theorem", "statement": "Statement one."}])
    b = _er(results=[{"kind": "theorem", "statement": "Statement two."}])
    assert len(merge_results([a, b]).results) == 2


def test_merge_dedups_notations_case_insensitive():
    a = _er(notations=[{"symbol_latex": "$W_t$", "meaning": "Brownian motion"}])
    b = _er(notations=[{"symbol_latex": "$w_T$", "meaning": "Brownian motion",
                        "concept": "Brownian motion"}])
    merged = merge_results([a, b])
    assert len(merged.notations) == 1
    assert merged.notations[0].concept == "Brownian motion"  # non-empty concept adopted
