from pipeline.analysis.analysis import ANALYSIS_FIELDS, PaperAnalysis, validate_analysis


def test_analysis_fields_match_research_skill_template():
    assert ANALYSIS_FIELDS == [
        "summary", "key_contributions", "methodology", "key_findings",
        "important_references", "atomic_notes", "definitions", "results",
    ]


def test_validate_analysis_requires_all_fields():
    import pytest
    with pytest.raises(ValueError):
        validate_analysis({"summary": "x"})


def _full_payload() -> dict:
    return {
        "summary": "A paper.",
        "key_contributions": ["c1"],
        "methodology": "method",
        "key_findings": ["f1"],
        "important_references": ["[1]"],
        "atomic_notes": ["note"],
        "definitions": [{"term": "WWR", "statement": "$P(\\tau)$"}],
        "results": [{"name": "Thm 1", "statement": "$x=y$"}],
    }


def test_validate_analysis_returns_canonical_dict():
    out = validate_analysis(_full_payload())
    assert set(out) == set(ANALYSIS_FIELDS)
    assert out["definitions"][0] == {"term": "WWR", "statement": "$P(\\tau)$"}
    assert out["results"][0] == {"name": "Thm 1", "statement": "$x=y$"}


def test_validate_analysis_rejects_wrong_field_type():
    import pytest
    bad = _full_payload() | {"key_contributions": "not a list"}
    with pytest.raises(ValueError):
        validate_analysis(bad)


def test_paper_analysis_result_name_defaults_to_empty():
    payload = _full_payload()
    payload["results"] = [{"statement": "$x=y$"}]  # no label
    out = PaperAnalysis.model_validate(payload)
    assert out.results[0].name == ""
