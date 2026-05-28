from pipeline.analysis import ANALYSIS_FIELDS, validate_analysis, strip_to_json

def test_analysis_fields_match_research_skill_template():
    assert ANALYSIS_FIELDS == [
        "summary", "key_contributions", "methodology", "key_findings",
        "important_references", "atomic_notes", "definitions", "results",
    ]

def test_validate_analysis_requires_all_fields():
    import pytest
    with pytest.raises(ValueError):
        validate_analysis({"summary": "x"})

def test_strip_to_json_extracts_outermost_object_from_prose():
    raw = 'Sure! Here is the analysis:\n```json\n{"a": 1, "b": [2, 3]}\n```\nHope that helps.'
    assert strip_to_json(raw) == '{"a": 1, "b": [2, 3]}'

def test_strip_to_json_passes_through_clean_json():
    assert strip_to_json('{"a": 1}') == '{"a": 1}'
