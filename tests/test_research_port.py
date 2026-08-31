from unittest.mock import MagicMock, patch
from pipeline.graph import research_port as rp
from pipeline.graph.research_port import (
    compute_paper_id, strip_arxiv_version, lookup_by_arxiv, references, top_reference_records,
)

def test_compute_paper_id_prefers_doi():
    assert compute_paper_id("10.1/AbC", "2401.1v2", "Title") == "doi:10.1/abc"

def test_compute_paper_id_strips_arxiv_version_when_no_doi():
    assert compute_paper_id(None, "2401.12345v2", "Title") == "arxiv:2401.12345"

def test_compute_paper_id_falls_back_to_normalized_title():
    assert compute_paper_id(None, None, "  Deep   BSDE ") == "title:deep bsde"

def test_strip_arxiv_version():
    assert strip_arxiv_version("2401.12345v3") == "2401.12345"

@patch("pipeline.graph.research_port.requests.get")
def test_lookup_by_arxiv_maps_fields(mock_get):
    mock_get.return_value = MagicMock(status_code=200, json=lambda: {
        "paperId": "abc", "title": "T", "abstract": "A", "year": 2020,
        "citationCount": 5, "influentialCitationCount": 2,
        "tldr": {"text": "tl;dr"}, "authors": [{"name": "X", "authorId": "1"}],
    })
    p = lookup_by_arxiv("2001.00001")
    assert p["s2_id"] == "abc" and p["tldr"] == "tl;dr" and p["authors"][0]["name"] == "X"

@patch("pipeline.graph.research_port.requests.get")
def test_references_returns_empty_list_when_s2_data_is_null(mock_get):
    # Regression: S2 can answer 200 with {"data": null}; .get("data", []) then returns
    # None, which crashed triage with "TypeError: 'NoneType' object is not iterable".
    mock_get.return_value = MagicMock(status_code=200, json=lambda: {"data": None})
    assert references("abc") == []


def test_top_reference_records_tolerates_none():
    # Defense in depth for the same incident: never iterate None.
    assert top_reference_records(None, limit=3) == []


def test_top_reference_records_sorts_by_influential():
    raw = [
        {"citedPaper": {"paperId": "a", "title": "A", "externalIds": {"DOI": "d1"},
                        "influentialCitationCount": 1}},
        {"citedPaper": {"paperId": "b", "title": "B", "externalIds": {"ArXiv": "x2"},
                        "influentialCitationCount": 9}},
    ]
    top = top_reference_records(raw, limit=1)
    assert top[0]["s2_id"] == "b" and top[0]["arxiv_id"] == "x2"


def test_clean_doi_passes_genuine_dois_through():
    assert rp.clean_doi("10.1111/jofi.13188") == "10.1111/jofi.13188"
    assert rp.clean_doi("10.1007/s00245-025-10263-5") == "10.1007/s00245-025-10263-5"
    assert rp.clean_doi("  10.1137/22m1542982  ") == "10.1137/22m1542982"


def test_clean_doi_strips_url_prefixes():
    assert rp.clean_doi("https://doi.org/10.1111/jofi.13188") == "10.1111/jofi.13188"
    assert rp.clean_doi("http://dx.doi.org/10.1111/jofi.13188") == "10.1111/jofi.13188"


def test_clean_doi_rejects_the_three_corpus_placeholders():
    # Real values found in the graph on 2026-08-31.
    assert rp.clean_doi("10.1145/NNNNNNN.NNNNNNN") is None
    assert rp.clean_doi("10.1017/S09624929XXXXXXXX") is None
    assert rp.clean_doi("http://dx.doi.org/10.1145/0000000.0000000") is None


def test_clean_doi_does_not_false_positive_on_realistic_dois():
    """Regression: an earlier case-insensitive `0{6,}` / `X{4,}` rule rejected both of
    these. Six zeros among digits and a lowercase-x suffix are both legitimate."""
    assert rp.clean_doi("10.1002/nla.2000000") == "10.1002/nla.2000000"
    assert rp.clean_doi("10.1088/1361-6420/abcxxxx") == "10.1088/1361-6420/abcxxxx"


def test_clean_doi_rejects_structurally_invalid():
    assert rp.clean_doi(None) is None
    assert rp.clean_doi("") is None
    assert rp.clean_doi("   ") is None
    assert rp.clean_doi("not-a-doi") is None
    assert rp.clean_doi("10.1145/") is None          # empty suffix
    assert rp.clean_doi("10.1/short-prefix") is None  # registrant must be 4-9 digits
