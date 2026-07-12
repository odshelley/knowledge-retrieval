from pipeline.books.extraction import (
    attach_pages, chapter_payload, chunk_with_context, flatten_concepts,
)
from pipeline.extraction.extraction import (
    Concept, Definition, ExtractionResult, Result, merge_results,
)


def test_definition_model_accepts_printed_label():
    d = Definition(term="Levy process", statement="$X_t$ has independent increments.",
                   name="Definition 1.1")
    assert d.name == "Definition 1.1"
    assert Definition(term="t", statement="s").name == ""   # default, paper path unaffected


def test_chunk_with_context_prepends_header_keeps_text():
    chapter = {"number": 3, "title": "Chapter 3 Convergence"}
    section = {"number": "3.2", "title": "3.2 Tightness"}
    out = chunk_with_context("Tiny Book", chapter, section, "The chunk body.")
    assert out.endswith("The chunk body.")
    assert '"Tiny Book"' in out and "Chapter 3" in out and "3.2" in out
    assert "exactly as printed" in out            # label-capture instruction present


def test_attach_pages_first_seen_page_wins():
    d = Definition(term="X", statement="$s_1$", name="Definition 1.1")
    r = Result(kind="theorem", statement="$t_1$", name="Theorem 1.2")
    per_chunk = [
        (ExtractionResult(definitions=[d]), 12, 0),
        (ExtractionResult(definitions=[d], results=[r]), 13, 1),   # d repeats on page 13
    ]
    merged = merge_results([e for e, _, _ in per_chunk])
    defs, results, proof_rows = attach_pages(merged, per_chunk)
    assert defs[0]["page"] == 12                                # first-seen
    assert results[0]["page"] == 13
    assert defs[0]["name"] == "Definition 1.1"
    assert len(proof_rows) == 0  # r has no proof


def test_flatten_concepts_dedups_across_sections_case_insensitive():
    a = ExtractionResult(concepts=[Concept(name="Levy process")])
    b = ExtractionResult(concepts=[Concept(name="levy PROCESS"), Concept(name="Martingale")])
    flat = flatten_concepts([a, b])
    assert [c["name"] for c in flat] == ["Levy process", "Martingale"]


def test_chapter_payload_shape():
    payload = chapter_payload(
        "isbn:x", {"id": "isbn:x:ch01"},
        section_outputs=[{"section_id": "isbn:x:ch01:s01", "definitions": [], "results": []}],
        concepts=[{"name": "Levy process", "kind": "concept"}])
    assert payload["book_id"] == "isbn:x" and payload["chapter_id"] == "isbn:x:ch01"
    assert payload["sections"][0]["section_id"] == "isbn:x:ch01:s01"
    assert payload["concepts"][0]["name"] == "Levy process"


def test_attach_pages_returns_proof_chunk_rows():
    from pipeline.extraction.extraction import ProofSketch
    r1 = Result(kind="theorem", name="9.7. Theorem.", statement="Full statement here.",
                proof_present=True, proof=ProofSketch(sketch="Sketchy.", technique="t"))
    r2 = Result(kind="theorem", name="9.7. Theorem.", statement="Full statement here.",
                proof_present=True)
    er1 = ExtractionResult(results=[r1])
    er2 = ExtractionResult(results=[r2])
    merged = merge_results([er1, er2])
    defs, results, proof_rows = attach_pages(merged, [(er1, 101, 7), (er2, 102, 8)])
    assert results[0]["proof"]["sketch"] == "Sketchy."
    positions = sorted(pr["position"] for pr in proof_rows)
    assert positions == [7, 8]
    assert all(pr["label"] == "9.7. Theorem." for pr in proof_rows)


def test_chapter_payload_carries_section_notations():
    payload = chapter_payload(
        "title:test book", {"id": "title:test book:ch:1", "number": 1, "title": "Ch"},
        [{"section_id": "s1", "definitions": [], "results": [], "proof_chunks": [],
          "notations": [{"symbol_latex": "$\\mu$", "meaning": "a measure", "concept": ""}]}],
        [])
    assert payload["sections"][0]["notations"][0]["symbol_latex"] == "$\\mu$"
