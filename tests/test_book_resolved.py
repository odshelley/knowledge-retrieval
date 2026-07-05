def test_book_chapter_resolved_reuses_paper_resolution_stack():
    """The Lévy-process guarantee is code reuse: the book asset must call the SAME
    resolve_concepts / resolved_concept_row as the paper asset — no parallel ladder."""
    import ast
    import pathlib

    src = pathlib.Path("pipeline/assets/book_chapter_resolved.py").read_text()
    tree = ast.parse(src)
    imports = {a.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
               for a in node.names}
    modules = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert "resolve_concepts" in imports
    assert "pipeline.resolution.resolver" in modules
    assert "resolved_concept_row" in imports          # shared row shape with graph_write
    assert "adjudicate" in imports                    # same LLM adjudicator


def test_book_chapter_resolved_passes_sections_through():
    from pipeline.assets.book_chapter_resolved import passthrough_payload
    payload = {"book_id": "isbn:x", "chapter_id": "isbn:x:ch01",
               "concepts": [{"name": "A", "kind": "concept"}],
               "sections": [{"section_id": "isbn:x:ch01:s01", "definitions": [], "results": []}]}
    out = passthrough_payload(payload, resolved_rows=[{"surface": "A", "name": "A",
                                                       "kind": "concept", "action": "create",
                                                       "embedding": [0.1]}],
                              alias_rows=[{"key": "a", "canonical": "A", "source": "det"}])
    assert out["sections"] == payload["sections"]
    assert out["chapter_id"] == "isbn:x:ch01"
    assert out["concepts"][0]["surface"] == "A"
    assert out["alias_registrations"][0]["canonical"] == "A"
