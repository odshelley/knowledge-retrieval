import pytest

from server.queries import (
    dependency_chain_cypher,
    merge_paper_hits,
    validate_depth,
    validate_expand,
    validate_kind,
    validate_top_k,
)


def test_validate_top_k_clamps():
    assert validate_top_k(8) == 8
    assert validate_top_k(0) == 1
    assert validate_top_k(500) == 25
    assert validate_top_k(None) == 8  # default


def test_validate_expand():
    assert validate_expand("local") == "local"
    assert validate_expand(None) == "local"  # default
    with pytest.raises(ValueError, match="expand"):
        validate_expand("global")


def test_validate_depth_clamps():
    assert validate_depth(3) == 3
    assert validate_depth(0) == 1
    assert validate_depth(99) == 5


def test_validate_kind():
    assert validate_kind("lemma") == "lemma"
    assert validate_kind(None) is None
    with pytest.raises(ValueError, match="kind"):
        validate_kind("conjecture")


def test_dependency_chain_cypher_interpolates_depth_safely():
    assert "*1..3" in dependency_chain_cypher(3)
    assert "*1..5" in dependency_chain_cypher(99)  # clamped, never raw


def test_merge_paper_hits_dedups_title_first():
    title_rows = [{"id": "p1", "title": "A", "score": 1.0}]
    vector_rows = [{"id": "p1", "title": "A", "score": 0.8},
                   {"id": "p2", "title": "B", "score": 0.7}]
    merged = merge_paper_hits(title_rows, vector_rows, top_k=5)
    assert [r["id"] for r in merged] == ["p1", "p2"]
    assert merged[0]["score"] == 1.0


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.last = None
    def run(self, cypher, **params):
        self.last = (cypher, params)
        class _Rec:
            def __init__(self, d): self._d = d
            def data(self): return self._d
        return [_Rec(r) for r in self._rows]
    def __enter__(self): return self
    def __exit__(self, *a): return False


class _FakeDriver:
    def __init__(self, rows): self.rows = rows; self.session_kwargs = None
    def session(self, **kwargs):
        self.session_kwargs = kwargs
        return _FakeSession(self.rows)
    def close(self): pass


def test_graph_client_reads_with_read_access():
    from neo4j import READ_ACCESS
    from server.graph import GraphClient
    from server.settings import Settings

    settings = Settings(neo4j_uri="bolt://x", neo4j_user="u", neo4j_password="p")
    driver = _FakeDriver(rows=[{"ok": 1}])
    gc = GraphClient(settings, driver=driver, openai_client=object())
    assert gc.read("RETURN 1 AS ok") == [{"ok": 1}]
    assert driver.session_kwargs["default_access_mode"] == READ_ACCESS
    assert driver.session_kwargs["database"] == "neo4j"
