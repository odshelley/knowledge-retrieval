from server.retrieve import search_chunks_core


class FakeGraph:
    """Returns canned rows per query constant; records calls."""
    def __init__(self, rows_by_query):
        self.rows_by_query = rows_by_query
        self.calls = []

    def embed(self, text):
        return [0.0] * 1536

    def read(self, cypher, **params):
        self.calls.append(cypher)
        for key, rows in self.rows_by_query.items():
            if key in cypher:
                return rows
        return []


def _chunk(cid, pid, score):
    return {"chunk_id": cid, "score": score, "text": "t", "position": 1,
            "paper_id": pid, "paper_title": "T", "year": 2024}


def test_core_merges_and_expands_local():
    g = FakeGraph({
        "db.index.vector.queryNodes('chunk_embedding'": [_chunk("c1", "p1", 0.9)],
        "db.index.fulltext.queryNodes('chunk_text'": [_chunk("c2", "p2", 8.0)],
        "UNWIND $paper_ids AS pid": [{"paper_id": "p1"}, {"paper_id": "p2"}],
    })
    out = search_chunks_core(g, "girsanov theorem", top_k=5, expand="local")
    assert {c["chunk_id"] for c in out["chunks"]} == {"c1", "c2"}
    assert "papers" in out


def test_core_expand_none_skips_expansion():
    g = FakeGraph({"db.index.vector.queryNodes('chunk_embedding'": [_chunk("c1", "p1", 0.9)]})
    out = search_chunks_core(g, "q", top_k=3, expand="none")
    assert list(out.keys()) == ["chunks"]
