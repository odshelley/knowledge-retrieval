"""Retrieval core shared by the MCP tools and the eval harness (scripts/run_eval.py).
Keeping this out of tools.py lets the harness exercise the EXACT production path
without standing up FastMCP."""
from __future__ import annotations

from server import queries as q
from server.graph import GraphClient


def search_chunks_core(graph: GraphClient, query: str, top_k: int = 8,
                       expand: str = "local", paper_id: str | None = None) -> dict:
    top_k = q.validate_top_k(top_k)
    expand = q.validate_expand(expand)
    emb = graph.embed(query)
    k = top_k * 4 if paper_id else top_k
    vec_hits = graph.read(q.VECTOR_SEARCH, k=k, top_k=top_k,
                          embedding=emb, paper_id=paper_id)
    ft_hits = graph.read(q.FULLTEXT_SEARCH, q=q.lucene_escape(query),
                         paper_id=paper_id, top_k=top_k)
    hits = q.merge_chunk_hits(vec_hits, ft_hits, top_k)
    out: dict = {"chunks": hits}
    paper_ids = sorted({h["paper_id"] for h in hits})
    if expand == "local" and paper_ids:
        out["papers"] = graph.read(q.EXPAND_LOCAL, paper_ids=paper_ids)
    elif expand == "concepts" and paper_ids:
        top = graph.read(q.TOP_CONCEPTS_FOR_PAPERS, paper_ids=paper_ids)
        out["concepts"] = graph.read(q.EXPAND_CONCEPTS,
                                     names=[t["name"] for t in top])
    return out
