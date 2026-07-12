"""The 8 typed read-only MCP tools. Synthesis is the caller's job; these only retrieve."""
from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from server import queries as q
from server.graph import GraphClient


def build_mcp(graph: GraphClient) -> FastMCP:
    # FastMCP defaults to localhost-only DNS-rebinding Host filtering, which 421s any
    # deployed hostname. Bearer auth is the perimeter here; the server is never bound
    # to a browser-reachable localhost port in production.
    security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
    mcp = FastMCP("kg", stateless_http=True, transport_security=security)

    @mcp.tool()
    def search_chunks(query: str, top_k: int = 8, expand: str = "local",
                      paper_id: str | None = None) -> dict:
        """Hybrid (vector + keyword) search over paper chunks; expand='local' adds each
        hit paper's concepts, definitions, results, and CITES neighbours; expand='concepts'
        pivots to the top concepts across hits. Cite results as (paper_title, chunk position)."""
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
            out["concepts"] = graph.read(
                q.EXPAND_CONCEPTS, names=[t["name"] for t in top])
        return out

    @mcp.tool()
    def get_paper(key: str) -> dict:
        """Look up one paper by id, DOI, arXiv id, or exact title. Returns metadata,
        authors, and the structured Claude analysis (summary) when present."""
        rows = graph.read(q.GET_PAPER, key=key)
        if not rows:
            return {"found": False, "key": key}
        row = rows[0]
        summary = json.loads(row["summary_json"]) if row.get("summary_json") else None
        return {"found": True, "paper": row["paper"], "authors": row["authors"],
                "summary": summary}

    @mcp.tool()
    def search_papers(query: str, top_k: int = 8) -> dict:
        """Find papers by title substring and by chunk-embedding relevance."""
        top_k = q.validate_top_k(top_k)
        title_rows = graph.read(q.TITLE_MATCH, q=query, top_k=top_k)
        vec_rows = graph.read(q.PAPER_VECTOR_AGG, k=top_k * 4, top_k=top_k,
                              embedding=graph.embed(query))
        return {"papers": q.merge_paper_hits(title_rows, vec_rows, top_k)}

    @mcp.tool()
    def get_concept(name: str) -> dict:
        """A concept with its definitions (verbatim, with source papers), papers that
        discuss it, and co-discussed related concepts."""
        rows = graph.read(q.GET_CONCEPT, name=name)
        return rows[0] if rows else {"found": False, "name": name}

    @mcp.tool()
    def get_results(concept: str | None = None, paper_id: str | None = None,
                    kind: str | None = None) -> dict:
        """Theorems/lemmas/propositions/corollaries that USE a concept and/or are
        STATED by a paper. Provide at least one of concept/paper_id."""
        if concept is None and paper_id is None:
            raise ValueError("provide at least one of concept or paper_id")
        kind = q.validate_kind(kind)
        return {"results": graph.read(q.GET_RESULTS, concept=concept,
                                      paper_id=paper_id, kind=kind)}

    @mcp.tool()
    def get_dependency_chain(result_id: str, depth: int = 3) -> dict:
        """Walk Result-DEPENDS_ON->Result up to `depth` hops (clamped 1..5), with each
        result's statement, source paper, used concepts, and direct dependencies."""
        rows = graph.read(q.dependency_chain_cypher(depth), result_id=result_id)
        return {"nodes": rows}

    @mcp.tool()
    def get_citations(paper_id: str, direction: str = "out") -> dict:
        """CITES neighbours of a paper. direction='out' → papers it cites;
        direction='in' → papers citing it."""
        if direction not in ("in", "out"):
            raise ValueError("direction must be 'in' or 'out'")
        return {"papers": graph.read(q.GET_CITATIONS, paper_id=paper_id,
                                     direction=direction)}

    @mcp.tool()
    def get_corpus_overview() -> dict:
        """Corpus shape: node counts, most-discussed concepts, most recent papers.
        Call this FIRST to judge whether the corpus can support a question."""
        counts = graph.read(q.OVERVIEW_COUNTS)
        return {"counts": counts[0] if counts else {},
                "top_concepts": graph.read(q.OVERVIEW_TOP_CONCEPTS),
                "recent_papers": graph.read(q.OVERVIEW_RECENT)}

    return mcp
