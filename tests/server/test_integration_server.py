"""End-to-end tool checks against the real Aura graph (read-only). Needs .env with
KG_NEO4J_* + OPENAI_API_KEY and a non-empty graph. Run:
uv run --extra dev --extra server pytest tests/server/test_integration_server.py \\
    --run-integration -v
"""
import os

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def graph():
    from server.graph import GraphClient
    from server.settings import Settings
    required = ("KG_NEO4J_URI", "KG_NEO4J_USER", "KG_NEO4J_PASSWORD", "OPENAI_API_KEY")
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        pytest.skip(f"server integration env not configured (missing: {', '.join(missing)})")
    gc = GraphClient(Settings.from_env())
    yield gc
    gc.close()


@pytest.fixture(scope="module")
def mcp(graph):
    from server.tools import build_mcp
    return build_mcp(graph)


def _call(mcp, name, args):
    import anyio
    result = anyio.run(mcp.call_tool, name, args)
    return result


def test_overview_nonempty(mcp):
    out = _call(mcp, "get_corpus_overview", {})
    assert out  # counts present; graph has content


def test_search_chunks_local_expand(mcp):
    out = _call(mcp, "search_chunks", {"query": "stochastic process", "top_k": 3})
    # shape only — corpus-dependent content
    assert out is not None


def test_search_chunks_rejects_bad_expand(mcp):
    with pytest.raises(Exception, match="expand"):
        _call(mcp, "search_chunks", {"query": "x", "expand": "global"})


def test_get_results_requires_a_filter(mcp):
    with pytest.raises(Exception, match="at least one"):
        _call(mcp, "get_results", {})


def test_write_attempt_fails_readonly(graph):
    """The READ_ACCESS session (and, once created, the read-only user) must refuse writes."""
    with pytest.raises(Exception):
        graph.read("CREATE (x:KgWriteProbe) RETURN x")


def test_run_cypher_write_attempt_rejected_by_guard(mcp):
    """run_cypher's courtesy guard must raise ValueError before ever reaching the driver."""
    with pytest.raises(Exception, match="read-only"):
        _call(mcp, "run_cypher", {"query": "CREATE (n)"})


def test_read_limited_write_blocked_by_driver_not_just_guard(graph):
    """read_limited is the ACTUAL run_cypher execution path — an autocommit run on a READ_ACCESS
    session, a different enforcement path from graph.read's managed read-transaction. Verify the
    driver itself refuses a write here, using a query the string-level guard would NOT catch (no
    write keyword in scannable text), so this exercises driver routing, not check_read_only."""
    with pytest.raises(Exception):
        # CALL apoc.create.node would write, but we cannot rely on apoc; instead use a plain
        # write that the guard is bypassed for by construction: call read_limited directly with a
        # CREATE (the guard lives in the tool layer, not in read_limited).
        graph.read_limited("CREATE (x:KgWriteProbe) RETURN x")


def test_read_limited_char_budget_truncates(graph):
    """A single aggregating row must not return unbounded payload: the char budget trips
    truncated=True rather than buffering the whole graph into one record."""
    rows, truncated = graph.read_limited(
        "MATCH (c:Chunk) RETURN collect(c.text) AS blob", max_chars=1000)
    assert truncated is True


def test_search_chunks_reaches_book_content(mcp, graph):
    """Williams (v2-ingested) must be findable by hybrid search with a section citation.
    'upcrossing' is Williams-specific vocabulary absent from the paper corpus."""
    from server.retrieve import search_chunks_core
    out = search_chunks_core(graph, "upcrossing lemma martingale convergence", top_k=8)
    book_hits = [c for c in out["chunks"] if c.get("source_type") == "book"]
    assert book_hits, f"no book chunks in hits: {[c['paper_title'] for c in out['chunks']]}"
    assert book_hits[0]["section"] is not None
    assert book_hits[0]["chapter"] is not None


def test_get_concept_returns_book_definition(mcp):
    """'martingale' is defined in Williams; its definition entry must cite the book."""
    import json
    out = _call(mcp, "get_concept", {"name": "martingale"})
    # MCP wraps the result in TextContent; extract the JSON from it
    if isinstance(out, list):
        result_text = out[0].text
        data = json.loads(result_text)
    else:
        data = out
    defs = data["definitions"]
    book_defs = [d for d in defs if d.get("source_type") == "book"]
    assert book_defs, f"no book-sourced definitions: {defs}"
    assert book_defs[0]["section"] is not None
