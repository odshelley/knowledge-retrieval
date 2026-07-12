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
