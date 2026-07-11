from starlette.testclient import TestClient

from server.app import create_app
from server.auth import hash_token
from server.settings import Settings
from tests.server.test_queries import _FakeDriver


TOKEN = "kg_osian_0123456789abcdef0123456789abcdef"


def make_app(rows=None):
    settings = Settings(
        neo4j_uri="bolt://x", neo4j_user="u", neo4j_password="p",
        tokens={"osian": ("s4lt", hash_token("s4lt", TOKEN))}, rate_limit_per_min=1000)
    from server.graph import GraphClient
    graph = GraphClient(settings, driver=_FakeDriver(rows or [{"ok": 1}]),
                        openai_client=object())
    return create_app(settings, graph=graph)


def test_healthz_no_auth_required():
    client = TestClient(make_app())
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"server": True, "graph": True}


def test_v1_requires_bearer_token():
    # The FastMCP session manager's task group only starts inside the ASGI
    # lifespan, which starlette's TestClient only fires when used as a
    # context manager (see server/app.py docstring on lifespan wiring).
    with TestClient(make_app()) as client:
        assert client.post("/v1/mcp", json={}).status_code == 401
        assert client.post("/v1/mcp", json={},
                           headers={"Authorization": "Bearer nope"}).status_code == 401


def test_v1_accepts_valid_token():
    with TestClient(make_app()) as client:
        resp = client.post("/v1/mcp", json={}, headers={"Authorization": f"Bearer {TOKEN}"})
        assert resp.status_code != 401  # reaches the MCP app (may 4xx on protocol, not auth)


def test_rate_limit_returns_429():
    settings = Settings(
        neo4j_uri="bolt://x", neo4j_user="u", neo4j_password="p",
        tokens={"osian": ("s4lt", hash_token("s4lt", TOKEN))}, rate_limit_per_min=1)
    from server.graph import GraphClient
    graph = GraphClient(settings, driver=_FakeDriver([{"ok": 1}]), openai_client=object())
    with TestClient(create_app(settings, graph=graph)) as client:
        headers = {"Authorization": f"Bearer {TOKEN}"}
        client.post("/v1/mcp", json={}, headers=headers)
        assert client.post("/v1/mcp", json={}, headers=headers).status_code == 429


def test_tools_are_registered():
    import anyio
    from server.tools import build_mcp
    from server.graph import GraphClient
    settings = Settings(neo4j_uri="bolt://x", neo4j_user="u", neo4j_password="p")
    graph = GraphClient(settings, driver=_FakeDriver([]), openai_client=object())
    mcp = build_mcp(graph)
    tools = anyio.run(mcp.list_tools)
    names = {t.name for t in tools}
    assert names == {"search_chunks", "get_paper", "search_papers", "get_concept",
                     "get_results", "get_dependency_chain", "get_citations",
                     "get_corpus_overview"}
