"""Post-deploy smoke check: healthz + every tool responds.

Usage: uv run --extra server python scripts/smoke_server.py <base_url> <token>
e.g.:  uv run --extra server python scripts/smoke_server.py https://kg-graph.fly.dev kg_osian_<hex>
"""
from __future__ import annotations

import asyncio
import sys

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

TOOL_CALLS = [
    ("get_corpus_overview", {}),
    ("search_chunks", {"query": "martingale", "top_k": 2}),
    ("search_papers", {"query": "diffusion", "top_k": 3}),
    ("get_paper", {"key": "nonexistent-id-ok"}),
    ("get_concept", {"name": "martingale"}),
    ("get_results", {"concept": "martingale"}),
    ("get_citations", {"paper_id": "nonexistent-id-ok"}),
    ("get_dependency_chain", {"result_id": "nonexistent-id-ok"}),
    # GraphRAG augmentations (PR #18) — a deploy that ships without these must fail the smoke
    ("search_concepts", {"query": "martingale convergence", "top_k": 2}),
    ("get_schema", {}),
    ("run_cypher", {"query": "MATCH (p:Paper) RETURN count(p) AS n"}),
]


async def main(base: str, token: str) -> int:
    try:
        health = httpx.get(f"{base}/healthz", timeout=10)
    except httpx.HTTPError as exc:
        print(f"healthz: unreachable ({exc})")
        return 1
    print(f"healthz: {health.status_code} {health.json()}")
    if health.status_code != 200:
        return 1
    headers = {"Authorization": f"Bearer {token}"}
    async with streamablehttp_client(f"{base}/v1/mcp", headers=headers) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()
            tools = await session.list_tools()
            print(f"tools: {sorted(t.name for t in tools.tools)}")
            failures = 0
            for name, args in TOOL_CALLS:
                res = await session.call_tool(name, args)
                status = "ERROR" if res.isError else "ok"
                failures += bool(res.isError)
                print(f"  {name}: {status}")
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <base_url> <token>")
        sys.exit(2)
    sys.exit(asyncio.run(main(sys.argv[1].rstrip("/"), sys.argv[2])))
