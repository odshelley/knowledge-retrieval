"""Starlette app: /healthz (unauthenticated) + bearer-auth + rate-limit middleware
in front of the FastMCP streamable-HTTP app mounted at /v1 (endpoint /v1/mcp)."""
from __future__ import annotations

import time

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from server.auth import RateLimiter, verify_token
from server.graph import GraphClient
from server.settings import Settings


class BearerAuthMiddleware:
    def __init__(self, app, tokens, limiter: RateLimiter):
        self.app, self.tokens, self.limiter = app, tokens, limiter

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope["path"].startswith("/v1/"):
            await self.app(scope, receive, send)
            return
        headers = dict(scope["headers"])
        auth = headers.get(b"authorization", b"").decode()
        token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
        name = verify_token(token, self.tokens)
        if name is None:
            resp = JSONResponse({"error": "invalid or missing bearer token"},
                                status_code=401)
            await resp(scope, receive, send)
            return
        if not self.limiter.allow(name, now=time.monotonic()):
            resp = JSONResponse({"error": "rate limit exceeded"}, status_code=429,
                                headers={"Retry-After": "60"})
            await resp(scope, receive, send)
            return
        scope.setdefault("state", {})["kg_token_name"] = name
        await self.app(scope, receive, send)


def create_app(settings: Settings, graph: GraphClient | None = None) -> Starlette:
    graph = graph or GraphClient(settings)
    from server.tools import build_mcp
    mcp = build_mcp(graph)
    mcp_app = mcp.streamable_http_app()  # serves at /mcp within the mount

    async def healthz(request):
        try:
            graph.read("RETURN 1 AS ok")
            graph_ok = True
        except Exception:
            graph_ok = False
        return JSONResponse({"server": True, "graph": graph_ok},
                            status_code=200 if graph_ok else 503)

    app = Starlette(routes=[Route("/healthz", healthz),
                            Mount("/v1", app=mcp_app)],
                    lifespan=lambda a: mcp.session_manager.run())
    app.add_middleware(BearerAuthMiddleware, tokens=settings.tokens,
                       limiter=RateLimiter(settings.rate_limit_per_min))
    return app
