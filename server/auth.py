"""Bearer-token auth + per-token rate limiting for the kg MCP server."""
from __future__ import annotations

import hashlib
import hmac
from collections import deque


def hash_token(salt: str, token: str) -> str:
    return hashlib.sha256((salt + token).encode("utf-8")).hexdigest()


def parse_tokens(raw: str) -> dict[str, tuple[str, str]]:
    """Parse KG_TOKENS ('name:salt:hash,...') into {name: (salt, hash)}."""
    entries: dict[str, tuple[str, str]] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        name, salt, digest = part.split(":")
        entries[name] = (salt, digest)
    return entries


def verify_token(token: str, entries: dict[str, tuple[str, str]]) -> str | None:
    """Return the token's name if valid, else None. Constant-time compare per entry."""
    if not token:
        return None
    for name, (salt, digest) in entries.items():
        if hmac.compare_digest(hash_token(salt, token), digest):
            return name
    return None


class RateLimiter:
    """Sliding 60s window per token name. In-memory; resets on restart (fine for v1)."""

    def __init__(self, limit_per_min: int):
        self.limit = limit_per_min
        self._hits: dict[str, deque[float]] = {}

    def allow(self, name: str, now: float) -> bool:
        window = self._hits.setdefault(name, deque())
        while window and now - window[0] >= 60.0:
            window.popleft()
        if len(window) >= self.limit:
            return False
        window.append(now)
        return True
