"""Bearer-token auth + per-token rate limiting for the kg MCP server."""
from __future__ import annotations

import hashlib
import hmac
from collections import deque


def hash_token(salt: str, token: str) -> str:
    return hashlib.sha256((salt + token).encode("utf-8")).hexdigest()


def parse_tokens(raw: str) -> dict[str, tuple[str, str]]:
    """Parse KG_TOKENS ('name:salt:hash,...') into {name: (salt, hash)}.

    Fails fast with a clear ValueError on malformed entries (an operator typo in
    `fly secrets set` would otherwise crash startup with a bare unpack error). Error
    messages reference entries by position only — never echo salt/hash material.
    """
    entries: dict[str, tuple[str, str]] = {}
    for i, part in enumerate(raw.split(",")):
        part = part.strip()
        if not part:
            continue
        fields = part.split(":")
        if len(fields) != 3 or not all(fields):
            raise ValueError(
                f"KG_TOKENS entry {i} is malformed: expected 'name:salt:hash', "
                f"got {len(fields)} field(s)"
            )
        name, salt, digest = fields
        if name in entries:
            raise ValueError(f"KG_TOKENS entry {i}: duplicate token name {name!r}")
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
