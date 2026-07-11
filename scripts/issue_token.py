"""Mint a kg bearer token: prints the token (give to the colleague, once) and the
KG_TOKENS entry (append to the server's env/secret). Usage: uv run python scripts/issue_token.py <name>"""
from __future__ import annotations

import re
import secrets
import sys

sys.path.insert(0, ".")  # allow `from server...` when run from repo root
from server.auth import hash_token  # noqa: E402


def mint(name: str) -> tuple[str, str]:
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise ValueError("name must be lowercase alphanumeric/underscore")
    token = f"kg_{name}_{secrets.token_hex(16)}"
    salt = secrets.token_hex(8)
    return token, f"{name}:{salt}:{hash_token(salt, token)}"


if __name__ == "__main__":
    token, entry = mint(sys.argv[1])
    print(f"token (give to colleague, do not store): {token}")
    print(f"KG_TOKENS entry (append server-side):    {entry}")
