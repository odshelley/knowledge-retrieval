"""Environment-driven configuration for the kg MCP server."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from server.auth import parse_tokens


@dataclass(frozen=True)
class Settings:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    neo4j_database: str = "neo4j"
    openai_api_key: str = ""
    embed_model: str = "text-embedding-3-small"
    tokens: dict[str, tuple[str, str]] = field(default_factory=dict)
    rate_limit_per_min: int = 60

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            neo4j_uri=os.environ["KG_NEO4J_URI"],
            neo4j_user=os.environ["KG_NEO4J_USER"],
            neo4j_password=os.environ["KG_NEO4J_PASSWORD"],
            neo4j_database=os.environ.get("KG_NEO4J_DATABASE", "neo4j"),
            openai_api_key=os.environ["OPENAI_API_KEY"],
            embed_model=os.environ.get("KG_EMBED_MODEL", "text-embedding-3-small"),
            tokens=parse_tokens(os.environ.get("KG_TOKENS", "")),
            rate_limit_per_min=int(os.environ.get("KG_RATE_LIMIT", "60")),
        )
