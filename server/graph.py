"""Read-only Neo4j access + server-side query embedding. The ONLY pipeline import
allowed in server/ is pipeline.embedding (dagster-free)."""
from __future__ import annotations

from neo4j import READ_ACCESS, GraphDatabase

from pipeline.embedding import embed_texts
from server.settings import Settings


class GraphClient:
    def __init__(self, settings: Settings, driver=None, openai_client=None):
        self.settings = settings
        self._driver = driver or GraphDatabase.driver(
            settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        if openai_client is None:
            from openai import OpenAI
            openai_client = OpenAI(api_key=settings.openai_api_key)
        self._openai = openai_client

    def read(self, cypher: str, **params) -> list[dict]:
        with self._driver.session(
            database=self.settings.neo4j_database, default_access_mode=READ_ACCESS
        ) as s:
            return [r.data() for r in s.run(cypher, **params)]

    def embed(self, text: str) -> list[float]:
        return embed_texts(self._openai, [text], self.settings.embed_model)[0]

    def close(self) -> None:
        self._driver.close()
