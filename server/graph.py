"""Read-only Neo4j access + server-side query embedding. The ONLY pipeline imports
allowed in server/ are pipeline.embedding and pipeline.graph.schema (both dagster-free)."""
from __future__ import annotations

from neo4j import READ_ACCESS, GraphDatabase, Query, unit_of_work

from pipeline.embedding import embed_texts
from server.settings import Settings

# Bound every read so a stalled Neo4j query can't pin a server worker.
READ_TIMEOUT_S = 15.0


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
        @unit_of_work(timeout=READ_TIMEOUT_S)
        def _read(tx):
            return [r.data() for r in tx.run(cypher, **params)]

        with self._driver.session(
            database=self.settings.neo4j_database, default_access_mode=READ_ACCESS
        ) as s:
            return s.execute_read(_read)

    def read_limited(self, cypher: str, timeout: float = 15.0, max_rows: int = 100,
                     max_chars: int = 200_000, **params) -> tuple[list[dict], bool]:
        """Guarded read for run_cypher: server-side tx timeout + row cap + a serialized-size
        budget. The row cap alone does not bound memory — a single aggregating row such as
        `RETURN collect(c.embedding)` collapses the whole graph into one record — so we also stop
        once the accumulated payload exceeds max_chars. Returns (rows, truncated)."""
        with self._driver.session(
            database=self.settings.neo4j_database, default_access_mode=READ_ACCESS
        ) as s:
            result = s.run(Query(cypher, timeout=timeout), **params)
            rows: list[dict] = []
            total = 0
            for record in result:
                data = record.data()
                total += len(repr(data))
                if total > max_chars and rows:
                    return rows, True
                rows.append(data)
                if len(rows) >= max_rows or total > max_chars:
                    return rows, True
            return rows, False

    def embed(self, text: str) -> list[float]:
        return embed_texts(self._openai, [text], self.settings.embed_model)[0]

    def close(self) -> None:
        self._driver.close()
