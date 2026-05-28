"""Create the pgvector extension + resolver tables."""
from __future__ import annotations

import os
import psycopg

DDL = [
    "CREATE EXTENSION IF NOT EXISTS vector",
    "CREATE TABLE IF NOT EXISTS entity_embeddings ("
    " canonical text, label text, embedding vector(1536),"
    " PRIMARY KEY (canonical, label))",
    "CREATE TABLE IF NOT EXISTS resolution_decisions ("
    " id bigserial PRIMARY KEY, candidate text, matched_to text, label text,"
    " score double precision, action text, run_id text, ts timestamptz DEFAULT now())",
    "CREATE TABLE IF NOT EXISTS alias_map ("
    " alias text, label text, canonical text, PRIMARY KEY (alias, label))",
    "CREATE TABLE IF NOT EXISTS pending_citations ("
    " id bigserial PRIMARY KEY, citing_paper_id text NOT NULL,"
    " ref_doi text, ref_arxiv_id text, ref_title_norm text, ref_s2_id text,"
    " influential_count int DEFAULT 0, created_ts timestamptz DEFAULT now(),"
    " resolved bool DEFAULT false)",
    "CREATE UNIQUE INDEX IF NOT EXISTS pending_citations_uniq ON pending_citations ("
    " citing_paper_id, coalesce(ref_doi,''), coalesce(ref_arxiv_id,''),"
    " coalesce(ref_s2_id,''), coalesce(ref_title_norm,''))",
]


def main() -> None:
    with psycopg.connect(os.environ["RESOLVER_POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:
            for stmt in DDL:
                cur.execute(stmt)
        conn.commit()
    print("resolver schema ready")


if __name__ == "__main__":
    main()
