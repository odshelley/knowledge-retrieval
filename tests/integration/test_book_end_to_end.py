"""Book pipeline integration tests. Setup (mirrors the paper fixtures):

    uv run python tests/fixtures/make_book_pdf.py "$BOOKS_SOURCE_DIR/tiny-book.pdf"
    export INTEGRATION_BOOK_HASH=$(shasum -a 256 "$BOOKS_SOURCE_DIR/tiny-book.pdf" | cut -d' ' -f1)
    uv run pytest tests/integration/test_book_end_to_end.py --run-integration -v
"""
import os
from contextlib import contextmanager

import pytest
from dagster import materialize

from pipeline.assets import (
    book_raw_blob, book_parsed, book_metadata, book_structure, book_chunks,
    book_structure_write, book_chapter_extraction, book_chapter_resolved,
    book_chapter_graph_write,
)
from pipeline.runtime.partitions import (
    BOOK_CHAPTERS_PARTITION, BOOKS_PARTITION, chapter_partition_key,
)
from pipeline.runtime.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env, postgres_from_env,
)

_BOOK_ASSETS = [book_raw_blob.book_raw_blob, book_parsed.book_parsed,
                book_metadata.book_metadata, book_structure.book_structure,
                book_chunks.book_chunks, book_structure_write.book_structure_write]
_CHAPTER_ASSETS = [book_chapter_extraction.book_chapter_extraction,
                   book_chapter_resolved.book_chapter_resolved,
                   book_chapter_graph_write.book_chapter_graph_write]

BOOK_ID = "isbn:9783161484100"   # fixed by the fixture's ISBN line


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"missing required env var: {name}")
    return value


def _res():
    return {"neo4j_new": new_neo4j_from_env(), "minio": minio_from_env(),
            "openai": OpenAILLMResource(), "anthropic": AnthropicResource(),
            "postgres": postgres_from_env()}


@contextmanager
def _session():
    new = new_neo4j_from_env()
    with new.get_driver() as driver:
        with driver.session(database=new.database) as session:
            yield session


def _ingest_book(instance, key):
    instance.add_dynamic_partitions(BOOKS_PARTITION, [key])
    result = materialize(_BOOK_ASSETS, partition_key=key, resources=_res(), instance=instance)
    assert result.success
    return result


@pytest.mark.integration
def test_book_structure_end_to_end():
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")
    _ingest_book(instance, key)
    with _session() as s:
        assert s.run("MATCH (b:Book {id:$b}) RETURN count(b) AS n", b=BOOK_ID).single()["n"] == 1
        assert s.run("MATCH (:Book {id:$b})-[:HAS_DOCUMENT]->(d:Document {id:$k}) "
                     "RETURN count(d) AS n", b=BOOK_ID, k=key).single()["n"] == 1
        # front matter + 2 chapters, 1+2+2 sections (fixture contract, Task 3)
        assert s.run("MATCH (:Book {id:$b})-[:HAS_CHAPTER]->(c) RETURN count(c) AS n",
                     b=BOOK_ID).single()["n"] == 3
        assert s.run("MATCH (:Book {id:$b})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->(s) "
                     "RETURN count(s) AS n", b=BOOK_ID).single()["n"] == 5
        # every chunk is located: BELONGS_TO document AND PART_OF a section, with pages
        orphans = s.run(
            "MATCH (c:Chunk)-[:BELONGS_TO]->(:Document {id:$k}) "
            "WHERE NOT (c)-[:PART_OF]->(:Section) OR c.page_start IS NULL "
            "RETURN count(c) AS n", k=key).single()["n"]
        assert orphans == 0
    # chapter partitions registered for the sensor to pick up
    chapters = instance.get_dynamic_partitions(BOOK_CHAPTERS_PARTITION)
    for n in (0, 1, 2):
        assert chapter_partition_key(key, n) in chapters


@pytest.mark.integration
def test_chapter_extraction_grounds_statements_in_sections():
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")
    _ingest_book(instance, key)
    ck = chapter_partition_key(key, 1)
    instance.add_dynamic_partitions(BOOK_CHAPTERS_PARTITION, [ck])
    result = materialize(_CHAPTER_ASSETS, partition_key=ck, resources=_res(),
                         instance=instance)
    assert result.success
    with _session() as s:
        # Definition 1.1 lands under a Section of chapter 1 with label + page
        row = s.run(
            "MATCH (:Book {id:$b})-[:HAS_CHAPTER]->(:Chapter {number:1})"
            "-[:HAS_SECTION]->(sec)-[:STATES]->(d:Definition) "
            "RETURN d.label AS label, d.page AS page, sec.number AS sec LIMIT 5",
            b=BOOK_ID).data()
        assert row, "no Definition attached to chapter 1 sections"
        assert any(r["label"] and r["label"].startswith("Definition 1.1") for r in row)
        assert all(r["page"] is not None for r in row)


@pytest.mark.integration
def test_levy_process_is_one_concept_across_paper_and_book_paths():
    """THE shared-resolution guarantee. Seed 'Lévy process' as if a paper created it
    (Concept node + pgvector embedding + alias), run book chapter 1 extraction, then
    assert the book attached to the SAME node and no near-duplicate Concept appeared."""
    from dagster import DagsterInstance

    from pipeline.embedding import embed_texts
    from pipeline.resolution.canonicalize import canonical_key
    from pipeline.resolution.resolver import upsert_alias, upsert_embedding

    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")

    # seed via the exact primitives graph_write uses on the paper path
    seed = "Levy process"
    cfg = OpenAILLMResource()
    vec = embed_texts(cfg.get_client(), [seed], model=cfg.embedding_model)[0]
    with _session() as s:
        s.run("MERGE (c:Concept {name:$n}) SET c.tags=['concept']", n=seed)
    pg = postgres_from_env()
    with pg.connect() as conn:
        with conn.cursor() as cur:
            upsert_embedding(cur, seed, "Concept", vec)
            upsert_alias(cur, "Concept", canonical_key(seed), seed, "integration-seed")
        conn.commit()

    _ingest_book(instance, key)
    ck = chapter_partition_key(key, 1)
    instance.add_dynamic_partitions(BOOK_CHAPTERS_PARTITION, [ck])
    assert materialize(_CHAPTER_ASSETS, partition_key=ck, resources=_res(),
                       instance=instance).success

    with _session() as s:
        n_concepts = s.run(
            "MATCH (c:Concept) WHERE toLower(c.name) CONTAINS 'levy' "
            "OR toLower(c.name) CONTAINS 'lévy' RETURN count(c) AS n").single()["n"]
        covered = s.run(
            "MATCH (:Book {id:$b})-[:COVERS]->(c:Concept {name:$n}) RETURN count(c) AS m",
            b=BOOK_ID, n=seed).single()["m"]
    assert covered == 1, "book did not attach to the seeded concept"
    assert n_concepts == 1, f"expected ONE Lévy concept, found {n_concepts} — resolution split"


@pytest.mark.integration
def test_book_rerun_is_idempotent():
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")

    def counts():
        with _session() as s:
            return {
                "book": s.run("MATCH (b:Book {id:$b}) RETURN count(b) AS n",
                              b=BOOK_ID).single()["n"],
                "chapters": s.run("MATCH (:Book {id:$b})-[:HAS_CHAPTER]->(c) "
                                  "RETURN count(c) AS n", b=BOOK_ID).single()["n"],
                "chunks": s.run("MATCH (c:Chunk)-[:BELONGS_TO]->(:Document {id:$k}) "
                                "RETURN count(c) AS n", k=key).single()["n"],
            }

    _ingest_book(instance, key)
    first = counts()
    _ingest_book(instance, key)
    assert counts() == first
    assert first["book"] == 1
