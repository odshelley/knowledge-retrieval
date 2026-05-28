import pytest
from dagster import materialize

from pipeline.assets import (raw_blob, parsed_document, triage_metadata, chunks,
                             extracted_graph, resolved_entities, graph_write, paper_analysis)
from pipeline.resources import (new_neo4j_from_env, minio_from_env, OpenAILLMResource,
                                AnthropicResource, postgres_from_env)
from pipeline.partitions import DOCUMENTS_PARTITION


_ASSETS = [raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
           chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
           graph_write.graph_write, paper_analysis.paper_analysis]


def _res():
    return {"neo4j_new": new_neo4j_from_env(), "minio": minio_from_env(),
            "openai": OpenAILLMResource(), "anthropic": AnthropicResource(),
            "postgres": postgres_from_env()}


@pytest.mark.integration
def test_one_paper_end_to_end(tmp_path):
    """Requires SOURCE_DIR with one fixture PDF, services up, and its hash registered."""
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = "FIXTURE_HASH"  # replace with the fixture PDF's sha256
    instance.add_dynamic_partitions(DOCUMENTS_PARTITION, [key])
    result = materialize(
        _ASSETS,
        partition_key=key,
        resources=_res(),
        instance=instance,
    )
    assert result.success
    new = new_neo4j_from_env()
    with new.get_driver().session(database=new.database) as s:
        assert s.run("MATCH (p:Paper {document_id:$k}) RETURN count(p) AS n",
                     k=key).single()["n"] == 1
        assert s.run("MATCH (:Paper {document_id:$k})-[:HAS_SUMMARY]->(:Summary) "
                     "RETURN count(*) AS n", k=key).single()["n"] == 1
        assert s.run("MATCH (c:Chunk)-[:BELONGS_TO]->(:Document {id:$k}) RETURN count(c) AS n",
                     k=key).single()["n"] > 0


@pytest.mark.integration
def test_rerun_is_idempotent():
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = "FIXTURE_HASH"

    def counts():
        new = new_neo4j_from_env()
        with new.get_driver().session(database=new.database) as s:
            return {
                "paper": s.run("MATCH (p:Paper {document_id:$k}) RETURN count(p) AS n", k=key).single()["n"],
                "def": s.run("MATCH (:Paper {document_id:$k})-[:STATES]->(d:Definition) RETURN count(d) AS n", k=key).single()["n"],
                "res": s.run("MATCH (:Paper {document_id:$k})-[:STATES]->(r:Result) RETURN count(r) AS n", k=key).single()["n"],
            }

    materialize(_ASSETS, partition_key=key, resources=_res(), instance=instance)
    first = counts()
    materialize(_ASSETS, partition_key=key, resources=_res(), instance=instance)
    assert counts() == first          # content-hash ids ⇒ no duplicate Definition/Result on re-run
    assert first["paper"] == 1


@pytest.mark.integration
def test_citation_backfill_b_then_a():
    """B is ingested first; A (which references B) is ingested second. The CITES edge must
    appear via graph_write's backward pending_citations pass."""
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key_b, key_a = "FIXTURE_B_HASH", "FIXTURE_A_HASH"  # A's references include B
    for k in (key_b, key_a):
        instance.add_dynamic_partitions(DOCUMENTS_PARTITION, [k])
        materialize(_ASSETS, partition_key=k, resources=_res(), instance=instance)
    new = new_neo4j_from_env()
    with new.get_driver().session(database=new.database) as s:
        n = s.run("MATCH (a:Paper {document_id:$a})-[:CITES]->(b:Paper {document_id:$b}) "
                  "RETURN count(*) AS n", a=key_a, b=key_b).single()["n"]
        assert n == 1
