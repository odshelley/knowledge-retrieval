"""Run kg_extracted against the first 5 papers in data/partitions.json.

Requires: docker compose up, schema applied, snapshot uploaded.
Run with: uv run pytest tests/integration/test_single_paper.py --run-integration -v
Cost: ~$0.50–$2 in OpenAI calls.
"""
from __future__ import annotations

import os

import pytest
from dagster import materialize

from pipeline.assets.kg_extracted import kg_extracted
from pipeline.assets.pdf_blob import pdf_blob
from pipeline.assets.v1_md_blob import v1_md_blob
from pipeline.partitions import paper_ids
from pipeline.resources import (
    AnthropicResource,
    OpenAILLMResource,
    legacy_neo4j_from_env,
    minio_from_env,
    new_neo4j_from_env,
)


@pytest.mark.integration
def test_extract_first_five_papers():
    sample = paper_ids()[:5]
    assert len(sample) == 5, "need at least 5 partitions for sample run"

    resources = {
        "minio": minio_from_env(),
        "neo4j_new": new_neo4j_from_env(),
        "neo4j_legacy": legacy_neo4j_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
    }
    for paper_id in sample:
        result = materialize(
            [pdf_blob, v1_md_blob, kg_extracted],
            partition_key=paper_id,
            resources=resources,
        )
        assert result.success, f"{paper_id}: {result}"

    new = new_neo4j_from_env()
    with new.get_driver().session(database=new.database) as s:
        n_papers = s.run("MATCH (p:Paper) RETURN count(p) AS n").single()["n"]
        n_chunks = s.run("MATCH (c:Chunk) RETURN count(c) AS n").single()["n"]
        n_concepts = s.run("MATCH (c:Concept) RETURN count(c) AS n").single()["n"]
    assert n_papers >= 5
    assert n_chunks > 50, f"expected many chunks across 5 papers, got {n_chunks}"
    assert n_concepts > 0, "expected LLM to extract at least one Concept"
