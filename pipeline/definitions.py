# pipeline/definitions.py
from __future__ import annotations

from dagster import Definitions

from pipeline.partitions import partitions_def
from pipeline.resources import (
    legacy_neo4j_from_env,
    minio_from_env,
    new_neo4j_from_env,
    OpenAILLMResource,
    AnthropicResource,
)

# Asset modules — populated in subsequent tasks
from pipeline.assets import (
    kg_extracted,
    legacy_mirror,
    paper_summary,
    pdf_blob,
    structural_overlay,
    v1_md_blob,
)
from pipeline.sensors import minio_pdf_sensor
from pipeline.jobs import bulk_reingest, legacy_mirror_job

defs = Definitions(
    assets=[
        pdf_blob.pdf_blob,
        v1_md_blob.v1_md_blob,
        legacy_mirror.legacy_graph_mirror,
        kg_extracted.kg_extracted,
        structural_overlay.structural_overlay,
        paper_summary.paper_summary,
    ],
    sensors=[minio_pdf_sensor],
    jobs=[bulk_reingest, legacy_mirror_job],
    resources={
        "neo4j_new": new_neo4j_from_env(),
        "neo4j_legacy": legacy_neo4j_from_env(),
        "minio": minio_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
    },
)
