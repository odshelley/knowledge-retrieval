from dagster import Definitions

from pipeline.assets import (
    raw_blob, parsed_document, triage_metadata, chunks,
    extracted_graph, resolved_entities, graph_write, paper_analysis,
)
from pipeline.runtime.jobs import ingest_document
from pipeline.runtime.schedules import daily_ingest_schedule
from pipeline.runtime.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env, postgres_from_env,
)

defs = Definitions(
    assets=[
        raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
        chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
        graph_write.graph_write, paper_analysis.paper_analysis,
    ],
    jobs=[ingest_document],
    schedules=[daily_ingest_schedule],
    resources={
        "neo4j_new": new_neo4j_from_env(),
        "minio": minio_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
        "postgres": postgres_from_env(),
    },
)
