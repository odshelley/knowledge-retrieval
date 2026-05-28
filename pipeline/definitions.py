from dagster import Definitions

from pipeline.assets import chunks, extracted_graph, parsed_document, raw_blob, triage_metadata
from pipeline.assets import resolved_entities
from pipeline.assets import graph_write
from pipeline.assets import paper_analysis
from pipeline.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env, postgres_from_env,
)

defs = Definitions(
    assets=[raw_blob.raw_blob, parsed_document.parsed_document, chunks.chunks, triage_metadata.triage_metadata, extracted_graph.extracted_graph, resolved_entities.resolved_entities, graph_write.graph_write, paper_analysis.paper_analysis],
    resources={
        "neo4j_new": new_neo4j_from_env(),
        "minio": minio_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
        "postgres": postgres_from_env(),
    },
)
