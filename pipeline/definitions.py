from dagster import Definitions

from pipeline.assets import chunks, parsed_document, raw_blob
from pipeline.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env,
)

defs = Definitions(
    assets=[raw_blob.raw_blob, parsed_document.parsed_document, chunks.chunks],
    resources={
        "neo4j_new": new_neo4j_from_env(),
        "minio": minio_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
    },
)
