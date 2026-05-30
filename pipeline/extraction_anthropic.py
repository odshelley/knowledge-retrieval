"""Claude (Anthropic SDK) extraction path — structured outputs via messages.parse().
Prototype alternative to the OpenAI extraction in pipeline/extraction.py, selected by the
EXTRACTION_PROVIDER env var in the extracted_graph asset. Shares the Pydantic models and system
prompt with the OpenAI path, so the output shape (ExtractionResult) is identical; .parse()
derives the json_schema output format from ExtractionResult and validates the response back."""
from __future__ import annotations

from pipeline.extraction import (
    SYSTEM_PROMPT,
    ExtractionResult,
)

_MAX_CHUNK_CHARS = 12000  # mirrors the OpenAI path


def extract_from_chunk_anthropic(
    client, model: str, chunk: str, timeout: float = 60.0
) -> ExtractionResult:
    """Extract typed concepts/definitions/results from one chunk via Claude structured outputs."""
    resp = client.messages.parse(
        model=model,
        max_tokens=16000,
        timeout=timeout,
        # Static instructions cached across chunks/papers (only caches above the ~1024-token
        # minimum — effective once few-shot exemplars are added to SYSTEM_PROMPT).
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": chunk[:_MAX_CHUNK_CHARS]}],
        output_format=ExtractionResult,
    )
    # .parse() validates the json_schema text block into ExtractionResult and surfaces it here.
    return resp.parsed_output
