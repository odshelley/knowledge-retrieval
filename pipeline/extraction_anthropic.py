"""Claude (Anthropic SDK) extraction path — structured outputs via output_config.format.
Prototype alternative to the OpenAI extraction in pipeline/extraction.py, selected by the
EXTRACTION_PROVIDER env var in the extracted_graph asset. Shares the schema, system prompt,
and parser with the OpenAI path so the output shape (ExtractionResult) is identical."""
from __future__ import annotations

import json

from pipeline.extraction import (
    EXTRACTION_SCHEMA,
    SYSTEM_PROMPT,
    ExtractionResult,
    parse_extraction,
)

_MAX_CHUNK_CHARS = 12000  # mirrors the OpenAI path


def extract_from_chunk_anthropic(
    client, model: str, chunk: str, timeout: float = 60.0
) -> ExtractionResult:
    """Extract typed concepts/definitions/results from one chunk via Claude structured outputs."""
    resp = client.messages.create(
        model=model,
        max_tokens=16000,
        timeout=timeout,
        # Static instructions cached across chunks/papers (only caches above the ~1024-token
        # minimum — effective once few-shot exemplars are added to SYSTEM_PROMPT).
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": chunk[:_MAX_CHUNK_CHARS]}],
        output_config={"format": {"type": "json_schema", "schema": EXTRACTION_SCHEMA}},
    )
    # json_schema format guarantees a text block of valid JSON; select it explicitly
    # (don't assume content[0]).
    text = next(b.text for b in resp.content if getattr(b, "type", None) == "text")
    return parse_extraction(json.loads(text))
