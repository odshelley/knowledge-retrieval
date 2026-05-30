# Prototype: Claude Extraction Path

**Branch:** `prototype/claude-extraction` (unmerged)

## What this adds

An alternative LLM extraction path using Claude (Anthropic SDK) alongside the existing OpenAI path. The Claude path uses Anthropic's **structured outputs** (`output_config.format` with a JSON schema) to extract typed concepts, definitions, and results from paper chunks. A runtime environment variable selects which provider runs.

## How to enable

```bash
export EXTRACTION_PROVIDER=anthropic   # use Claude
# or
export EXTRACTION_PROVIDER=openai      # default — uses the existing OpenAI path
```

The default is `openai`, so the existing behaviour is unchanged unless the variable is explicitly set.

## Model default

`claude-opus-4-7`, configurable via `AnthropicResource.extraction_model` in `pipeline/resources.py` or via Dagster resource config.

## Gate-B side-by-side comparison

Run both providers over a parsed markdown file to compare outputs:

```bash
uv run python scripts/eval_extraction.py path/to/parsed.md
```

Requires `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` in the environment. Prints a JSON summary (concepts, definitions, results) for each provider.

## Deliberate design choices

- **Structured outputs, not free-form JSON.** The Claude path passes `output_config.format` with the provider-neutral `EXTRACTION_SCHEMA` constant (defined in `pipeline/extraction.py`). This guarantees valid JSON matching the schema without prompt-level JSON coaxing.
- **System-prompt caching.** The system prompt is sent with `cache_control: {type: "ephemeral"}` to enable Anthropic prompt caching across chunks of the same paper. Caching becomes effective once few-shot exemplars are added to `SYSTEM_PROMPT` (pushing it above the ~1024-token minimum).
- **No `thinking` parameter.** Structured extraction is deterministic by nature; omitting `thinking` keeps the call robust and avoids interaction with the `output_config` format.
- **No `temperature`.** Removed on Opus 4.7; the call is omitted to keep the code forward-compatible.
- **OpenAI path unchanged.** All modifications are additive. The existing `extract_from_chunk` function and its callers are untouched.
- **Client injected, not imported.** `pipeline/extraction_anthropic.py` does not import `anthropic` at the top level — the client is passed in, keeping the module cheap to import and easy to test with mocks.

## Files changed

| File | Change |
|------|--------|
| `pipeline/extraction.py` | Added `EXTRACTION_SCHEMA` constant |
| `pipeline/extraction_anthropic.py` | New — Claude extraction path |
| `pipeline/resources.py` | Added `extraction_model` field to `AnthropicResource` |
| `pipeline/assets/extracted_graph.py` | Provider switch via `EXTRACTION_PROVIDER`; added `"anthropic"` to `required_resource_keys`; added `provider`/`model` metadata |
| `scripts/eval_extraction.py` | New — Gate-B side-by-side harness |
| `tests/test_extraction_anthropic.py` | New — mocked unit tests for the Claude path |
