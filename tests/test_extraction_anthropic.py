from unittest.mock import MagicMock

from pipeline.extraction import ExtractionResult, parse_extraction
from pipeline.extraction_anthropic import extract_from_chunk_anthropic


def _resp(payload: dict):
    # messages.parse() returns a ParsedMessage whose .parsed_output is the validated model.
    return MagicMock(parsed_output=parse_extraction(payload))


def test_extract_from_chunk_anthropic_parses_structured_output():
    payload = {
        "concepts": [{"name": "Wrong-Way Risk", "kind": "concept"}],
        "definitions": [{"term": "WWR", "statement": "Let $X$ be a martingale."}],
        "results": [{"name": "Thm 1", "kind": "theorem", "statement": "$x = y$"}],
    }
    client = MagicMock()
    client.messages.parse.return_value = _resp(payload)
    res = extract_from_chunk_anthropic(client, "claude-opus-4-7", "some chunk")
    assert isinstance(res, ExtractionResult)
    assert [(c.name, c.kind) for c in res.concepts] == [("Wrong-Way Risk", "concept")]
    assert res.definitions[0].term == "WWR"
    assert res.results[0].kind == "theorem"
    # called with structured-output format (the Pydantic model) + cache_control system block
    kwargs = client.messages.parse.call_args.kwargs
    assert kwargs["output_format"] is ExtractionResult
    assert kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}


def test_extract_from_chunk_anthropic_returns_empty_result():
    payload = {"concepts": [], "definitions": [], "results": []}
    client = MagicMock()
    client.messages.parse.return_value = _resp(payload)
    res = extract_from_chunk_anthropic(client, "claude-opus-4-7", "chunk")
    assert res.concepts == [] and res.definitions == [] and res.results == []
