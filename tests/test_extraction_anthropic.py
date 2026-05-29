import json
from unittest.mock import MagicMock

from pipeline.extraction import ExtractionResult
from pipeline.extraction_anthropic import extract_from_chunk_anthropic


def _resp(payload: dict):
    block = MagicMock()
    block.type = "text"
    block.text = json.dumps(payload)
    return MagicMock(content=[block])


def test_extract_from_chunk_anthropic_parses_structured_output():
    payload = {
        "concepts": [{"name": "Wrong-Way Risk", "kind": "concept"}],
        "definitions": [{"term": "WWR", "statement": "Let $X$ be a martingale."}],
        "results": [{"name": "Thm 1", "kind": "theorem", "statement": "$x = y$"}],
    }
    client = MagicMock()
    client.messages.create.return_value = _resp(payload)
    res = extract_from_chunk_anthropic(client, "claude-opus-4-7", "some chunk")
    assert isinstance(res, ExtractionResult)
    assert [(c.name, c.kind) for c in res.concepts] == [("Wrong-Way Risk", "concept")]
    assert res.definitions[0].term == "WWR"
    assert res.results[0].kind == "theorem"
    # called with structured-output config + cache_control system block
    kwargs = client.messages.create.call_args.kwargs
    assert kwargs["output_config"]["format"]["type"] == "json_schema"
    assert kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}


def test_extract_from_chunk_anthropic_skips_non_text_blocks():
    thinking = MagicMock()
    thinking.type = "thinking"
    payload = {"concepts": [], "definitions": [], "results": []}
    text = MagicMock()
    text.type = "text"
    text.text = json.dumps(payload)
    client = MagicMock()
    client.messages.create.return_value = MagicMock(content=[thinking, text])
    res = extract_from_chunk_anthropic(client, "claude-opus-4-7", "chunk")
    assert res.concepts == [] and res.definitions == [] and res.results == []
