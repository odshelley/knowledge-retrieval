from unittest.mock import MagicMock

from pipeline.assets.paper_summary import build_summary_prompt, parse_claude_response


def test_build_summary_prompt_includes_chunks_and_required_sections():
    chunks = ["Section A text.", "Section B text."]
    prompt = build_summary_prompt("Paper Title", "alice_2024", chunks)
    assert "Paper Title" in prompt
    assert "Section A text." in prompt
    for sec in ("motivation", "contributions", "method", "key_results", "limitations", "related_work"):
        assert sec in prompt


def test_parse_claude_response_extracts_six_sections():
    raw = """
    {
      "motivation": "m",
      "contributions": "c",
      "method": "M",
      "key_results": "r",
      "limitations": "l",
      "related_work": "rw"
    }
    """
    parsed = parse_claude_response(raw)
    assert parsed["motivation"] == "m"
    assert parsed["related_work"] == "rw"


def test_parse_claude_response_strips_code_fences():
    raw = "```json\n{\"motivation\":\"m\",\"contributions\":\"c\",\"method\":\"M\",\"key_results\":\"r\",\"limitations\":\"l\",\"related_work\":\"rw\"}\n```"
    parsed = parse_claude_response(raw)
    assert parsed["method"] == "M"
