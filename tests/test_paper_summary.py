from pipeline.assets.paper_summary import build_summary_prompt


def test_build_summary_prompt_includes_chunks_and_required_sections():
    chunks = ["Section A text.", "Section B text."]
    prompt = build_summary_prompt("Paper Title", "alice_2024", chunks)
    assert "Paper Title" in prompt
    assert "Section A text." in prompt
    for sec in ("motivation", "contributions", "method", "key_results", "limitations", "related_work"):
        assert sec in prompt
