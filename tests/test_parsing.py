from pipeline.ingest.parsing import needs_ocr, ParseResult

def test_needs_ocr_true_when_no_text_layer():
    assert needs_ocr(extractable_chars=0, page_count=10) is True

def test_needs_ocr_false_for_rich_text_layer():
    assert needs_ocr(extractable_chars=50000, page_count=10) is False

def test_parse_result_flags_empty():
    assert ParseResult(markdown="", mode="text").is_empty is True
    assert ParseResult(markdown="# Title\n\neq $$x$$", mode="text").is_empty is False


def test_sanitize_text_strips_nul_bytes():
    # Regression: bridge_schrodinger.pdf's text layer contained \x00, which flowed into a
    # concept name and crashed resolution ("PostgreSQL text fields cannot contain NUL").
    from pipeline.ingest.parsing import sanitize_text
    assert sanitize_text("Schr\x00odinger bridge\x00") == "Schrodinger bridge"
    assert sanitize_text("clean text") == "clean text"
