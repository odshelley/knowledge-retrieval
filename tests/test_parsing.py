from pipeline.parsing import needs_ocr, ParseResult

def test_needs_ocr_true_when_no_text_layer():
    assert needs_ocr(extractable_chars=0, page_count=10) is True

def test_needs_ocr_false_for_rich_text_layer():
    assert needs_ocr(extractable_chars=50000, page_count=10) is False

def test_parse_result_flags_empty():
    assert ParseResult(markdown="", mode="text").is_empty is True
    assert ParseResult(markdown="# Title\n\neq $$x$$", mode="text").is_empty is False
