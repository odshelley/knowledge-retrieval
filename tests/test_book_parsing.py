from pipeline.books.parsing import parse_book_pdf


def test_parse_book_pdf_returns_one_string_per_page(book_pdf):
    parsed = parse_book_pdf(str(book_pdf))
    assert parsed.mode == "text"
    assert not parsed.is_empty
    assert len(parsed.pages) == 5
    assert "ISBN 978-3-16-148410-0" in parsed.pages[0]
    assert "Definition 1.1" in parsed.pages[1]


def test_parse_book_pdf_reads_outline_with_levels_and_pages(book_pdf):
    parsed = parse_book_pdf(str(book_pdf))
    flat = [(e.level, e.title, e.page_index) for e in parsed.toc]
    assert (0, "Chapter 1 Levy Processes", 1) in flat
    assert (1, "1.1 Definitions", 1) in flat
    assert (1, "1.2 First Results", 2) in flat
    assert (0, "Chapter 2 Poisson Processes", 3) in flat
    assert (1, "2.2 Compound Sums", 4) in flat
