import pytest

from pipeline.books.metadata import book_record, find_isbn, frontmatter_head


def test_find_isbn_hyphenated_and_labeled():
    assert find_isbn("Some Press\nISBN 978-3-16-148410-0\n2026") == "978-3-16-148410-0"
    assert find_isbn("ISBN-13: 9783161484100") == "9783161484100"


def test_find_isbn_none_when_absent():
    assert find_isbn("no identifiers here, just prose 123") is None


def test_frontmatter_head_limits_pages_and_chars():
    pages = [f"page {i} " + "x" * 3000 for i in range(20)]
    head = frontmatter_head(pages)
    assert "page 0" in head and "page 12" not in head
    assert len(head) <= 8000


def test_book_record_regex_isbn_wins_and_id_computed():
    fm = {"title": "Tiny Book", "authors": ["A. Author"], "year": 2026,
          "edition": "1st", "publisher": "Tiny Press", "isbn": None}
    pages = ["Tiny Book\nISBN 978-3-16-148410-0"]
    rec = book_record(fm, pages, document_id="f" * 64)
    assert rec["book_id"] == "isbn:9783161484100"
    assert rec["isbn"] == "9783161484100"
    assert rec["document_id"] == "f" * 64
    assert rec["authors"] == ["A. Author"]


def test_book_record_title_fallback():
    rec = book_record({"title": "Tiny Book", "authors": []}, ["no isbn"], "f" * 64)
    assert rec["book_id"] == "title:tiny book"
    assert rec["isbn"] is None


def test_book_record_raises_without_title_or_isbn():
    with pytest.raises(ValueError):
        book_record({"title": None, "authors": []}, ["nothing"], "f" * 64)
