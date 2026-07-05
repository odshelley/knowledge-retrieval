import pytest

from pipeline.books.identity import (
    chapter_node_id, compute_book_id, normalize_isbn, section_node_id,
)


def test_normalize_isbn_strips_hyphens_and_spaces():
    assert normalize_isbn("978-3-16-148410-0") == "9783161484100"
    assert normalize_isbn("978 3 16 148410 0") == "9783161484100"


def test_normalize_isbn_accepts_isbn10_with_check_x():
    assert normalize_isbn("0-8044-2957-X") == "080442957X"


def test_normalize_isbn_rejects_wrong_length_or_garbage():
    assert normalize_isbn("1234") is None
    assert normalize_isbn("not an isbn") is None
    assert normalize_isbn("") is None


def test_compute_book_id_prefers_isbn_over_title():
    assert compute_book_id("978-3-16-148410-0", "Some Title") == "isbn:9783161484100"


def test_compute_book_id_falls_back_to_normalized_title():
    assert compute_book_id(None, "  Financial   Modelling ") == "title:financial modelling"
    assert compute_book_id("garbage", "Financial Modelling") == "title:financial modelling"


def test_compute_book_id_raises_without_either():
    with pytest.raises(ValueError):
        compute_book_id(None, None)


def test_chapter_and_section_node_ids_zero_pad():
    assert chapter_node_id("isbn:9783161484100", 3) == "isbn:9783161484100:ch03"
    assert section_node_id("isbn:9783161484100", 3, 2) == "isbn:9783161484100:ch03:s02"
