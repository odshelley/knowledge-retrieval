import pytest

from pipeline.runtime.partitions import (
    BOOK_CHAPTERS_PARTITION, BOOKS_PARTITION,
    book_chapters_partitions_def, books_partitions_def,
    chapter_partition_key, split_chapter_key,
)


def test_partition_set_names_are_distinct_from_documents():
    assert BOOKS_PARTITION == "books"
    assert BOOK_CHAPTERS_PARTITION == "book_chapters"
    assert books_partitions_def().name == "books"
    assert book_chapters_partitions_def().name == "book_chapters"


def test_chapter_partition_key_round_trips():
    key = chapter_partition_key("a" * 64, 3)
    assert key == "a" * 64 + ":ch03"
    assert split_chapter_key(key) == ("a" * 64, 3)


def test_chapter_keys_sort_in_chapter_order():
    keys = [chapter_partition_key("a" * 64, n) for n in (10, 2, 1)]
    assert sorted(keys) == [chapter_partition_key("a" * 64, n) for n in (1, 2, 10)]


def test_split_chapter_key_rejects_malformed():
    with pytest.raises(ValueError):
        split_chapter_key("nochapterhere")


def test_books_source_dir_requires_env(monkeypatch):
    from pipeline.ingest.source import books_source_dir
    monkeypatch.delenv("BOOKS_SOURCE_DIR", raising=False)
    with pytest.raises(RuntimeError):
        books_source_dir()
    monkeypatch.setenv("BOOKS_SOURCE_DIR", "/tmp/books")
    assert str(books_source_dir()) == "/tmp/books"
