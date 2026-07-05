from pipeline.books.chunking import ChunkSpan, section_chunk_rows, split_pages


def test_split_pages_single_short_page_one_chunk():
    spans = split_pages([(3, "Only paragraph.")])
    assert spans == [ChunkSpan(text="Only paragraph.", page_start=3, page_end=3)]


def test_split_pages_tracks_page_range_across_pages():
    p1 = "para one. " * 30      # ~300 chars
    p2 = "para two. " * 30
    spans = split_pages([(1, p1), (2, p2)], target=10_000)
    assert len(spans) == 1
    assert spans[0].page_start == 1 and spans[0].page_end == 2


def test_split_pages_overflow_splits_and_pages_attributed():
    pages = [(n, f"page {n} sentence. " * 40) for n in (1, 2, 3, 4)]  # ~720 chars each
    spans = split_pages(pages, target=1500, overlap=0)
    assert len(spans) >= 2
    assert spans[0].page_start == 1
    assert spans[-1].page_end == 4
    for s in spans:
        assert s.page_start <= s.page_end


def test_split_pages_keeps_math_block_atomic():
    math = "$$\n" + "x = y\n" * 50 + "$$"          # oversized display block
    spans = split_pages([(1, "before.\n\n" + math + "\n\nafter.")], target=100, overlap=0)
    assert any(s.text == math for s in spans)       # never split


def test_split_pages_overlap_carries_trailing_segment_and_its_page():
    seg_a = "alpha. " * 20                            # ~140 chars, page 1
    seg_b = "beta. " * 20                             # page 2
    seg_c = "gamma. " * 20                            # page 3
    spans = split_pages([(1, seg_a), (2, seg_b), (3, seg_c)], target=300, overlap=150)
    assert len(spans) == 2
    assert spans[1].page_start == 2                   # overlap re-seeds from page 2's segment


def test_section_chunk_rows_ids_and_metadata():
    chapter = {"key": "f" * 64 + ":ch01", "number": 1}
    section = {"id": "isbn:x:ch01:s01", "number": "1.1", "title": "1.1 Defs",
               "page_start": 2, "page_end": 2}
    pages = ["front", "Definition 1.1 text here. " * 10, "later page"]
    rows = section_chunk_rows("f" * 64, chapter, section, pages)
    assert rows[0]["id"] == "f" * 64 + ":ch01:s01:0"
    assert rows[0]["chapter_key"] == "f" * 64 + ":ch01"
    assert rows[0]["section_id"] == "isbn:x:ch01:s01"
    assert rows[0]["page_start"] == 2 and rows[0]["page_end"] == 2
    assert "Definition 1.1" in rows[0]["text"]
