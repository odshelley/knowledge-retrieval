import pytest

from pipeline.books.outline import (
    NoStructureError, build_structure, detect_headings, structure_artifact,
)
from pipeline.books.parsing import TocEntry


TOC = [
    TocEntry(0, "Chapter 1 Levy Processes", 1),
    TocEntry(1, "1.1 Definitions", 1),
    TocEntry(1, "1.2 First Results", 2),
    TocEntry(0, "Chapter 2 Poisson Processes", 3),
    TocEntry(1, "2.1 Counting Processes", 3),
    TocEntry(1, "2.2 Compound Sums", 4),
]


def test_build_structure_two_chapters_with_page_ranges():
    chapters = build_structure(TOC, n_pages=5)
    # front matter (page 1) + 2 real chapters
    assert [c.number for c in chapters] == [0, 1, 2]
    front, ch1, ch2 = chapters
    assert front.title == "Front Matter" and front.page_start == 1 and front.page_end == 1
    assert ch1.page_start == 2 and ch1.page_end == 3
    assert ch2.page_start == 4 and ch2.page_end == 5


def test_build_structure_sections_numbers_and_ranges():
    chapters = build_structure(TOC, n_pages=5)
    ch1 = chapters[1]
    assert [(s.number, s.title) for s in ch1.sections] == [
        ("1.1", "1.1 Definitions"), ("1.2", "1.2 First Results")]
    assert ch1.sections[0].page_start == 2 and ch1.sections[0].page_end == 2
    assert ch1.sections[1].page_start == 3 and ch1.sections[1].page_end == 3


def test_build_structure_synthesizes_leading_section():
    # Chapter bookmark on page 1 but first section bookmark only on page 3.
    toc = [TocEntry(0, "Chapter 1 Alpha", 0), TocEntry(1, "1.1 Later", 2),
           TocEntry(0, "Chapter 2 Beta", 4)]
    chapters = build_structure(toc, n_pages=6)
    ch1 = chapters[0]  # no front matter: chapter 1 starts on page 1
    assert [s.number for s in ch1.sections] == ["1.0", "1.1"]
    assert ch1.sections[0].title == "Chapter 1 Alpha"
    assert ch1.sections[0].page_start == 1 and ch1.sections[0].page_end == 2


def test_build_structure_deep_levels_fold_into_sections():
    # level >= 2 (subsections) are ignored, not treated as sections.
    toc = TOC + [TocEntry(2, "1.1.1 Sub", 1)]
    chapters = build_structure(toc, n_pages=5)
    assert [s.number for s in chapters[1].sections] == ["1.1", "1.2"]


def test_build_structure_requires_two_chapters():
    with pytest.raises(NoStructureError):
        build_structure([TocEntry(0, "Only Chapter", 0)], n_pages=3)
    with pytest.raises(NoStructureError):
        build_structure([], n_pages=3)


def test_sections_sharing_a_page_clamp_page_end():
    toc = [TocEntry(0, "Chapter 1 A", 0), TocEntry(1, "1.1 X", 0), TocEntry(1, "1.2 Y", 0),
           TocEntry(0, "Chapter 2 B", 1)]
    ch1 = build_structure(toc, n_pages=2)[0]
    assert ch1.sections[0].page_end >= ch1.sections[0].page_start


def test_detect_headings_fallback_finds_chapter_lines():
    pages = ["Preface text " * 20,
             "Chapter 1 Introduction\n" + "body " * 40,
             "more body " * 40,
             "Chapter 2 Advanced Topics\n" + "body " * 40]
    toc = detect_headings(pages)
    assert [(e.level, e.page_index) for e in toc] == [(0, 1), (0, 3)]
    assert toc[0].title == "Chapter 1 Introduction"


def test_structure_artifact_shape_and_ids():
    chapters = build_structure(TOC, n_pages=5)
    art = structure_artifact("isbn:9783161484100", "f" * 64, chapters)
    assert art["book_id"] == "isbn:9783161484100"
    ch1 = art["chapters"][1]
    assert ch1["id"] == "isbn:9783161484100:ch01"
    assert ch1["key"] == "f" * 64 + ":ch01"
    assert ch1["sections"][0]["id"] == "isbn:9783161484100:ch01:s01"
    assert ch1["sections"][0]["number"] == "1.1"


def test_choose_toc_prefers_outline_falls_back_to_headings():
    from pipeline.books.outline import choose_toc
    pages = ["Chapter 1 Intro\n" + "x" * 200, "y" * 200, "Chapter 2 More\n" + "x" * 200]
    assert [e.page_index for e in choose_toc([], pages)] == [0, 2]          # fallback
    outline = [TocEntry(0, "Chapter 1 A", 0), TocEntry(0, "Chapter 2 B", 2)]
    assert choose_toc(outline, pages) == outline                            # outline wins
    one_entry = [TocEntry(0, "Chapter 1 A", 0)]
    assert [e.title for e in choose_toc(one_entry, pages)] == [
        "Chapter 1 Intro", "Chapter 2 More"]                                # thin outline → fallback
