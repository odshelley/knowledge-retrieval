"""Outline (or heading-fallback) → Chapter/Section tree with 1-based inclusive page ranges.

Level-0 bookmarks are chapters, level-1 are sections, deeper levels are ignored. Pages
before the first chapter become chapter 0 "Front Matter"; chapter content before its first
section becomes a synthetic section "{n}.0". A node's page_end is the next sibling's
page_start - 1 (clamped to >= page_start; last node runs to the end of its parent range).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from pipeline.books.identity import chapter_node_id, section_node_id
from pipeline.books.parsing import TocEntry


class NoStructureError(Exception):
    """Raised when neither the outline nor heading detection yields >= 2 chapters."""


@dataclass
class SectionNode:
    number: str
    title: str
    page_start: int
    page_end: int


@dataclass
class ChapterNode:
    number: int
    title: str
    page_start: int
    page_end: int
    sections: list[SectionNode] = field(default_factory=list)


_CHAPTER_HEADING = re.compile(r"^\s*(?:Chapter|CHAPTER)\s+(\d+|[IVXLC]+)\b.*$", re.MULTILINE)
_SECTION_NUMBER = re.compile(r"^(\d+(?:\.\d+)+)\b")


def detect_headings(pages: list[str]) -> list[TocEntry]:
    """Fallback when the PDF has no outline: 'Chapter N ...' at the top of a page (first
    400 chars) marks a chapter start. Sections are not recoverable reliably — chapters only."""
    toc = []
    for i, text in enumerate(pages):
        m = _CHAPTER_HEADING.search(text[:400])
        if m:
            toc.append(TocEntry(level=0, title=m.group(0).strip(), page_index=i))
    return toc


def _section_number(title: str, chapter_no: int, ordinal: int) -> str:
    m = _SECTION_NUMBER.match(title)
    return m.group(1) if m else f"{chapter_no}.{ordinal}"


def build_structure(toc: list[TocEntry], n_pages: int) -> list[ChapterNode]:
    chapter_entries = [e for e in toc if e.level == 0]
    if len(chapter_entries) < 2:
        raise NoStructureError(
            f"only {len(chapter_entries)} chapter-level outline entries — need >= 2")

    chapters: list[ChapterNode] = []
    if chapter_entries[0].page_index > 0:
        fm_end = chapter_entries[0].page_index  # 1-based end = 0-based start of ch1
        chapters.append(ChapterNode(
            number=0, title="Front Matter", page_start=1, page_end=fm_end,
            sections=[SectionNode("0.0", "Front Matter", 1, fm_end)]))

    for n, entry in enumerate(chapter_entries, start=1):
        page_start = entry.page_index + 1
        if n < len(chapter_entries):
            page_end = max(page_start, chapter_entries[n].page_index)  # next ch's 0-based
            # start, read as a 1-based page number, IS the previous page
        else:
            page_end = max(page_start, n_pages)
        ch = ChapterNode(number=n, title=entry.title, page_start=page_start, page_end=page_end)

        # section entries belonging to this chapter: level-1 entries positionally between
        # this chapter entry and the next chapter entry in the ORIGINAL toc order
        i0 = toc.index(entry)
        i1 = toc.index(chapter_entries[n]) if n < len(chapter_entries) else len(toc)
        sec_entries = [e for e in toc[i0 + 1:i1] if e.level == 1]

        secs: list[SectionNode] = []
        if not sec_entries or sec_entries[0].page_index + 1 > page_start:
            lead_end = (sec_entries[0].page_index if sec_entries else page_end)
            secs.append(SectionNode(f"{n}.0", entry.title, page_start,
                                    max(page_start, lead_end)))
        for j, se in enumerate(sec_entries):
            s_start = se.page_index + 1
            s_end = (sec_entries[j + 1].page_index if j + 1 < len(sec_entries) else page_end)
            secs.append(SectionNode(_section_number(se.title, n, j + 1), se.title,
                                    s_start, max(s_start, s_end)))
        ch.sections = secs
        chapters.append(ch)
    return chapters


def structure_artifact(book_id: str, sha: str, chapters: list[ChapterNode]) -> dict:
    out = {"book_id": book_id, "chapters": []}
    for ch in chapters:
        out["chapters"].append({
            "id": chapter_node_id(book_id, ch.number),
            "key": f"{sha}:ch{ch.number:02d}",
            "number": ch.number, "title": ch.title,
            "page_start": ch.page_start, "page_end": ch.page_end,
            "sections": [{
                "id": section_node_id(book_id, ch.number, s_i),
                "number": s.number, "title": s.title,
                "page_start": s.page_start, "page_end": s.page_end,
            } for s_i, s in enumerate(ch.sections, start=0 if ch.sections
                                      and ch.sections[0].number.endswith(".0") else 1)],
        })
    return out
