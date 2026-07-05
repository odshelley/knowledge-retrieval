"""Page-aware section chunker. Same equation-atomic packing as ingest.chunking.split_markdown,
but each segment carries its page number so every chunk gets a page range."""
from __future__ import annotations

from dataclasses import dataclass

from pipeline.ingest.chunking import _segments


@dataclass
class ChunkSpan:
    text: str
    page_start: int
    page_end: int


def _pack(items: list[tuple[int, str]]) -> ChunkSpan:
    return ChunkSpan(text="\n\n".join(seg for _, seg in items),
                     page_start=min(p for p, _ in items),
                     page_end=max(p for p, _ in items))


def _take_overlap_pairs(items: list[tuple[int, str]], budget: int) -> tuple[list[tuple[int, str]], int]:
    tail: list[tuple[int, str]] = []
    total = 0
    for page, seg in reversed(items):
        if total + len(seg) > budget:
            break
        tail.insert(0, (page, seg))
        total += len(seg)
    return tail, total


def split_pages(pages: list[tuple[int, str]], target: int = 4000,
                overlap: int = 600) -> list[ChunkSpan]:
    """`pages` = [(1-based page_no, page_text), ...] in order. Mirrors split_markdown's
    accumulate/flush/overlap loop over (page, segment) pairs."""
    tagged: list[tuple[int, str]] = []
    for page_no, text in pages:
        tagged.extend((page_no, seg) for seg in _segments(text))

    chunks: list[ChunkSpan] = []
    cur: list[tuple[int, str]] = []
    cur_len = 0
    for pair in tagged:
        if cur and cur_len + len(pair[1]) > target:
            chunks.append(_pack(cur))
            cur, cur_len = _take_overlap_pairs(cur, overlap)
        cur.append(pair)
        cur_len += len(pair[1])
    if cur:
        chunks.append(_pack(cur))
    return chunks


def section_chunk_rows(sha: str, chapter: dict, section: dict, pages: list[str]) -> list[dict]:
    """Chunk one section (artifact dicts from structure_artifact; `pages` is the full
    0-indexed page-text list). Embeddings are added by the asset afterwards."""
    page_pairs = [(n, pages[n - 1]) for n in range(section["page_start"],
                                                   min(section["page_end"], len(pages)) + 1)]
    s_suffix = section["id"].rsplit(":", 1)[-1]  # "s01"
    return [{
        "id": f"{chapter['key']}:{s_suffix}:{i}",
        "chapter_key": chapter["key"],
        "section_id": section["id"],
        "position": i,
        "text": span.text,
        "page_start": span.page_start,
        "page_end": span.page_end,
    } for i, span in enumerate(split_pages(page_pairs))]
