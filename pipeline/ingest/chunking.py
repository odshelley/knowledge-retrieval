"""Equation-aware markdown chunker. A LaTeX display block is never split."""
from __future__ import annotations

import re

_MATH_BLOCK = re.compile(r"\$\$.*?\$\$|\\begin\{([^}]+)\}.*?\\end\{\1\}", re.DOTALL)


def _segments(md: str) -> list[str]:
    """Split into atomic segments: math blocks stay whole; prose splits on blank lines."""
    segments: list[str] = []
    pos = 0
    for m in _MATH_BLOCK.finditer(md):
        before = md[pos:m.start()]
        for para in re.split(r"\n\s*\n", before):
            if para.strip():
                segments.append(para.strip())
        segments.append(m.group(0))
        pos = m.end()
    for para in re.split(r"\n\s*\n", md[pos:]):
        if para.strip():
            segments.append(para.strip())
    return segments


def _take_overlap(segments: list[str], budget: int) -> tuple[list[str], int]:
    """Whole trailing segments whose combined length stays within `budget` chars.

    Walks the just-closed chunk from the end, taking complete segments (never slicing
    one) until the next would exceed `budget`. Under-shoots rather than over-shoots, so a
    final segment larger than `budget` carries nothing forward. Keeps math blocks atomic
    on the overlap side, just as packing does. Returns (segments, their total length).
    """
    tail: list[str] = []
    total = 0
    for seg in reversed(segments):
        if total + len(seg) > budget:
            break
        tail.insert(0, seg)  # prepend so the tail stays in original order
        total += len(seg)
    return tail, total


def split_markdown(md: str, target: int = 4000, overlap: int = 600) -> list[str]:
    """Accumulate atomic segments into ~target-sized chunks with overlap, never splitting math.

    Both sizes are approximate: segments are indivisible, so a chunk overruns `target` only
    by at most one segment, and the overlap is whole trailing segments within `overlap` chars.
    """
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for seg in _segments(md):
        if cur and cur_len + len(seg) > target:  # flush before this segment overflows
            chunks.append("\n\n".join(cur))
            cur, cur_len = _take_overlap(cur, overlap)  # reseed next chunk with the tail
        cur.append(seg)
        cur_len += len(seg)
    if cur:
        chunks.append("\n\n".join(cur))
    return chunks
