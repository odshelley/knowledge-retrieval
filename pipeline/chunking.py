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


def split_markdown(md: str, target: int = 4000, overlap: int = 600) -> list[str]:
    """Accumulate atomic segments into ~target-sized chunks with overlap, never splitting math."""
    segs = _segments(md)
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for seg in segs:
        if cur and cur_len + len(seg) > target:
            chunks.append("\n\n".join(cur))
            # build overlap from the tail of the previous chunk
            tail, tlen = [], 0
            for s in reversed(cur):
                if tlen + len(s) > overlap:
                    break
                tail.insert(0, s)
                tlen += len(s)
            cur, cur_len = list(tail), tlen
        cur.append(seg)
        cur_len += len(seg)
    if cur:
        chunks.append("\n\n".join(cur))
    return chunks
