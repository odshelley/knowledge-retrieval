"""Book identity: isbn: > title:, mirroring compute_paper_id's doi > arxiv > title idiom."""
from __future__ import annotations

import hashlib
import re

from pipeline.graph.research_port import normalize_title

_ISBN_CHARS = re.compile(r"[^0-9Xx]")


def normalize_isbn(raw: str) -> str | None:
    """Strip separators; accept only well-formed ISBN-10/13 shapes (no checksum validation)."""
    if not raw:
        return None
    digits = _ISBN_CHARS.sub("", raw).upper()
    if len(digits) == 13 and digits.isdigit():
        return digits
    if len(digits) == 10 and digits[:9].isdigit() and (digits[9].isdigit() or digits[9] == "X"):
        return digits
    return None


def compute_book_id(isbn: str | None, title: str | None) -> str:
    norm = normalize_isbn(isbn) if isbn else None
    if norm:
        return "isbn:" + norm
    if title:
        return "title:" + normalize_title(title)
    raise ValueError("cannot form book id: no isbn/title")


def chapter_node_id(book_id: str, n: int) -> str:
    return f"{book_id}:ch{n:02d}"


def section_node_id(book_id: str, ch: int, s: int) -> str:
    return f"{book_id}:ch{ch:02d}:s{s:02d}"


def _norm_symbol(symbol_latex: str) -> str:
    return re.sub(r"\s+", "", symbol_latex.strip().lower())


def notation_node_id(section_id: str, symbol_latex: str) -> str:
    """Scoped per section, not per book: the same symbol reintroduced with a different
    meaning in a later chapter must not clobber the earlier node (meaning is a plain SET
    in WRITE_BOOK_NOTATIONS). section_id embeds the book id, so wipe_book's book-prefix
    matching and cross-book non-collision both still hold."""
    h = hashlib.sha256(_norm_symbol(symbol_latex).encode()).hexdigest()[:12]
    return f"{section_id}:not:{h}"
