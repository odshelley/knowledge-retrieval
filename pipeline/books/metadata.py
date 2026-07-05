"""Book frontmatter: LLM extraction over the opening pages + deterministic ISBN regex.
No Semantic Scholar / external lookup (spec §4 — v1)."""
from __future__ import annotations

import json
import re

from pipeline.books.identity import compute_book_id, normalize_isbn

BOOK_FRONTMATTER_PROMPT = (
    "You are extracting bibliographic metadata from the opening pages of a BOOK. "
    "Return strict JSON: {\"title\": str, \"authors\": [str], \"year\": int|null, "
    "\"edition\": str|null, \"publisher\": str|null, \"isbn\": str|null}. "
    "Prefer the ISBN-13 from the copyright page if present."
)

_ISBN_RE = re.compile(r"ISBN(?:-1[03])?:?\s*([0-9][0-9\- ]{8,16}[0-9Xx])")


def find_isbn(text: str) -> str | None:
    m = _ISBN_RE.search(text)
    return m.group(1).strip() if m else None


def frontmatter_head(pages: list[str], max_pages: int = 10, max_chars: int = 8000) -> str:
    return "\n\n".join(pages[:max_pages])[:max_chars]


def extract_book_frontmatter(client, model: str, head: str, timeout: float = 60.0) -> dict:
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": BOOK_FRONTMATTER_PROMPT},
                  {"role": "user", "content": head}],
        response_format={"type": "json_object"},
        timeout=timeout,
    )
    return json.loads(resp.choices[0].message.content)


def book_record(fm: dict, pages: list[str], document_id: str) -> dict:
    raw_isbn = find_isbn(frontmatter_head(pages)) or fm.get("isbn")
    isbn = normalize_isbn(raw_isbn) if raw_isbn else None
    title = fm.get("title")
    book_id = compute_book_id(isbn, title)  # raises ValueError if neither
    return {
        "book_id": book_id, "document_id": document_id, "title": title,
        "authors": [a for a in (fm.get("authors") or []) if a],
        "year": fm.get("year"), "edition": fm.get("edition"),
        "publisher": fm.get("publisher"), "isbn": isbn,
    }
