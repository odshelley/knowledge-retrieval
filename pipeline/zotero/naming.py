"""Attachment filename formatting: "Title - Author(s) - Year.pdf".

Pure — no I/O, no Zotero types. The 200-byte cap leaves headroom under the common
255-byte filesystem limit, which matters once files land on a WebDAV/NAS backend.
"""
from __future__ import annotations

import re

MAX_FILENAME_BYTES = 200
_PATH_HOSTILE = re.compile(r'[/\\:*?"<>|\x00-\x1f]')
_WHITESPACE = re.compile(r"\s+")


def surname(name: str) -> str:
    """Last whitespace-separated token. A single-token name is returned whole."""
    parts = _WHITESPACE.sub(" ", (name or "").strip()).split(" ")
    return parts[-1] if parts and parts[0] else ""


def _sanitize(text: str) -> str:
    cleaned = _PATH_HOSTILE.sub("-", text or "")
    cleaned = _WHITESPACE.sub(" ", cleaned).strip()
    return cleaned.strip(".").strip()


def author_segment(authors: list[str]) -> str:
    """'' | 'Surname' | 'A and B' | 'A et al.' — Zotero's own citation convention.

    Sanitizes each surname BEFORE joining. Sanitizing the joined result instead would
    strip the period off "et al." (_sanitize ends in .strip(".")), which is load-bearing
    for titles but wrong here.
    """
    names = [s for s in (_sanitize(surname(a)) for a in (authors or [])) if s]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{names[0]} et al."


def attachment_filename(title: str | None, authors: list[str],
                        year: int | str | None) -> str:
    """Build the attachment filename, omitting absent segments and their separators."""
    safe_title = _sanitize(title or "") or "Untitled"
    # author_segment sanitizes its own parts — re-sanitizing here would eat "et al."
    segments = [author_segment(authors), _sanitize(str(year)) if year else ""]
    tail = "".join(f" - {s}" for s in segments if s) + ".pdf"

    budget = MAX_FILENAME_BYTES - len(tail.encode("utf-8"))
    encoded = safe_title.encode("utf-8")
    if len(encoded) > budget:
        # Cut on a byte boundary, then drop any partial trailing UTF-8 sequence.
        safe_title = encoded[:max(budget, 0)].decode("utf-8", errors="ignore").strip()
    return f"{safe_title}{tail}"
