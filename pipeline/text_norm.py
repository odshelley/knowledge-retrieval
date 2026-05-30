"""Shared text normalization so extraction dedup keys line up with graph_write node ids."""
from __future__ import annotations

import re


def normalize_statement(s: str) -> str:
    """Whitespace-collapse, strip, lowercase. Used for content-hash ids and dedup keys."""
    return re.sub(r"\s+", " ", s.strip().lower())
