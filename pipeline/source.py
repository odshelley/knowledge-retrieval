"""Discover source documents. v1: a local folder. Future: same contract for cloud."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path


def source_dir() -> Path:
    return Path(os.environ["SOURCE_DIR"]).expanduser()


def list_pdf_files(root: Path) -> list[Path]:
    return [p for p in root.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]


def file_partition_key(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
