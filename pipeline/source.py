"""Discover source documents. v1: a local folder. Future: same contract for cloud."""
from __future__ import annotations

import os
from pathlib import Path

from pipeline.partitions import hash_bytes


def source_dir() -> Path:
    return Path(os.environ["SOURCE_DIR"]).expanduser()


def list_pdf_files(root: Path) -> list[Path]:
    return [p for p in root.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]


def file_partition_key(path: Path) -> str:
    return hash_bytes(path.read_bytes())
