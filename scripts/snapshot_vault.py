"""One-shot: tarball the vault to MinIO and upload per-paper PDFs + v1 mds.

Idempotent: per-file uploads are keyed by paper_id so re-running overwrites in place.
The tarball is keyed by date — re-running on the same day overwrites that day's snapshot.

Usage:
    uv run python scripts/snapshot_vault.py
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
import tarfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from pipeline.resources import minio_from_env

load_dotenv()

VAULT = Path(os.environ["ALETHOGRAPH_VAULT_PATH"]) if os.environ.get("ALETHOGRAPH_VAULT_PATH") else None
PARTITIONS_FILE = Path("data/partitions.json")


def content_hash(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_tarball(vault: Path, out: Path) -> None:
    """Tar the entire vault (every .pdf + .md) to a gzipped archive."""
    out.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(out, "w:gz") as tar:
        for ext in (".pdf", ".md"):
            for f in vault.rglob(f"*{ext}"):
                tar.add(f, arcname=str(f.relative_to(vault)))


def upload_tarball(s3, tar_path: Path) -> str:
    today = dt.date.today().isoformat()
    key = f"{today}-initial.tar.gz"
    with tar_path.open("rb") as f:
        s3.put_object(Bucket="vault-snapshots", Key=key, Body=f)
    return key


def upload_partitioned_files(vault: Path, partitions: list[dict], s3) -> dict[str, str]:
    """Upload each paper's PDF + v1 md keyed by paper_id. Returns {paper_id: pdf_hash}."""
    hashes: dict[str, str] = {}
    for part in partitions:
        pdf = vault / part["pdf_path"]
        md = vault / part["md_path"] if part.get("md_path") else None
        if not pdf.exists():
            print(f"skip {part['paper_id']}: pdf missing at {pdf}")
            continue
        with pdf.open("rb") as f:
            s3.put_object(
                Bucket="pdfs",
                Key=f"{part['paper_id']}.pdf",
                Body=f,
                Metadata={"sha256": content_hash(pdf)},
            )
        hashes[part["paper_id"]] = content_hash(pdf)
        if md is not None and md.exists():
            with md.open("rb") as f:
                s3.put_object(
                    Bucket="legacy-summaries",
                    Key=f"{part['paper_id']}.md",
                    Body=f,
                    Metadata={"sha256": content_hash(md)},
                )
    return hashes


def main() -> None:
    if VAULT is None or not VAULT.exists():
        raise SystemExit(f"vault path missing or invalid: {VAULT}")
    if not PARTITIONS_FILE.exists():
        raise SystemExit("data/partitions.json missing — run scripts/discover_partitions.py first")

    s3 = minio_from_env().get_client()
    partitions = json.loads(PARTITIONS_FILE.read_text())

    cache_dir = Path("data/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    tar_path = cache_dir / "vault.tar.gz"
    print(f"tarring {VAULT} → {tar_path}")
    write_tarball(VAULT, tar_path)
    key = upload_tarball(s3, tar_path)
    print(f"tarball uploaded: vault-snapshots/{key}")

    print(f"uploading {len(partitions)} paper PDFs + v1 mds")
    hashes = upload_partitioned_files(VAULT, partitions, s3)
    print(f"uploaded {len(hashes)} PDFs + matching mds")


if __name__ == "__main__":
    main()
