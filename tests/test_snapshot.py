from __future__ import annotations

import io
import tarfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts import snapshot_vault


@pytest.fixture
def fake_vault(tmp_path: Path) -> Path:
    (tmp_path / "Files" / "topic1").mkdir(parents=True)
    (tmp_path / "Files" / "topic1" / "paper_a.pdf").write_bytes(b"%PDF-fake-A")
    (tmp_path / "Files" / "topic1" / "paper_b.pdf").write_bytes(b"%PDF-fake-B")
    (tmp_path / "Paper_A.md").write_text("# Paper A\n\nNotes about A.")
    (tmp_path / "Paper_B.md").write_text("# Paper B\n\nNotes about B.")
    (tmp_path / "Concept_X.md").write_text("# Concept X")
    return tmp_path


def test_tarball_contains_all_pdfs_and_mds(fake_vault, tmp_path):
    out = tmp_path / "snap.tar.gz"
    snapshot_vault.write_tarball(fake_vault, out)
    with tarfile.open(out, "r:gz") as tar:
        names = sorted(tar.getnames())
    assert any(n.endswith("paper_a.pdf") for n in names)
    assert any(n.endswith("paper_b.pdf") for n in names)
    assert any(n.endswith("Paper_A.md") for n in names)
    assert any(n.endswith("Concept_X.md") for n in names)


def test_content_hash_stable_for_same_bytes(tmp_path):
    f = tmp_path / "x.pdf"
    f.write_bytes(b"hello")
    assert snapshot_vault.content_hash(f) == snapshot_vault.content_hash(f)
    assert len(snapshot_vault.content_hash(f)) == 64


def test_upload_partitioned_files_calls_put_for_each(fake_vault):
    partitions = [
        {"paper_id": "a", "pdf_path": "Files/topic1/paper_a.pdf", "md_path": "Paper_A.md"},
        {"paper_id": "b", "pdf_path": "Files/topic1/paper_b.pdf", "md_path": "Paper_B.md"},
    ]
    s3 = MagicMock()
    snapshot_vault.upload_partitioned_files(fake_vault, partitions, s3)
    assert s3.put_object.call_count == 4
    buckets = [c.kwargs["Bucket"] for c in s3.put_object.call_args_list]
    assert buckets.count("pdfs") == 2
    assert buckets.count("legacy-summaries") == 2
    pdf_keys = sorted(c.kwargs["Key"] for c in s3.put_object.call_args_list if c.kwargs["Bucket"] == "pdfs")
    assert pdf_keys == ["a.pdf", "b.pdf"]
