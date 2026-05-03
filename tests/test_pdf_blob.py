from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

from pipeline.assets.pdf_blob import compute_pdf_metadata


def test_compute_pdf_metadata_returns_hash_and_size():
    body = b"%PDF-fake-bytes"
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(iter_chunks=lambda chunk_size=1 << 20: iter([body])),
        "ContentLength": len(body),
    }
    meta = compute_pdf_metadata(s3, "abc123.pdf")
    assert meta["size_bytes"] == len(body)
    assert meta["sha256"] == hashlib.sha256(body).hexdigest()
    s3.get_object.assert_called_once_with(Bucket="pdfs", Key="abc123.pdf")
