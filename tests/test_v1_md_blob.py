from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

import botocore.exceptions

from pipeline.assets.v1_md_blob import fetch_md_metadata


def test_fetch_md_metadata_when_present():
    body = b"# title\nbody"
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(iter_chunks=lambda chunk_size=1 << 20: iter([body])),
        "ContentLength": len(body),
    }
    meta = fetch_md_metadata(s3, "abc.md")
    assert meta["present"] is True
    assert meta["sha256"] == hashlib.sha256(body).hexdigest()


def test_fetch_md_metadata_when_missing():
    s3 = MagicMock()
    s3.get_object.side_effect = botocore.exceptions.ClientError(
        error_response={"Error": {"Code": "NoSuchKey"}},
        operation_name="GetObject",
    )
    meta = fetch_md_metadata(s3, "abc.md")
    assert meta == {"present": False}
