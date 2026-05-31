"""Tests for the parsed_document asset — quarantine path."""
from unittest.mock import MagicMock

import pytest
from dagster import build_asset_context

import pipeline.assets.parsed_document as pd_mod
from pipeline.assets.parsed_document import QuarantineError, parsed_document
from pipeline.ingest.parsing import ParseResult


def _minio_with_pdf_bytes(data: bytes) -> MagicMock:
    """Return a minio resource mock whose s3 client yields *data* then stops."""
    body = MagicMock()
    # walrus-operator loop: first call returns data, subsequent calls return b""
    body.read.side_effect = [data, b""]
    body.close.return_value = None
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": body}
    minio = MagicMock()
    minio.get_client.return_value = s3
    return minio


def _ctx(minio: MagicMock):
    return build_asset_context(partition_key="deadbeef", resources={"minio": minio})


def test_parsed_document_quarantines_empty_parse(monkeypatch, tmp_path):
    minio = _minio_with_pdf_bytes(b"%PDF-1.4 fake")
    monkeypatch.setattr(pd_mod, "parse_pdf", lambda path: ParseResult(markdown="", mode="text"))
    with pytest.raises(QuarantineError):
        parsed_document(_ctx(minio))
