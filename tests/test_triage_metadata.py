"""Tests for the triage_metadata asset — quarantine paths."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from dagster import build_asset_context

import pipeline.assets.triage_metadata as tm_mod
from pipeline.assets.parsed_document import QuarantineError
from pipeline.assets.triage_metadata import triage_metadata


def _ctx() -> object:
    """Build a minimal asset context; neo4j_new and openai are present but not reached
    for the two quarantine paths (both raise before the neo4j dup-check)."""
    minio = MagicMock()
    s3 = minio.get_client.return_value
    s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b"# Some Title\n\ntext"))
    }
    return build_asset_context(
        partition_key="deadbeef",
        resources={
            "minio": minio,
            "neo4j_new": MagicMock(),
            "openai": MagicMock(),
        },
    )


def test_triage_quarantines_non_paper(monkeypatch):
    monkeypatch.setattr(tm_mod, "_extract_frontmatter", lambda *a, **k: {"is_paper": False})
    with pytest.raises(QuarantineError):
        triage_metadata(_ctx())


def test_triage_quarantines_malformed_frontmatter_json(monkeypatch):
    def boom(*a, **k):
        raise json.JSONDecodeError("bad", "", 0)

    monkeypatch.setattr(tm_mod, "_extract_frontmatter", boom)
    with pytest.raises(QuarantineError):
        triage_metadata(_ctx())
