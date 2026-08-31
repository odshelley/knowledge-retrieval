"""Tests for the zotero_push asset.

The load-bearing contract: zotero_key is written back to the Paper node ONLY when
push_one reports complete=True, even if it also reports a real zotero_key (push_one
deliberately does this on some failure paths — see pipeline/zotero/push.py). Also
covers the two no-op paths (unconfigured, ensure_collections failure) that must never
fail the run.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call

import botocore.exceptions
import pytest
from dagster import build_asset_context

import pipeline.assets.zotero_push as zpa
from pipeline.zotero.client import ZoteroClientError, ZoteroTransientError

PAPER_NODE = {"id": "arxiv:1", "title": "T", "year": 2020, "doi": None, "arxiv_id": "1",
              "venue": None, "journal_name": None, "volume": None, "pages": None,
              "publication_types": [], "abstract": None, "zotero_key": None}


def _neo4j(row):
    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False
    session.run.return_value.single.return_value = row

    driver = MagicMock()
    driver.__enter__.return_value = driver
    driver.__exit__.return_value = False
    driver.session.return_value = session

    new = MagicMock()
    new.get_driver.return_value = driver
    new.database = "neo4j"
    return new, session


def _ctx(*, configured=True, row=None, client=None):
    new, session = _neo4j(row)
    zotero = MagicMock()
    zotero.configured = configured
    if client is not None:
        zotero.get_client.return_value = client
    context = build_asset_context(
        partition_key="deadbeef",
        resources={"minio": MagicMock(), "neo4j_new": new, "zotero": zotero},
    )
    return context, session, zotero


def test_fetch_pdf_returns_none_on_genuine_absence():
    s3 = MagicMock()
    s3.get_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "404"}}, "GetObject")
    assert zpa.fetch_pdf(s3, "k") is None


def test_fetch_pdf_reraises_on_transient_error():
    """A throttle, connection reset, or 500 is not "no PDF" -- swallowing it here is
    exactly the bug this fix closes, so it must propagate to the caller."""
    s3 = MagicMock()
    s3.get_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "500"}}, "GetObject")
    with pytest.raises(botocore.exceptions.ClientError):
        zpa.fetch_pdf(s3, "k")


def test_noop_when_not_configured(monkeypatch):
    monkeypatch.setattr(zpa, "fetch_pdf", lambda *a, **k: None)
    context, session, zotero = _ctx(configured=False)
    result = zpa.zotero_push(context)
    assert result.metadata["pushed"] is False
    assert "not configured" in result.metadata["reason"]
    session.run.assert_not_called()


def test_noop_when_no_paper_node(monkeypatch):
    monkeypatch.setattr(zpa, "fetch_pdf", lambda *a, **k: None)
    client = MagicMock()
    context, session, zotero = _ctx(row=None, client=client)
    result = zpa.zotero_push(context)
    assert result.metadata["pushed"] is False
    assert "no Paper node" in result.metadata["reason"]
    zotero.get_client.assert_not_called()


def test_noop_when_already_pushed(monkeypatch):
    monkeypatch.setattr(zpa, "fetch_pdf", lambda *a, **k: None)
    row = {"node": {**PAPER_NODE, "zotero_key": "EXISTING"}, "authors": ["Ada Lovelace"]}
    client = MagicMock()
    context, session, zotero = _ctx(row=row, client=client)
    result = zpa.zotero_push(context)
    assert result.metadata["pushed"] is False
    assert "already in Zotero" in result.metadata["reason"]
    assert result.metadata["zotero_key"] == "EXISTING"
    zotero.get_client.assert_not_called()


@pytest.mark.parametrize("exc", [ZoteroClientError("revoked"), ZoteroTransientError("throttled")])
def test_ensure_collections_failure_is_a_clean_noop(monkeypatch, exc):
    monkeypatch.setattr(zpa, "fetch_pdf", lambda *a, **k: None)
    row = {"node": dict(PAPER_NODE), "authors": ["Ada Lovelace"]}
    client = MagicMock()
    client.ensure_collections.side_effect = exc
    context, session, zotero = _ctx(row=row, client=client)

    result = zpa.zotero_push(context)  # must not raise

    assert result.metadata["pushed"] is False
    assert "Zotero unavailable" in result.metadata["reason"]
    # Only the read query ran; no write-back call was made.
    assert session.run.call_count == 1


def test_writes_zotero_key_only_when_complete(monkeypatch):
    row = {"node": dict(PAPER_NODE), "authors": ["Ada Lovelace"]}
    client = MagicMock()
    client.ensure_collections.return_value = {"papers": "PAPERS_COLL", "books": "BOOKS_COLL"}
    context, session, zotero = _ctx(row=row, client=client)
    monkeypatch.setattr(zpa, "fetch_pdf", lambda *a, **k: b"%PDF")

    out = {"pushed": True, "complete": True, "outcome": "created", "zotero_key": "ZKEY",
           "item_type": "preprint", "filename": "T - Lovelace - 2020.pdf",
           "attachment": "uploaded", "reason": None}
    monkeypatch.setattr(zpa.zp, "push_one", lambda *a, **k: out)

    result = zpa.zotero_push(context)

    assert session.run.call_count == 2
    write_call = session.run.call_args_list[1]
    assert write_call == call(zpa.zp.MARK_PAPER_PUSHED, id=PAPER_NODE["id"], key="ZKEY")
    assert result.metadata["zotero_key"] == "ZKEY"
    assert result.metadata["complete"] is True


def test_does_not_write_zotero_key_when_incomplete_even_with_a_real_key(monkeypatch):
    """push_one can report a real zotero_key alongside complete=False (e.g. matched an
    existing item but the attachment upload failed). Writing zotero_key in that case
    would permanently exclude the record from the repair query -- must not happen."""
    row = {"node": dict(PAPER_NODE), "authors": ["Ada Lovelace"]}
    client = MagicMock()
    client.ensure_collections.return_value = {"papers": "PAPERS_COLL", "books": "BOOKS_COLL"}
    context, session, zotero = _ctx(row=row, client=client)
    monkeypatch.setattr(zpa, "fetch_pdf", lambda *a, **k: b"%PDF")

    out = {"pushed": True, "complete": False, "outcome": "matched", "zotero_key": "EXISTING",
           "item_type": None, "filename": "T - Lovelace - 2020.pdf",
           "attachment": None, "reason": "throttled"}
    monkeypatch.setattr(zpa.zp, "push_one", lambda *a, **k: out)

    result = zpa.zotero_push(context)

    # Only the read query ran -- no MARK_PAPER_PUSHED write despite a non-null key.
    assert session.run.call_count == 1
    assert result.metadata["zotero_key"] == "EXISTING"
    assert result.metadata["complete"] is False


def test_transient_pdf_fetch_failure_is_a_clean_noop(monkeypatch):
    """A ClientError out of fetch_pdf (throttle, connection reset, a 500 -- anything that
    is not a genuine 404/NoSuchKey/NotFound) must not be treated as "no PDF". If it were,
    push_one would return complete=True and the caller would write zotero_key, stranding
    the record without its attachment permanently -- the repair query only revisits
    zotero_key IS NULL rows."""
    row = {"node": dict(PAPER_NODE), "authors": ["Ada Lovelace"]}
    client = MagicMock()
    client.ensure_collections.return_value = {"papers": "PAPERS_COLL", "books": "BOOKS_COLL"}
    context, session, zotero = _ctx(row=row, client=client)
    exc = botocore.exceptions.ClientError({"Error": {"Code": "500"}}, "GetObject")
    monkeypatch.setattr(zpa, "fetch_pdf", MagicMock(side_effect=exc))
    push_one = MagicMock()
    monkeypatch.setattr(zpa.zp, "push_one", push_one)

    result = zpa.zotero_push(context)  # must not raise

    assert result.metadata["pushed"] is False
    assert "PDF fetch failed" in result.metadata["reason"]
    push_one.assert_not_called()
    # Only the read query ran -- no MARK_PAPER_PUSHED write.
    assert session.run.call_count == 1
