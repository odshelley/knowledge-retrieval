"""Tests for the book_zotero_push asset — mirrors test_zotero_push_asset.py.

Same load-bearing contract as the paper asset: zotero_key is written back to the Book
node only when push_one reports complete=True, and a Zotero outage (ensure_collections
raising) must be a clean no-op, never a failed run.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call

import botocore.exceptions
import pytest
from dagster import build_asset_context

import pipeline.assets.book_zotero_push as bzpa
from pipeline.zotero.client import ZoteroClientError, ZoteroTransientError

BOOK_NODE = {"id": "isbn:9780521406055", "title": "Probability with Martingales", "year": 1991,
             "publisher": "CUP", "edition": None, "isbn": "9780521406055", "zotero_key": None}


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


def test_noop_when_not_configured(monkeypatch):
    monkeypatch.setattr(bzpa, "fetch_pdf", lambda *a, **k: None)
    context, session, zotero = _ctx(configured=False)
    result = bzpa.book_zotero_push(context)
    assert result.metadata["pushed"] is False
    assert "not configured" in result.metadata["reason"]
    session.run.assert_not_called()


def test_noop_when_no_book_node(monkeypatch):
    monkeypatch.setattr(bzpa, "fetch_pdf", lambda *a, **k: None)
    client = MagicMock()
    context, session, zotero = _ctx(row=None, client=client)
    result = bzpa.book_zotero_push(context)
    assert result.metadata["pushed"] is False
    assert "no Book node" in result.metadata["reason"]
    zotero.get_client.assert_not_called()


def test_noop_when_already_pushed(monkeypatch):
    monkeypatch.setattr(bzpa, "fetch_pdf", lambda *a, **k: None)
    row = {"node": {**BOOK_NODE, "zotero_key": "EXISTING"}, "authors": ["David Williams"]}
    client = MagicMock()
    context, session, zotero = _ctx(row=row, client=client)
    result = bzpa.book_zotero_push(context)
    assert result.metadata["pushed"] is False
    assert "already in Zotero" in result.metadata["reason"]
    assert result.metadata["zotero_key"] == "EXISTING"
    zotero.get_client.assert_not_called()


@pytest.mark.parametrize("exc", [ZoteroClientError("revoked"), ZoteroTransientError("throttled")])
def test_ensure_collections_failure_is_a_clean_noop(monkeypatch, exc):
    monkeypatch.setattr(bzpa, "fetch_pdf", lambda *a, **k: None)
    row = {"node": dict(BOOK_NODE), "authors": ["David Williams"]}
    client = MagicMock()
    client.ensure_collections.side_effect = exc
    context, session, zotero = _ctx(row=row, client=client)

    result = bzpa.book_zotero_push(context)  # must not raise

    assert result.metadata["pushed"] is False
    assert "Zotero unavailable" in result.metadata["reason"]
    assert session.run.call_count == 1


def test_writes_zotero_key_only_when_complete(monkeypatch):
    row = {"node": dict(BOOK_NODE), "authors": ["David Williams"]}
    client = MagicMock()
    client.ensure_collections.return_value = {"papers": "PAPERS_COLL", "books": "BOOKS_COLL"}
    context, session, zotero = _ctx(row=row, client=client)
    monkeypatch.setattr(bzpa, "fetch_pdf", lambda *a, **k: b"%PDF")

    out = {"pushed": True, "complete": True, "outcome": "created", "zotero_key": "ZKEY",
           "item_type": "book", "filename": "PwM - Williams - 1991.pdf",
           "attachment": "uploaded", "reason": None}
    monkeypatch.setattr(bzpa.zp, "push_one", lambda *a, **k: out)

    result = bzpa.book_zotero_push(context)

    assert session.run.call_count == 2
    write_call = session.run.call_args_list[1]
    assert write_call == call(bzpa.zp.MARK_BOOK_PUSHED, id=BOOK_NODE["id"], key="ZKEY")
    assert result.metadata["zotero_key"] == "ZKEY"
    assert result.metadata["complete"] is True


def test_does_not_write_zotero_key_when_incomplete_even_with_a_real_key(monkeypatch):
    row = {"node": dict(BOOK_NODE), "authors": ["David Williams"]}
    client = MagicMock()
    client.ensure_collections.return_value = {"papers": "PAPERS_COLL", "books": "BOOKS_COLL"}
    context, session, zotero = _ctx(row=row, client=client)
    monkeypatch.setattr(bzpa, "fetch_pdf", lambda *a, **k: b"%PDF")

    out = {"pushed": True, "complete": False, "outcome": "matched", "zotero_key": "EXISTING",
           "item_type": None, "filename": "PwM - Williams - 1991.pdf",
           "attachment": None, "reason": "throttled"}
    monkeypatch.setattr(bzpa.zp, "push_one", lambda *a, **k: out)

    result = bzpa.book_zotero_push(context)

    assert session.run.call_count == 1
    assert result.metadata["zotero_key"] == "EXISTING"
    assert result.metadata["complete"] is False


def test_transient_pdf_fetch_failure_is_a_clean_noop(monkeypatch):
    """A ClientError out of fetch_pdf (throttle, connection reset, a 500 -- anything that
    is not a genuine 404/NoSuchKey/NotFound) must not be treated as "no PDF". If it were,
    push_one would return complete=True and the caller would write zotero_key, stranding
    the record without its attachment permanently -- the repair query only revisits
    zotero_key IS NULL rows."""
    row = {"node": dict(BOOK_NODE), "authors": ["David Williams"]}
    client = MagicMock()
    client.ensure_collections.return_value = {"papers": "PAPERS_COLL", "books": "BOOKS_COLL"}
    context, session, zotero = _ctx(row=row, client=client)
    exc = botocore.exceptions.ClientError({"Error": {"Code": "500"}}, "GetObject")
    monkeypatch.setattr(bzpa, "fetch_pdf", MagicMock(side_effect=exc))
    push_one = MagicMock()
    monkeypatch.setattr(bzpa.zp, "push_one", push_one)

    result = bzpa.book_zotero_push(context)  # must not raise

    assert result.metadata["pushed"] is False
    assert "PDF fetch failed" in result.metadata["reason"]
    push_one.assert_not_called()
    # Only the read query ran -- no MARK_BOOK_PUSHED write.
    assert session.run.call_count == 1
