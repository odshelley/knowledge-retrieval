"""End-to-end against the real Zotero library. Creates one item, verifies it, deletes it.

Skipped unless --run-integration. Requires ZOTERO_API_KEY and ZOTERO_USER_ID.
"""
import os

import pytest

from pipeline.zotero.client import ZoteroClient
from pipeline.zotero.items import attachment_item, paper_item

pytestmark = pytest.mark.integration


@pytest.fixture
def client():
    key, user = os.environ.get("ZOTERO_API_KEY"), os.environ.get("ZOTERO_USER_ID")
    if not key or not user:
        pytest.skip("ZOTERO_API_KEY / ZOTERO_USER_ID not set")
    return ZoteroClient(api_key=key, user_id=user)


def test_collections_bootstrap_is_idempotent(client):
    first = client.ensure_collections()
    second = client.ensure_collections()
    assert first == second
    assert first["papers"] and first["books"]


def test_create_attach_and_delete_roundtrip(client):
    collections = client.ensure_collections()
    paper = {"title": "ZZZ Alethograph Integration Test — safe to delete",
             "year": 2026, "arxiv_id": "0000.00001", "doi": None,
             "publication_types": []}
    key = client.create_items([paper_item(paper, ["Test Author"], collections["papers"])])[0]
    try:
        fetched = client.get_item(key)
        assert fetched["data"]["itemType"] == "preprint"
        assert collections["papers"] in fetched["data"]["collections"]
        assert client.has_attachment(key) is False

        att_key = client.create_items([attachment_item(key, "test.pdf")])[0]
        assert client.upload_attachment(att_key, "test.pdf", b"%PDF-1.4\n%%EOF\n") in (
            "uploaded", "exists")
        assert client.has_attachment(key) is True
    finally:
        client.request("DELETE", f"/items/{key}",
                       headers={"If-Unmodified-Since-Version":
                                str(client.get_item(key)["data"]["version"])})
