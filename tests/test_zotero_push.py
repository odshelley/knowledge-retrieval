import pytest

from pipeline.zotero.client import ZoteroClientError, ZoteroQuotaError, ZoteroTransientError
from pipeline.zotero.push import push_one

PAPER = {
    "id": "arxiv:2503.13804", "title": "A Preprint", "year": 2025,
    "arxiv_id": "2503.13804", "doi": None, "publication_types": [],
    "kind": "paper",
}


class StubClient:
    def __init__(self, candidates=None, created="NEWKEY", upload="uploaded",
                 raises=None, upload_raises=None, has_att=False, has_att_raises=None):
        self._candidates = candidates or []
        self._created = created
        self._upload = upload
        self._raises = raises
        self._upload_raises = upload_raises
        self._has_att = has_att
        self._has_att_raises = has_att_raises
        self.calls = []

    def search_candidates(self, title, limit=25):
        self.calls.append(("search", title))
        return self._candidates

    def create_items(self, payload):
        if self._raises:
            raise self._raises
        self.calls.append(("create", payload[0].get("itemType")))
        return [self._created if payload[0]["itemType"] != "attachment" else "ATTKEY"]

    def add_to_collection(self, item_key, collection_key):
        self.calls.append(("add_to_collection", item_key, collection_key))
        return True

    def has_attachment(self, item_key):
        if self._has_att_raises:
            raise self._has_att_raises
        self.calls.append(("has_attachment", item_key))
        return self._has_att

    def upload_attachment(self, item_key, filename, data):
        if self._upload_raises:
            raise self._upload_raises
        self.calls.append(("upload", item_key, filename, len(data)))
        return self._upload


def test_creates_item_and_uploads_when_no_match():
    c = StubClient()
    out = push_one(c, "COLL", PAPER, ["Ada Lovelace"], b"%PDF-1.4")
    assert out["pushed"] is True and out["complete"] is True
    assert out["outcome"] == "created"
    assert out["zotero_key"] == "NEWKEY"
    assert out["item_type"] == "preprint"
    assert out["filename"] == "A Preprint - Lovelace - 2025.pdf"
    assert any(call[0] == "upload" for call in c.calls)


def test_matched_item_without_an_attachment_gets_the_pdf():
    """A metadata-only item the user saved from a browser must become openable."""
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att=False)
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["outcome"] == "matched"
    assert out["zotero_key"] == "EXISTING"
    assert out["attachment"] == "uploaded"
    assert out["complete"] is True
    assert ("add_to_collection", "EXISTING", "COLL") in c.calls
    assert any(call[0] == "upload" for call in c.calls)


def test_matched_item_with_an_attachment_is_left_alone():
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att=True)
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["attachment"] == "skipped-has-attachment"
    assert out["complete"] is True
    assert not any(call[0] == "upload" for call in c.calls)


def test_matched_item_is_never_recreated():
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att=True)
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["complete"] is True
    assert not any(call[0] == "create" for call in c.calls)


def test_matched_item_transient_error_checking_attachment_leaves_record_incomplete():
    """The matched branch must fail as safely as the created branch: has_attachment can
    throttle or hit a locked library too. zotero_key is already known at that point, so
    it stays reported (retrying is idempotent, not a recreation) but complete must be
    False, or the repair query would stop revisiting an item that never got its PDF."""
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att_raises=ZoteroTransientError("locked"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["complete"] is False
    assert out["zotero_key"] == "EXISTING"


def test_matched_item_upload_failure_leaves_record_incomplete():
    """Same asymmetry pinned for the upload step of the matched branch, not just the
    created branch: an existing item's zotero_key is retained, but the record stays
    incomplete until the PDF actually lands."""
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att=False,
                    upload_raises=ZoteroTransientError("throttled"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["complete"] is False
    assert out["zotero_key"] == "EXISTING"


def test_supplied_candidates_skip_the_search_request():
    c = StubClient()
    push_one(c, "COLL", PAPER, [], b"%PDF", candidates=[])
    assert not any(call[0] == "search" for call in c.calls)


def test_supplied_nonempty_candidates_are_used_for_matching():
    """The backfill supplies a pre-fetched library index instead of one search per item;
    a match must be found from that pool with no search request issued."""
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient()
    out = push_one(c, "COLL", PAPER, [], b"%PDF", candidates=candidates)
    assert out["outcome"] == "matched"
    assert out["zotero_key"] == "EXISTING"
    assert not any(call[0] == "search" for call in c.calls)


def test_transient_error_reports_incomplete_and_does_not_raise():
    c = StubClient(raises=ZoteroTransientError("throttled"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["pushed"] is False and out["complete"] is False
    assert "throttled" in out["reason"]
    assert out["zotero_key"] is None


def test_upload_failure_leaves_the_record_incomplete():
    """zotero_key must not be committed when the PDF never landed, or the repair query
    will never revisit it and the item stays un-openable forever."""
    c = StubClient(upload_raises=ZoteroTransientError("throttled"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["zotero_key"] == "NEWKEY", "the item was created; report its key"
    assert out["complete"] is False, "but do not mark the record done"


def test_quota_exceeded_is_reported_not_raised():
    c = StubClient(upload_raises=ZoteroQuotaError("413"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["attachment"] == "quota-exceeded"
    assert out["complete"] is False


def test_quota_exceeded_creating_the_item_reports_incomplete_and_does_not_raise():
    """Distinct from a quota failure during upload (handled in _attach): here the item
    itself was never created, so there is no zotero_key to report at all — unlike the
    matched/upload-quota case where the item exists but its file does not."""
    c = StubClient(raises=ZoteroQuotaError("413"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["pushed"] is False
    assert out["complete"] is False
    assert out["zotero_key"] is None
    assert "413" in out["reason"]


def test_client_error_propagates():
    c = StubClient(raises=ZoteroClientError("HTTP 400: bad field"))
    with pytest.raises(ZoteroClientError):
        push_one(c, "COLL", PAPER, [], b"%PDF")


def test_book_records_use_the_book_item_shape():
    book = {"id": "isbn:9780521406055", "title": "Probability with Martingales",
            "year": 1991, "publisher": "CUP", "isbn": "9780521406055", "kind": "book"}
    c = StubClient()
    out = push_one(c, "BOOKS", book, ["David Williams"], b"%PDF")
    assert out["item_type"] == "book"
    assert out["filename"] == "Probability with Martingales - Williams - 1991.pdf"


def test_missing_pdf_still_creates_the_item_and_is_complete():
    c = StubClient()
    out = push_one(c, "COLL", PAPER, [], None)
    assert out["pushed"] is True and out["complete"] is True
    assert out["attachment"] == "skipped-no-pdf"


def test_exists_upload_is_reported_and_complete():
    c = StubClient(upload="exists")
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["attachment"] == "exists" and out["complete"] is True
