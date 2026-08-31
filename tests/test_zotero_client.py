import json

import pytest
import requests

from pipeline.zotero.client import ZoteroClient, ZoteroClientError, ZoteroTransientError


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else []
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json


class BadJSONResponse(FakeResponse):
    """A 200 whose body is not valid JSON — an intermediary's HTML error page, or a
    body truncated below the point the decoder can even start."""

    def json(self):
        raise json.JSONDecodeError("Expecting value", "<html>Bad Gateway</html>", 0)


class FakeHTTP:
    """Records requests and replays queued responses, so tests assert on call shape."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def client(http):
    return ZoteroClient(api_key="KEY", user_id="5666366", http=http)


def test_auth_header_is_sent_and_key_is_not_in_the_url():
    http = FakeHTTP([FakeResponse(json_data=[])])
    c = client(http)
    c.list_collections()
    call = http.calls[0]
    assert call["headers"]["Zotero-API-Key"] == "KEY"
    assert call["headers"]["Zotero-API-Version"] == "3"
    assert "KEY" not in call["url"]


def test_ensure_collections_creates_all_three_when_library_is_empty():
    http = FakeHTTP([
        FakeResponse(json_data=[]),                                        # list
        FakeResponse(json_data={"successful": {"0": {"key": "ALEPH"}}}),   # create root
        FakeResponse(json_data={"successful": {"0": {"key": "PAP"},
                                               "1": {"key": "BKS"}}}),    # create children
    ])
    assert client(http).ensure_collections() == {"papers": "PAP", "books": "BKS"}


def test_ensure_collections_is_idempotent_when_all_exist():
    existing = [
        {"key": "ALEPH", "data": {"name": "Alethograph", "parentCollection": False}},
        {"key": "PAP", "data": {"name": "Papers", "parentCollection": "ALEPH"}},
        {"key": "BKS", "data": {"name": "Books", "parentCollection": "ALEPH"}},
        {"key": "OTHER", "data": {"name": "Papers", "parentCollection": "SOMEWHERE"}},
    ]
    http = FakeHTTP([FakeResponse(json_data=existing)])
    c = client(http)
    assert c.ensure_collections() == {"papers": "PAP", "books": "BKS"}
    assert len(http.calls) == 1, "nothing should be created when all three exist"


def test_ensure_collections_creates_only_the_missing_child():
    existing = [
        {"key": "ALEPH", "data": {"name": "Alethograph", "parentCollection": False}},
        {"key": "PAP", "data": {"name": "Papers", "parentCollection": "ALEPH"}},
    ]
    http = FakeHTTP([
        FakeResponse(json_data=existing),
        FakeResponse(json_data={"successful": {"0": {"key": "BKS"}}}),
    ])
    c = client(http)
    assert c.ensure_collections() == {"papers": "PAP", "books": "BKS"}
    assert http.calls[1]["json"] == [{"name": "Books", "parentCollection": "ALEPH"}]


def test_ensure_collections_ignores_same_named_collection_under_another_parent():
    existing = [
        {"key": "ALEPH", "data": {"name": "Alethograph", "parentCollection": False}},
        {"key": "STRAY", "data": {"name": "Papers", "parentCollection": "UNRELATED"}},
    ]
    http = FakeHTTP([
        FakeResponse(json_data=existing),
        FakeResponse(json_data={"successful": {"0": {"key": "PAP"}, "1": {"key": "BKS"}}}),
    ])
    assert client(http).ensure_collections() == {"papers": "PAP", "books": "BKS"}


def test_list_collections_paginates_until_short_page():
    page1 = [{"key": f"K{i}", "data": {"name": str(i), "parentCollection": False}}
             for i in range(100)]
    page2 = [{"key": "LAST", "data": {"name": "last", "parentCollection": False}}]
    http = FakeHTTP([FakeResponse(json_data=page1), FakeResponse(json_data=page2)])
    assert len(client(http).list_collections()) == 101
    assert http.calls[1]["params"]["start"] == 100


def test_client_error_raises():
    http = FakeHTTP([FakeResponse(status_code=403, text="Forbidden")])
    with pytest.raises(ZoteroClientError):
        client(http).list_collections()


@pytest.mark.parametrize("status", [429, 409, 500, 503])
def test_retryable_statuses_retry_then_raise_transient(monkeypatch, status):
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: None)
    http = FakeHTTP([FakeResponse(status_code=status, headers={"Retry-After": "1"})
                     for _ in range(4)])
    with pytest.raises(ZoteroTransientError):
        client(http).list_collections()


def test_429_then_success_succeeds(monkeypatch):
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: None)
    http = FakeHTTP([
        FakeResponse(status_code=429, headers={"Retry-After": "1"}),
        FakeResponse(json_data=[]),
    ])
    assert client(http).list_collections() == []


def test_backoff_header_delays_the_next_request(monkeypatch):
    slept = []
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: slept.append(s))
    http = FakeHTTP([
        FakeResponse(json_data=[], headers={"Backoff": "3"}),
        FakeResponse(json_data=[]),
    ])
    c = client(http)
    c.list_collections()
    c.list_collections()
    # Elapsed monotonic time makes the wait fractionally under 3 — assert a range, never
    # exact equality.
    assert any(2 < s <= 3 for s in slept)


def test_non_numeric_retry_after_falls_back_to_normal_backoff(monkeypatch):
    """RFC 9110 permits Retry-After as an HTTP-date; Zotero sits behind a CDN that can
    inject one. float() would crash — the client must degrade to the normal backoff."""
    slept = []
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: slept.append(s))
    http = FakeHTTP([
        FakeResponse(status_code=429,
                     headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}),
        FakeResponse(json_data=[]),
    ])
    assert client(http).list_collections() == []
    assert slept == [2.0]


def test_non_numeric_backoff_header_is_ignored(monkeypatch):
    slept = []
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: slept.append(s))
    http = FakeHTTP([
        FakeResponse(json_data=[], headers={"Backoff": "soon"}),
        FakeResponse(json_data=[]),
    ])
    c = client(http)
    c.list_collections()
    c.list_collections()
    assert slept == [], "a garbage Backoff header must not impose an artificial delay"


def test_create_collections_raises_when_zotero_reports_a_failure():
    http = FakeHTTP([FakeResponse(json_data={
        "successful": {}, "failed": {"0": {"code": 400, "message": "bad name"}}})])
    with pytest.raises(ZoteroClientError, match="bad name"):
        client(http).create_collections([{"name": "X"}])


def test_malformed_json_body_raises_transient_not_a_raw_decode_error():
    """A response that clears request()'s status gate can still carry a non-JSON body.
    That must not escape as a raw json.JSONDecodeError / ValueError."""
    http = FakeHTTP([BadJSONResponse()])
    with pytest.raises(ZoteroTransientError):
        client(http).list_collections()


def test_request_exception_exhausts_retries_and_raises_transient(monkeypatch):
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: None)

    def always_raises(method, url, **kwargs):
        raise requests.RequestException("boom")

    with pytest.raises(ZoteroTransientError):
        ZoteroClient(api_key="KEY", user_id="5666366", http=always_raises).list_collections()


def test_create_items_returns_keys_in_submission_order():
    http = FakeHTTP([FakeResponse(json_data={
        "successful": {"0": {"key": "A"}, "1": {"key": "B"}}, "failed": {}})])
    assert client(http).create_items([{"itemType": "preprint"},
                                      {"itemType": "book"}]) == ["A", "B"]


def test_create_items_raises_when_zotero_reports_a_failure():
    http = FakeHTTP([FakeResponse(json_data={
        "successful": {}, "failed": {"0": {"code": 400, "message": "bad field"}}})])
    with pytest.raises(ZoteroClientError, match="bad field"):
        client(http).create_items([{"itemType": "nonsense"}])


def test_create_items_rejects_batches_over_the_limit():
    with pytest.raises(ValueError, match="50"):
        client(FakeHTTP([])).create_items([{"itemType": "preprint"}] * 51)


def test_has_attachment_detects_a_child_attachment():
    http = FakeHTTP([FakeResponse(json_data=[{"key": "ATT", "data": {
        "itemType": "attachment", "linkMode": "imported_file"}}])])
    assert client(http).has_attachment("ITEM") is True


def test_has_attachment_false_for_a_metadata_only_item():
    http = FakeHTTP([FakeResponse(json_data=[])])
    assert client(http).has_attachment("ITEM") is False


def test_add_to_collection_patches_only_the_collections_field():
    http = FakeHTTP([
        FakeResponse(json_data={"data": {"key": "ITEM", "version": 7,
                                         "collections": ["OLD"], "title": "Keep me"}}),
        FakeResponse(status_code=204),
    ])
    assert client(http).add_to_collection("ITEM", "NEW") is True
    patch = http.calls[1]
    assert patch["method"] == "PATCH"
    assert patch["json"] == {"collections": ["OLD", "NEW"]}
    assert "title" not in patch["json"], "must not rewrite the user's own metadata"
    assert patch["headers"]["If-Unmodified-Since-Version"] == "7"


def test_add_to_collection_is_a_noop_when_already_a_member():
    http = FakeHTTP([FakeResponse(json_data={
        "data": {"key": "ITEM", "version": 7, "collections": ["NEW"]}})])
    assert client(http).add_to_collection("ITEM", "NEW") is False
    assert len(http.calls) == 1


def test_upload_attachment_short_circuits_when_file_exists():
    http = FakeHTTP([FakeResponse(json_data={"exists": 1})])
    assert client(http).upload_attachment("ITEM", "f.pdf", b"data") == "exists"
    assert len(http.calls) == 1


def test_upload_attachment_runs_all_three_steps():
    http = FakeHTTP([
        FakeResponse(json_data={"url": "https://s3.example/put", "contentType": "text/plain",
                                "prefix": "PRE", "suffix": "SUF", "uploadKey": "UK"}),
        FakeResponse(status_code=201),
        FakeResponse(status_code=204),
    ])
    assert client(http).upload_attachment("ITEM", "f.pdf", b"data") == "uploaded"
    assert len(http.calls) == 3
    assert http.calls[1]["data"] == b"PREdataSUF"
    assert http.calls[2]["data"]["upload"] == "UK"


def test_upload_never_sends_the_api_key_to_the_storage_host():
    """Step 2 targets Zotero's S3 backend, not the API. The key must not leave our host."""
    http = FakeHTTP([
        FakeResponse(json_data={"url": "https://s3.example/put", "contentType": "text/plain",
                                "prefix": "PRE", "suffix": "SUF", "uploadKey": "UK"}),
        FakeResponse(status_code=201),
        FakeResponse(status_code=204),
    ])
    client(http).upload_attachment("ITEM", "f.pdf", b"data")
    assert "Zotero-API-Key" not in http.calls[1]["headers"]
    assert "Zotero-API-Key" in http.calls[0]["headers"], "the API call still authenticates"


def test_upload_authorization_sends_md5_filesize_and_millisecond_mtime():
    import hashlib
    http = FakeHTTP([FakeResponse(json_data={"exists": 1})])
    client(http).upload_attachment("ITEM", "f.pdf", b"data")
    sent = http.calls[0]["data"]
    assert sent["md5"] == hashlib.md5(b"data").hexdigest()
    assert sent["filesize"] == 4
    assert sent["filename"] == "f.pdf"
    assert sent["mtime"] > 10**12, "mtime must be milliseconds, not seconds"
    assert http.calls[0]["headers"]["If-None-Match"] == "*"


def test_quota_exceeded_raises_the_specific_error():
    from pipeline.zotero.client import ZoteroQuotaError
    http = FakeHTTP([FakeResponse(status_code=413, text="quota")])
    with pytest.raises(ZoteroQuotaError):
        client(http).upload_attachment("ITEM", "f.pdf", b"data")


def test_search_candidates_returns_empty_without_a_title():
    http = FakeHTTP([])
    assert client(http).search_candidates(None) == []
    assert http.calls == []
