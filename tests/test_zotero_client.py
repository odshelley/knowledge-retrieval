import pytest

from pipeline.zotero.client import ZoteroClient, ZoteroClientError, ZoteroTransientError


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else []
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json


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
