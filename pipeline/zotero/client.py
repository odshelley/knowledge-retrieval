"""Zotero Web API v3 transport.

Hand-rolled on `requests`, mirroring how research_port.py handles Semantic Scholar —
pyzotero would be a new dependency for a handful of endpoints. All item-shaping logic
lives in items.py; this module only moves bytes.

The API key goes in the Zotero-API-Key header, never a URL, and is withheld entirely from
absolute-URL requests, which target Zotero's third-party storage host rather than the API.
"""
from __future__ import annotations

import hashlib
import logging
import time

import requests
from requests import RequestException

log = logging.getLogger(__name__)

PAGE_SIZE = 100        # Zotero's documented maximum; larger values are silently clamped.
BATCH_LIMIT = 50       # Item/collection creation cap per request.
# 409 = target library locked, genuinely transient.
_RETRYABLE_STATUS = frozenset({409, 429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 4

COLLECTION_ROOT = "Alethograph"
COLLECTION_PAPERS = "Papers"
COLLECTION_BOOKS = "Books"

# Excludes attachments and notes. The leading "-" negates the whole OR expression, not
# just the first term — verified empirically; a web search gives the wrong answer here.
NON_FILE_ITEMS = "-attachment || note"


def _parse_delay(value, fallback: float, *, header: str) -> float:
    """Parse a Retry-After/Backoff header, falling back on anything missing or malformed.

    RFC 9110 allows Retry-After to be an HTTP-date instead of delta-seconds, and Zotero
    sits behind a CDN that can inject either form (or outright garbage) into either
    header. A missing header is normal and silent; a malformed one degrades to the same
    fallback but logs a warning, so a persistently misbehaving intermediary is visible
    instead of being silently absorbed.
    """
    if not value:
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        log.warning("Zotero sent a non-numeric %s header (%r); using %ss instead",
                    header, value, fallback)
        return fallback


class ZoteroClientError(RuntimeError):
    """A 4xx that will not resolve by retrying: bad payload, revoked key, missing item."""


class ZoteroTransientError(RuntimeError):
    """Throttling, a lock, or a 5xx that survived every retry. Callers log and continue."""


class ZoteroQuotaError(ZoteroClientError):
    """HTTP 413 — the upload would exceed the library owner's storage quota.

    Subclasses ZoteroClientError so it is never silently retried, but callers catch it
    specifically: the item was created, only its file could not be stored.
    """


class ZoteroClient:
    def __init__(self, api_key: str, user_id: str,
                 base_url: str = "https://api.zotero.org",
                 timeout: float = 60.0, http=None):
        self.api_key = api_key
        self.user_id = user_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._http = http or requests.request
        self._backoff_until = 0.0

    @property
    def prefix(self) -> str:
        return f"{self.base_url}/users/{self.user_id}"

    def _headers(self, extra: dict | None = None) -> dict:
        headers = {"Zotero-API-Key": self.api_key, "Zotero-API-Version": "3"}
        headers.update(extra or {})
        return headers

    def request(self, method: str, path: str, *, params: dict | None = None,
                json_body=None, data=None, headers: dict | None = None,
                absolute: bool = False):
        """One API call with Backoff/Retry-After handling and bounded retries.

        `absolute=True` targets a non-Zotero host (the storage backend during upload) and
        therefore sends NO credentials — the request is authorized by the signature Zotero
        embeds in the multipart prefix, and leaking the key to a third party is forbidden.
        """
        url = path if absolute else f"{self.prefix}{path}"
        for attempt in range(_MAX_ATTEMPTS):
            wait = self._backoff_until - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._backoff_until = 0.0

            sent = dict(headers or {}) if absolute else self._headers(headers)
            try:
                resp = self._http(method, url, params=params, json=json_body, data=data,
                                  headers=sent, timeout=self.timeout)
            except RequestException as exc:
                log.warning("Zotero %s %s failed: %s", method, path, exc)
                time.sleep(2.0 * (attempt + 1))
                continue

            backoff = resp.headers.get("Backoff")
            if backoff:
                self._backoff_until = time.monotonic() + _parse_delay(
                    backoff, 0.0, header="Backoff")

            if resp.status_code in _RETRYABLE_STATUS:
                delay = _parse_delay(resp.headers.get("Retry-After"),
                                     2.0 * (attempt + 1), header="Retry-After")
                log.warning("Zotero %s %s throttled (HTTP %s), sleeping %ss",
                            method, path, resp.status_code, delay)
                time.sleep(delay)
                continue
            if resp.status_code == 413:
                raise ZoteroQuotaError(
                    f"Zotero {method} {path} -> 413: storage quota exceeded")
            if resp.status_code >= 400:
                raise ZoteroClientError(
                    f"Zotero {method} {path} -> HTTP {resp.status_code}: {resp.text[:300]}")
            return resp

        raise ZoteroTransientError(
            f"Zotero {method} {path} exhausted {_MAX_ATTEMPTS} attempts")

    @staticmethod
    def _json(resp):
        """Decode a response body, translating a malformed one into ZoteroTransientError.

        A response can clear request()'s status gate (retryable statuses handled, < 400)
        and still carry a body that isn't valid JSON — an intermediary's HTML error page,
        or one truncated below the point the decoder can even start. That's ordinarily an
        intermediary hiccup, worth retrying at the caller's level, not a raw stdlib
        exception escaping the client's documented error hierarchy.
        """
        try:
            return resp.json()
        except ValueError as exc:
            raise ZoteroTransientError(f"Zotero returned a malformed JSON body: {exc}") from exc

    def _paginate(self, path: str, params: dict | None = None) -> list[dict]:
        out: list[dict] = []
        start = 0
        while True:
            page = self._json(self.request("GET", path,
                              params={**(params or {}), "limit": PAGE_SIZE,
                                      "start": start, "format": "json"}))
            out.extend(page)
            if len(page) < PAGE_SIZE:
                return out
            start += PAGE_SIZE

    # --- collections -------------------------------------------------------------

    def list_collections(self) -> list[dict]:
        return self._paginate("/collections")

    def create_collections(self, payload: list[dict]) -> list[str]:
        """Create collections, returning their keys in submission order."""
        resp = self._json(self.request("POST", "/collections", json_body=payload))
        failed = resp.get("failed") or {}
        if failed:
            raise ZoteroClientError(f"Zotero rejected {len(failed)} collection(s): {failed}")
        successful = resp.get("successful") or {}
        # Zotero also has an `unchanged` bucket. Fresh creates never land there, but
        # indexing `successful` blindly would KeyError if one ever did.
        unchanged = resp.get("unchanged") or {}
        missing = [i for i in range(len(payload)) if str(i) not in successful]
        if missing:
            raise ZoteroClientError(
                f"Zotero returned no key for collection(s) {missing} "
                f"(unchanged={list(unchanged)})")
        return [successful[str(i)]["key"] for i in range(len(payload))]

    def ensure_collections(self) -> dict[str, str]:
        """Idempotently ensure Alethograph/{Papers,Books} exist. Returns their keys.

        Matches on name AND parent, so an unrelated 'Papers' elsewhere in the user's 45
        collections is never adopted. A top-level collection's parentCollection is the
        literal boolean false, not null — verified live.
        """
        collections = self.list_collections()

        def find(name: str, parent) -> str | None:
            for c in collections:
                data = c.get("data") or {}
                if data.get("name") == name and (data.get("parentCollection") or False) == parent:
                    return c["key"]
            return None

        root = find(COLLECTION_ROOT, False)
        if root is None:
            root = self.create_collections([{"name": COLLECTION_ROOT}])[0]

        found = {"papers": find(COLLECTION_PAPERS, root),
                 "books": find(COLLECTION_BOOKS, root)}
        missing = [(dest, name) for dest, name in
                   (("papers", COLLECTION_PAPERS), ("books", COLLECTION_BOOKS))
                   if found[dest] is None]
        if missing:
            keys = self.create_collections(
                [{"name": name, "parentCollection": root} for _, name in missing])
            for (dest, _), key in zip(missing, keys):
                found[dest] = key
        return found

    # --- items -------------------------------------------------------------------

    def search_candidates(self, title: str | None, limit: int = 25) -> list[dict]:
        """Candidate items for deduplication, found by title. One request.

        Used on the per-ingest path; the backfill uses library_index() so it does not
        issue one search per item. qmode=titleCreatorYear is the documented default.
        """
        if not title or not title.strip():
            return []
        return self._json(self.request("GET", "/items", params={
            "q": title.strip(), "qmode": "titleCreatorYear", "limit": limit,
            "format": "json", "itemType": NON_FILE_ITEMS,
        }))

    def library_index(self) -> list[dict]:
        """Every non-attachment, non-note item in the library. One paginated sweep,
        reused across a whole backfill run."""
        return self._paginate("/items", {"itemType": NON_FILE_ITEMS})

    def create_items(self, payload: list[dict]) -> list[str]:
        """Create items, returning their keys in submission order."""
        if len(payload) > BATCH_LIMIT:
            raise ValueError(f"Zotero accepts at most {BATCH_LIMIT} items per request")
        resp = self._json(self.request("POST", "/items", json_body=payload))
        failed = resp.get("failed") or {}
        if failed:
            raise ZoteroClientError(f"Zotero rejected {len(failed)} item(s): {failed}")
        successful = resp.get("successful") or {}
        # Zotero also has an `unchanged` bucket. Fresh creates never land there, but
        # indexing `successful` blindly would KeyError if one ever did.
        unchanged = resp.get("unchanged") or {}
        missing = [i for i in range(len(payload)) if str(i) not in successful]
        if missing:
            raise ZoteroClientError(
                f"Zotero returned no key for item(s) {missing} "
                f"(unchanged={list(unchanged)})")
        return [successful[str(i)]["key"] for i in range(len(payload))]

    def get_item(self, item_key: str) -> dict:
        return self._json(self.request("GET", f"/items/{item_key}"))

    def has_attachment(self, item_key: str) -> bool:
        """True if the item already has a child attachment that carries an actual file.

        Lets the push attach a PDF to a metadata-only item the user saved themselves,
        while never adding a second file to one that already has one. A `linked_url`
        attachment is a bookmark with no file behind it at all, so it does not count.
        Every other linkMode — imported_file, imported_url, linked_file, or one we've
        never seen (including an absent linkMode) — is treated conservatively as
        already having a file, since a linked_file in particular means the user has
        genuinely attached a PDF already, just not into Zotero's own storage; uploading
        a second copy would visibly duplicate it.
        """
        children = self._json(self.request("GET", f"/items/{item_key}/children",
                              params={"format": "json"}))
        for child in children:
            data = child.get("data") or {}
            if data.get("itemType") == "attachment" and data.get("linkMode") != "linked_url":
                return True
        return False

    def add_to_collection(self, item_key: str, collection_key: str) -> bool:
        """Add an existing item to a collection without touching any other field.

        Returns False if already a member. PATCH carries only `collections`; Zotero leaves
        properties absent from the body untouched, so the user's metadata is never rewritten.
        """
        item = self.get_item(item_key)
        data = item.get("data") or {}
        collections = list(data.get("collections") or [])
        if collection_key in collections:
            return False
        self.request("PATCH", f"/items/{item_key}",
                     json_body={"collections": collections + [collection_key]},
                     headers={"If-Unmodified-Since-Version": str(data.get("version", 0))})
        return True

    # --- files -------------------------------------------------------------------

    def upload_attachment(self, item_key: str, filename: str, data: bytes) -> str:
        """Zotero's three-step file upload. Returns "exists" or "uploaded".

        Raises ZoteroQuotaError on 413 so callers can report a quota problem distinctly
        from a code defect. "exists" means Zotero already stores a file with this md5 and
        has associated it with the item — free server-side deduplication.
        """
        auth_headers = {"If-None-Match": "*",
                        "Content-Type": "application/x-www-form-urlencoded"}
        auth = self._json(self.request(
            "POST", f"/items/{item_key}/file", headers=auth_headers, data={
                "md5": hashlib.md5(data).hexdigest(),
                "filename": filename,
                "filesize": len(data),
                "mtime": int(time.time() * 1000),  # milliseconds, per the API docs
            }))

        if auth.get("exists"):
            return "exists"

        # Every field below is dereferenced unconditionally past this point; a 200 whose
        # body doesn't match the documented shape must not escape as a raw KeyError.
        required = ("url", "contentType", "prefix", "suffix", "uploadKey")
        missing = [f for f in required if f not in auth]
        if missing:
            raise ZoteroClientError(
                f"Zotero's upload authorization response is missing {missing}: {auth}")

        body = auth["prefix"].encode("utf-8") + data + auth["suffix"].encode("utf-8")
        # absolute=True withholds the API key: this host is Zotero's storage backend.
        self.request("POST", auth["url"], absolute=True, data=body,
                     headers={"Content-Type": auth["contentType"]})

        self.request("POST", f"/items/{item_key}/file", headers=auth_headers,
                     data={"upload": auth["uploadKey"]})
        return "uploaded"
