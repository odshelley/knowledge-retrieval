"""Zotero Web API v3 transport.

Hand-rolled on `requests`, mirroring how research_port.py handles Semantic Scholar —
pyzotero would be a new dependency for a handful of endpoints. All item-shaping logic
lives in items.py; this module only moves bytes.

The API key goes in the Zotero-API-Key header, never a URL, and is withheld entirely from
absolute-URL requests, which target Zotero's third-party storage host rather than the API.
"""
from __future__ import annotations

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
                self._backoff_until = time.monotonic() + float(backoff)

            if resp.status_code in _RETRYABLE_STATUS:
                delay = float(resp.headers.get("Retry-After") or 2.0 * (attempt + 1))
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

    def _paginate(self, path: str, params: dict | None = None) -> list[dict]:
        out: list[dict] = []
        start = 0
        while True:
            page = self.request("GET", path,
                                params={**(params or {}), "limit": PAGE_SIZE,
                                        "start": start, "format": "json"}).json()
            out.extend(page)
            if len(page) < PAGE_SIZE:
                return out
            start += PAGE_SIZE

    # --- collections -------------------------------------------------------------

    def list_collections(self) -> list[dict]:
        return self._paginate("/collections")

    def create_collections(self, payload: list[dict]) -> list[str]:
        """Create collections, returning their keys in submission order."""
        resp = self.request("POST", "/collections", json_body=payload).json()
        successful = resp.get("successful") or {}
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
