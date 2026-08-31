"""Push orchestration shared by the Dagster assets and the backfill script.

One code path so the asset and the backfill cannot diverge in how an item is built,
matched, or attached. push_one() returns a result dict rather than raising on transient
trouble or a full quota: a Zotero problem must never fail an ingest run whose graph write
has already succeeded.

`complete` is the contract with callers — write zotero_key ONLY when it is true. Anything
else leaves the record visible to the repair query so a later run finishes the job.
"""
from __future__ import annotations

import logging

from pipeline.zotero.client import ZoteroQuotaError, ZoteroTransientError
from pipeline.zotero.items import attachment_item, book_item, match_existing, paper_item
from pipeline.zotero.naming import attachment_filename

log = logging.getLogger(__name__)

PAPER_FOR_PUSH = """
MATCH (p:Paper {document_id: $document_id})
OPTIONAL MATCH (a:Author)-[:AUTHORED]->(p)
RETURN p{.id, .title, .year, .doi, .arxiv_id, .venue, .journal_name, .volume, .pages,
         .publication_types, .abstract, .zotero_key} AS node,
       collect(DISTINCT a.name) AS authors
"""

BOOK_FOR_PUSH = """
MATCH (b:Book {document_id: $document_id})
OPTIONAL MATCH (a:Author)-[:AUTHORED]->(b)
RETURN b{.id, .title, .year, .publisher, .edition, .isbn, .zotero_key} AS node,
       collect(DISTINCT a.name) AS authors
"""

PAPERS_NEEDING_PUSH = """
MATCH (p:Paper) WHERE p.zotero_key IS NULL
RETURN p.document_id AS document_id, p.id AS id ORDER BY p.id
"""

BOOKS_NEEDING_PUSH = """
MATCH (b:Book) WHERE b.zotero_key IS NULL
RETURN b.document_id AS document_id, b.id AS id ORDER BY b.id
"""

MARK_PAPER_PUSHED = "MATCH (p:Paper {id:$id}) SET p.zotero_key = $key"
MARK_BOOK_PUSHED = "MATCH (b:Book {id:$id}) SET b.zotero_key = $key"


def _attach(client, item_key: str, filename: str, pdf_bytes: bytes | None) -> str:
    """Upload the PDF, returning a status string. Never raises for quota or throttling."""
    if not pdf_bytes:
        return "skipped-no-pdf"
    try:
        return client.upload_attachment(item_key, filename, pdf_bytes)
    except ZoteroQuotaError:
        log.warning("Zotero storage quota exceeded uploading %s", filename)
        return "quota-exceeded"


def push_one(client, collection_key: str, record: dict, authors: list[str],
             pdf_bytes: bytes | None, candidates: list[dict] | None = None) -> dict:
    """File one paper or book into Zotero.

    `record` carries a "kind" of "paper" or "book". `candidates` lets the backfill supply a
    pre-fetched library index instead of one search per item; when None, one search is issued.

    Returns {pushed, complete, outcome, zotero_key, item_type, filename, attachment, reason}.
    Raises only ZoteroClientError (a payload defect or revoked key) — never on throttling,
    a locked library, or an exhausted storage quota.
    """
    is_book = record.get("kind") == "book"
    result = {"pushed": False, "complete": False, "outcome": None, "zotero_key": None,
              "item_type": None, "filename": None, "attachment": None, "reason": None}
    filename = attachment_filename(record.get("title"), authors, record.get("year"))
    result["filename"] = filename

    try:
        pool = client.search_candidates(record.get("title")) if candidates is None \
            else candidates
        existing = match_existing(pool, record.get("doi"), record.get("arxiv_id"),
                                  record.get("title"),
                                  isbn=record.get("isbn") if is_book else None)

        if existing:
            client.add_to_collection(existing, collection_key)
            result.update(pushed=True, outcome="matched", zotero_key=existing)
            # Attach only when the user's item has no file — a metadata-only entry saved
            # from a browser is exactly the case this project exists to make openable.
            if client.has_attachment(existing):
                result.update(attachment="skipped-has-attachment", complete=True)
            else:
                status = _attach(client, existing, filename, pdf_bytes)
                result.update(attachment=status,
                              complete=status != "quota-exceeded")
            return result

        item = (book_item(record, authors, collection_key) if is_book
                else paper_item(record, authors, collection_key))
        result["item_type"] = item["itemType"]
        key = client.create_items([item])[0]
        result.update(pushed=True, outcome="created", zotero_key=key)

        if not pdf_bytes:
            result.update(attachment="skipped-no-pdf", complete=True)
            return result

        att_key = client.create_items([attachment_item(key, filename)])[0]
        status = _attach(client, att_key, filename, pdf_bytes)
        result.update(attachment=status, complete=status != "quota-exceeded")
        return result

    except ZoteroTransientError as exc:
        # Throttling, a locked library, or a 5xx that survived retries. `complete` stays
        # False, so the caller withholds zotero_key and the repair query revisits this.
        log.warning("Zotero push incomplete for %s: %s", record.get("id"), exc)
        result["reason"] = str(exc)
        return result
