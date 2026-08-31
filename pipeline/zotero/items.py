"""Zotero item construction and deduplication matching.

Pure — builds dicts and compares them. All HTTP lives in client.py, so every rule here
(item-type mapping, creator splitting, match precedence) is unit-testable without fakes.

Field-name casing is load-bearing: Zotero silently drops unknown keys, so `DOI` and `ISBN`
must be uppercase. Verified against the live items/new templates, schema v42.
"""
from __future__ import annotations

import re

from pipeline.graph.research_port import clean_doi, normalize_title, strip_arxiv_version

# DOI registrants that mint DOIs for preprints, not publications. A paper carrying only
# one of these is still a preprint.
_PREPRINT_DOI_PREFIXES = ("10.48550/", "10.2139/ssrn")

# Preprint/working-paper hosts that S2 sometimes reports as `venue` or `journal.name` for
# a paper that was never actually published in a journal — e.g. journal.name == "ArXiv"
# with an "abs/..." volume, or venue == "Social Science Research Network" for an SSRN
# working paper that S2's own publicationTypes still (wrongly) tags "JournalArticle".
_PREPRINT_HOST_NAMES = {
    "arxiv", "arxiv.org", "corr",
    "ssrn", "ssrn electronic journal", "social science research network",
}

_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]")


def _is_preprint_host(value: str | None) -> bool:
    return bool(value) and value.strip().lower() in _PREPRINT_HOST_NAMES


def publisher_doi(doi: str | None) -> str | None:
    """A cleaned DOI, unless it belongs to a preprint registrar (arXiv, SSRN)."""
    d = clean_doi(doi)
    if d is None:
        return None
    return None if d.lower().startswith(_PREPRINT_DOI_PREFIXES) else d


def split_creator(name: str) -> dict:
    """Zotero two-field mode, splitting on the last space. A single-token name uses
    Zotero's single-field mode — which takes `name` alone, with NO `fieldMode` key
    (that is an internal Zotero client concept, not Web API v3)."""
    parts = (name or "").strip().split()
    if len(parts) < 2:
        return {"creatorType": "author", "name": " ".join(parts)}
    return {"creatorType": "author", "firstName": " ".join(parts[:-1]),
            "lastName": parts[-1]}


def _journal_fields(paper: dict) -> tuple[str | None, str | None, str | None, str | None]:
    """(publication title, abbreviation, volume, pages), with junk journal data dropped.

    `venue` is the authoritative display name; `journal.name` is often an abbreviation and
    is occasionally "ArXiv" for a paper published elsewhere, in which case its volume is an
    arXiv identifier rather than a volume number and the whole object must be discarded.
    """
    venue = paper.get("venue")
    journal_name = paper.get("journal_name")
    volume, pages = paper.get("volume"), paper.get("pages")

    if _is_preprint_host(journal_name):
        journal_name, volume, pages = None, None, None

    title = venue or journal_name
    abbrev = journal_name if (journal_name and journal_name != title) else None
    return title, abbrev, volume, pages


def _base(title: str | None, authors: list[str], year, collection_key: str) -> dict:
    item = {
        "title": title or "Untitled",
        "creators": [split_creator(a) for a in (authors or []) if a and a.strip()],
        "collections": [collection_key],
        "tags": [],
        "relations": {},
    }
    if year:
        item["date"] = str(year)
    return item


def paper_item(paper: dict, authors: list[str], collection_key: str) -> dict:
    """Map a Paper node onto a Zotero item, typed from publication_types then DOI shape."""
    item = _base(paper.get("title"), authors, paper.get("year"), collection_key)
    types = [t.lower() for t in (paper.get("publication_types") or [])]
    doi = publisher_doi(paper.get("doi"))
    pub_title, abbrev, volume, pages = _journal_fields(paper)
    arxiv = paper.get("arxiv_id")

    if paper.get("abstract"):
        item["abstractNote"] = paper["abstract"]

    # A preprint-host venue/journal name (SSRN, arXiv, CoRR) with no genuine publisher
    # DOI means the paper was never actually published, whatever publication_types
    # claims — S2 tags SSRN working papers "JournalArticle" and is simply wrong. A real
    # publisher DOI is the trustworthy signal, so it always overrides this branch.
    hosted_as_preprint = _is_preprint_host(paper.get("venue")) or _is_preprint_host(
        paper.get("journal_name"))
    if hosted_as_preprint and doi is None:
        item["itemType"] = "preprint"
        if arxiv:
            bare = strip_arxiv_version(arxiv)
            item["repository"] = "arXiv"
            item["archiveID"] = f"arXiv:{bare}"
            item["url"] = f"https://arxiv.org/abs/{bare}"
        return item

    # "conference" before "journalarticle": S2 returns both for proceedings a journal later
    # indexed, and the proceedings is the more specific truth. Casing has no space —
    # S2's own OpenAPI example says "Journal Article" and is stale; live values do not.
    if "conference" in types:
        item["itemType"] = "conferencePaper"
        if pub_title:
            item["proceedingsTitle"] = pub_title
        if pages:
            item["pages"] = pages
        if doi:
            item["DOI"] = doi
        return item

    if "journalarticle" in types or doi:
        item["itemType"] = "journalArticle"
        if pub_title:
            item["publicationTitle"] = pub_title
        if abbrev:
            item["journalAbbreviation"] = abbrev
        if volume:
            item["volume"] = volume
        if pages:
            item["pages"] = pages
        if doi:
            item["DOI"] = doi
        return item

    # preprint has no publicationTitle/volume/pages field — do not set them here.
    item["itemType"] = "preprint"
    if arxiv:
        bare = strip_arxiv_version(arxiv)
        item["repository"] = "arXiv"
        item["archiveID"] = f"arXiv:{bare}"
        item["url"] = f"https://arxiv.org/abs/{bare}"
    return item


def book_item(book: dict, authors: list[str], collection_key: str) -> dict:
    item = _base(book.get("title"), authors, book.get("year"), collection_key)
    item["itemType"] = "book"
    for src, dest in (("publisher", "publisher"), ("edition", "edition"), ("isbn", "ISBN")):
        if book.get(src):
            item[dest] = book[src]
    return item


def attachment_item(parent_key: str, filename: str) -> dict:
    """A child attachment. `collections` is omitted: child items cannot be members."""
    return {
        "itemType": "attachment",
        "parentItem": parent_key,
        "linkMode": "imported_file",
        "title": filename,
        "filename": filename,
        "contentType": "application/pdf",
        "tags": [],
        "relations": {},
    }


def _norm_isbn(value: str | None) -> str | None:
    return _NON_ALNUM.sub("", value).upper() if value else None


def match_existing(candidates: list[dict], doi: str | None, arxiv_id: str | None,
                   title: str | None, isbn: str | None = None) -> str | None:
    """Return the Zotero item key of an existing item describing this work, or None.

    Precedence: ISBN (books), cleaned DOI, arXiv id, then normalized title. A placeholder
    DOI is never used for matching.
    """
    want_isbn = _norm_isbn(isbn)
    if want_isbn:
        for c in candidates:
            if _norm_isbn((c.get("data") or {}).get("ISBN")) == want_isbn:
                return c["key"]

    target_doi = clean_doi(doi)
    if target_doi:
        want = target_doi.lower()
        for c in candidates:
            cand = clean_doi((c.get("data") or {}).get("DOI"))
            if cand and cand.lower() == want:
                return c["key"]

    if arxiv_id:
        bare = strip_arxiv_version(arxiv_id.strip().lower())
        if bare:
            # Digit boundaries: plain substring matching made "1707.08464" match
            # "11707.084640", filing a paper against an unrelated item.
            pattern = re.compile(rf"(?<!\d){re.escape(bare)}(?!\d)")
            for c in candidates:
                data = c.get("data") or {}
                haystack = f"{data.get('archiveID') or ''} {data.get('url') or ''}".lower()
                if pattern.search(haystack):
                    return c["key"]

    if title:
        want = normalize_title(title)
        for c in candidates:
            cand_title = (c.get("data") or {}).get("title")
            if cand_title and normalize_title(cand_title) == want:
                return c["key"]

    return None
