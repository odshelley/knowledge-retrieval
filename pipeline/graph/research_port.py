"""Vendored from research_tools.py (~/Projects/alethograph/skills/research/scripts/
research_tools.py @ 0f22fa6). CLI/argparse and the ~/.claude/research-neo4j.json default
connection stripped; callers pass the pipeline's Neo4j driver. NOT a runtime dependency."""
from __future__ import annotations

import logging
import re
import time

import requests
from requests import RequestException

log = logging.getLogger(__name__)

BASE = "https://api.semanticscholar.org/graph/v1"
FIELDS = ("paperId,title,abstract,year,venue,externalIds,citationCount,"
          "influentialCitationCount,tldr,authors,publicationTypes,journal")
REF_FIELDS = "title,externalIds,influentialCitationCount"


# --- paper identity (spec §5.4) --------------------------------------------------------
def strip_arxiv_version(arxiv_id: str) -> str:
    return re.sub(r"v\d+$", "", arxiv_id)


def normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", title.strip().lower())


# A DOI is "10.<4-9 digit registrant>/<suffix>". Anything else is not a DOI.
_DOI_SHAPE = re.compile(r"^10\.\d{4,9}/\S+$")
# Unfilled LaTeX template DOIs seen in the corpus. Case-SENSITIVE on purpose: a
# case-insensitive rule rejected the legitimate "10.1088/1361-6420/abcxxxx".
_PLACEHOLDER_RUN = re.compile(r"N{4,}|X{4,}")
# All-zeros suffix, e.g. "0000000.0000000". Requiring the WHOLE suffix keeps
# "10.1002/nla.2000000" (six zeros among real digits) valid.
_ALL_ZEROS_SUFFIX = re.compile(r"^[0.]+$")
_DOI_URL_PREFIX = re.compile(r"^https?://(dx\.)?doi\.org/", re.IGNORECASE)


def clean_doi(doi: str | None) -> str | None:
    """Normalize a DOI, returning None for absent, malformed, or placeholder values.

    Strips doi.org/dx.doi.org URL prefixes (stored verbatim by the frontmatter LLM, which
    breaks both S2 lookup and compute_paper_id's "doi:"+lower() identity) and rejects the
    unfilled LaTeX template DOIs that reached the graph as real identifiers.
    """
    if not doi:
        return None
    d = _DOI_URL_PREFIX.sub("", doi.strip())
    if not _DOI_SHAPE.match(d):
        return None
    suffix = d.split("/", 1)[1]
    if _PLACEHOLDER_RUN.search(suffix) or _ALL_ZEROS_SUFFIX.match(suffix):
        return None
    return d


def compute_paper_id(doi: str | None, arxiv_id: str | None, title: str | None) -> str:
    if doi:
        return "doi:" + doi.strip().lower()
    if arxiv_id:
        return "arxiv:" + strip_arxiv_version(arxiv_id.strip().lower())
    if title:
        return "title:" + normalize_title(title)
    raise ValueError("cannot form paper id: no doi/arxiv/title")


# --- Semantic Scholar (vendored) -------------------------------------------------------
def _paper_json_to_record(j: dict) -> dict:
    ext = j.get("externalIds") or {}
    journal = j.get("journal") or {}
    return {
        "s2_id": j.get("paperId"), "title": j.get("title"), "abstract": j.get("abstract"),
        "year": j.get("year"), "venue": j.get("venue"),
        "journal_name": journal.get("name"),
        "volume": journal.get("volume"),
        "pages": journal.get("pages"),
        # S2 returns literal null here, not [], when the field is absent.
        "publication_types": j.get("publicationTypes") or [],
        "doi": ext.get("DOI"), "arxiv_id": ext.get("ArXiv"),
        "citation_count": j.get("citationCount"),
        "influential_citation_count": j.get("influentialCitationCount"),
        "tldr": (j.get("tldr") or {}).get("text"),
        "authors": [{"name": a.get("name"), "s2_author_id": a.get("authorId")}
                    for a in (j.get("authors") or [])],
    }


_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def with_retry(fn, *args, attempts: int = 3, base_sleep: float = 5.0):
    """Retry a call that signals failure by returning a falsy value.

    Used for rp.references(), which has no internal retry. Do NOT wrap lookup_by_arxiv /
    lookup_by_doi in this — they retry internally, and nesting multiplies the sleeps.
    """
    out = None
    for i in range(attempts):
        out = fn(*args)
        if out:
            return out
        if i < attempts - 1:
            time.sleep(base_sleep * (i + 1))
    return out


def _get_paper(path: str) -> dict | None:
    """One S2 GET with retry on throttling/5xx. A 404 is definitive and returns at once.

    Path prefixes are "arXiv:<id>" and "DOI:<doi>". S2 documents these uppercase as
    "ARXIV:"; prefix matching is case-insensitive in practice, verified live.
    """
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE}/paper/{path}", params={"fields": FIELDS}, timeout=20)
        except RequestException as exc:
            log.warning("S2 request for %s failed: %s", path, exc)
            time.sleep(5.0 * (attempt + 1))
            continue
        if r.status_code == 200:
            return _paper_json_to_record(r.json())
        if r.status_code not in _RETRYABLE_STATUS:
            log.info("S2 has no record for %s (HTTP %s)", path, r.status_code)
            return None
        log.warning("S2 throttled/erred for %s (HTTP %s), attempt %s/3",
                    path, r.status_code, attempt + 1)
        time.sleep(5.0 * (attempt + 1))
    log.warning("S2 lookup for %s exhausted retries — treating as unenriched", path)
    return None


def lookup_by_arxiv(arxiv_id: str) -> dict | None:
    return _get_paper(f"arXiv:{arxiv_id}")


def lookup_by_doi(doi: str) -> dict | None:
    return _get_paper(f"DOI:{doi}")


def references(s2_id: str) -> list[dict]:
    try:
        r = requests.get(f"{BASE}/paper/{s2_id}/references",
                         params={"fields": REF_FIELDS, "limit": 100}, timeout=20)
        # S2 can answer 200 with {"data": null} — `or []` guards the None (.get's default
        # only covers a MISSING key), which otherwise crashes reference iteration in triage.
        return (r.json().get("data") or []) if r.status_code == 200 else []
    except RequestException:
        return []


def top_reference_records(raw_refs: list[dict], limit: int = 3) -> list[dict]:
    recs = []
    for ref in raw_refs or []:
        cp = ref.get("citedPaper") or {}
        ext = cp.get("externalIds") or {}
        recs.append({
            "s2_id": cp.get("paperId"),
            "doi": ext.get("DOI"),
            "arxiv_id": ext.get("ArXiv"),
            "title_norm": normalize_title(cp["title"]) if cp.get("title") else None,
            "influential_count": cp.get("influentialCitationCount") or 0,
        })
    return sorted(recs, key=lambda r: r["influential_count"], reverse=True)[:limit]


# --- vendored Cypher (db-add-paper / db-cite-paper) ------------------------------------
WRITE_PAPER = """
MERGE (p:Paper {id: $id})
SET p.title = coalesce($title, p.title),
    p.year = coalesce($year, p.year),
    p.arxiv_id = coalesce($arxiv_id, p.arxiv_id),
    p.doi = coalesce($doi, p.doi),
    p.s2_id = coalesce($s2_id, p.s2_id),
    p.abstract = coalesce($abstract, p.abstract),
    p.tldr = coalesce($tldr, p.tldr),
    p.citation_count = coalesce($citation_count, p.citation_count),
    p.influential_citation_count = coalesce($influential_citation_count,
                                            p.influential_citation_count),
    p.venue = coalesce($venue, p.venue),
    p.journal_name = coalesce($journal_name, p.journal_name),
    p.volume = coalesce($volume, p.volume),
    p.pages = coalesce($pages, p.pages),
    p.publication_types = coalesce($publication_types, p.publication_types),
    p.document_id = $document_id
WITH p
UNWIND $authors AS author
  MERGE (a:Author {name: author.name})
  SET a.s2_author_id = coalesce(author.s2_author_id, a.s2_author_id)
  MERGE (a)-[:AUTHORED]->(p)
"""
