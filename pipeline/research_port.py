"""Vendored from research_tools.py (~/Projects/alethograph/skills/research/scripts/
research_tools.py @ 0f22fa6). CLI/argparse and the ~/.claude/research-neo4j.json default
connection stripped; callers pass the pipeline's Neo4j driver. NOT a runtime dependency."""
from __future__ import annotations

import re

import requests
from requests import RequestException

BASE = "https://api.semanticscholar.org/graph/v1"
FIELDS = "paperId,title,abstract,year,venue,externalIds,citationCount,influentialCitationCount,tldr,authors"
REF_FIELDS = "title,externalIds,influentialCitationCount"


# --- paper identity (spec §5.4) --------------------------------------------------------
def strip_arxiv_version(arxiv_id: str) -> str:
    return re.sub(r"v\d+$", "", arxiv_id)


def normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", title.strip().lower())


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
    return {
        "s2_id": j.get("paperId"), "title": j.get("title"), "abstract": j.get("abstract"),
        "year": j.get("year"), "venue": j.get("venue"),
        "doi": ext.get("DOI"), "arxiv_id": ext.get("ArXiv"),
        "citation_count": j.get("citationCount"),
        "influential_citation_count": j.get("influentialCitationCount"),
        "tldr": (j.get("tldr") or {}).get("text"),
        "authors": [{"name": a.get("name"), "s2_author_id": a.get("authorId")}
                    for a in (j.get("authors") or [])],
    }


def lookup_by_arxiv(arxiv_id: str) -> dict | None:
    try:
        r = requests.get(f"{BASE}/paper/arXiv:{arxiv_id}", params={"fields": FIELDS}, timeout=20)
        return _paper_json_to_record(r.json()) if r.status_code == 200 else None
    except RequestException:
        return None


def lookup_by_doi(doi: str) -> dict | None:
    try:
        r = requests.get(f"{BASE}/paper/DOI:{doi}", params={"fields": FIELDS}, timeout=20)
        return _paper_json_to_record(r.json()) if r.status_code == 200 else None
    except RequestException:
        return None


def references(s2_id: str) -> list[dict]:
    try:
        r = requests.get(f"{BASE}/paper/{s2_id}/references",
                         params={"fields": REF_FIELDS, "limit": 100}, timeout=20)
        return r.json().get("data", []) if r.status_code == 200 else []
    except RequestException:
        return []


def top_reference_records(raw_refs: list[dict], limit: int = 3) -> list[dict]:
    recs = []
    for ref in raw_refs:
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
SET p.title=$title, p.year=$year, p.arxiv_id=$arxiv_id, p.doi=$doi, p.s2_id=$s2_id,
    p.abstract=$abstract, p.tldr=$tldr, p.citation_count=$citation_count,
    p.influential_citation_count=$influential_citation_count, p.document_id=$document_id
WITH p
UNWIND $authors AS author
  MERGE (a:Author {name: author.name})
  SET a.s2_author_id = coalesce(author.s2_author_id, a.s2_author_id)
  MERGE (a)-[:AUTHORED]->(p)
"""
