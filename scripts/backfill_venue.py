"""One-off backfill: re-fetch Semantic Scholar metadata for papers ingested before venue
data was persisted, and write venue/journal/volume/pages/publication_types onto the node.

Deliberately does NOT write `doi`: compute_paper_id prefers DOI over arXiv id, so adding a
DOI to a node whose id was derived from its arXiv id decouples the two and makes a later
re-ingest mint a duplicate Paper. Venue is this script's only job.

Papers with neither an arXiv id nor a usable DOI cannot be enriched (S2 lookup is
identifier-only) and are reported as 'unidentifiable' rather than silently skipped.

Writes through research_port.WRITE_PAPER so coalesce semantics match the ingest asset
exactly — a paper whose lookup fails keeps whatever it already had.

Idempotent; free S2 API, 1 req/paper, retried internally on 429, paced at ~1 req/s to
stay under S2's unauthenticated limit (the same 1.1s pacing scripts/backfill_citations.py
uses; without it, 124 back-to-back requests throttle hard and each 429 costs 15s of
in-lookup backoff).

Run: uv run python scripts/backfill_venue.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.graph import research_port as rp

NEEDS_VENUE = """
MATCH (p:Paper)
WHERE p.venue IS NULL AND p.journal_name IS NULL
RETURN p.id AS id, p.document_id AS document_id,
       p.arxiv_id AS arxiv_id, p.doi AS doi, p.title AS title
ORDER BY p.id
"""

EXISTING_AUTHORS = """
MATCH (a:Author)-[:AUTHORED]->(p:Paper {id:$id}) RETURN collect(a.name) AS names
"""


def enrich(rec: dict, p: dict, authors: list[dict]) -> dict:
    """Build WRITE_PAPER params. Every enrichment field may be None; coalesce protects the
    stored value, so a failed lookup is a no-op rather than data loss.

    `doi` and `arxiv_id` deliberately pass through UNCHANGED from the stored node — see the
    module docstring on why this script must not alter identity inputs.
    """
    return {
        "id": p["id"],
        "document_id": p["document_id"],
        "title": rec.get("title"),
        "year": rec.get("year"),
        "arxiv_id": p.get("arxiv_id"),
        "doi": p.get("doi"),
        "s2_id": rec.get("s2_id"),
        "abstract": rec.get("abstract"),
        "tldr": rec.get("tldr"),
        "citation_count": rec.get("citation_count"),
        "influential_citation_count": rec.get("influential_citation_count"),
        "venue": rec.get("venue"),
        "journal_name": rec.get("journal_name"),
        "volume": rec.get("volume"),
        "pages": rec.get("pages"),
        "publication_types": rec.get("publication_types") or None,
        "authors": authors,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    args = ap.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]),
    )
    database = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")

    enriched = no_record = unidentifiable = 0
    with driver, driver.session(database=database) as s:
        papers = [dict(r) for r in s.run(NEEDS_VENUE)]
        print(f"{len(papers)} papers missing venue data")

        for p in papers:
            doi = rp.clean_doi(p.get("doi"))
            arxiv = p.get("arxiv_id")
            if not arxiv and not doi:
                unidentifiable += 1
                print(f"  SKIP  {p['id']}  (no arxiv id, no usable doi)")
                continue

            # ~1 req/s. S2's unauthenticated tier shares a global budget and throttles
            # unpredictably; do NOT wrap these in with_retry — they retry internally as
            # of Task 2, and nesting multiplies both the requests and the backoff.
            time.sleep(1.1)
            rec = rp.lookup_by_arxiv(arxiv) if arxiv else None
            if rec is None and doi:
                time.sleep(1.1)
                rec = rp.lookup_by_doi(doi)

            if not rec:
                no_record += 1
                print(f"  MISS  {p['id']}  (S2 returned nothing)")
                continue

            print(f"  OK    {p['id']}  venue={rec.get('venue')!r} "
                  f"types={rec.get('publication_types')}")
            if not args.dry_run:
                # S2 author names win when present; otherwise keep what the node already
                # has, so WRITE_PAPER's MERGE is a no-op rather than dropping authorship.
                authors = rec.get("authors")
                if not authors:
                    names = s.run(EXISTING_AUTHORS, id=p["id"]).single()["names"]
                    authors = [{"name": n, "s2_author_id": None} for n in names]
                s.run(rp.WRITE_PAPER, **enrich(rec, p, authors))
            enriched += 1

    verb = "would enrich" if args.dry_run else "enriched"
    print(f"\n{verb}: {enriched}   no S2 record: {no_record}   "
          f"unidentifiable: {unidentifiable}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
