"""One-off backfill: resolve each paper's Semantic Scholar id (from the node, else via
arXiv/DOI lookup), re-fetch its references, and link in-corpus CITES edges. Repairs papers
ingested under the old top-3 reference stash (triage_metadata limit=3), which left the
citation graph at 2 edges / 135 papers.

Cited papers are matched in-memory against ALL corpus identifiers (s2, doi, arxiv,
normalized title) — the graph-side FIND_CITED can only match identifier fields, and most
Paper nodes carry an arXiv id or title only. Resolved s2_ids are written back to the node.

Idempotent (MERGE); free S2 API, ~1-2 req/paper, gentle pacing.
Run: uv run python scripts/backfill_citations.py [--dry-run]
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

MERGE_CITES = "MATCH (a:Paper {id:$citing}),(b:Paper {id:$cited}) MERGE (a)-[:CITES]->(b)"


def build_indexes(papers: list[dict]) -> dict[str, dict[str, str]]:
    """Corpus lookup tables: identifier -> paper id."""
    by = {"s2": {}, "doi": {}, "arxiv": {}, "title": {}}
    for p in papers:
        if p.get("s2"):
            by["s2"][p["s2"]] = p["id"]
        if p.get("doi"):
            by["doi"][p["doi"].lower()] = p["id"]
        if p.get("arxiv"):
            by["arxiv"][rp.strip_arxiv_version(p["arxiv"])] = p["id"]
        if p.get("title"):
            by["title"][rp.normalize_title(p["title"])] = p["id"]
    return by


def match_ref(by: dict, ref: dict) -> str | None:
    if ref.get("s2_id") and ref["s2_id"] in by["s2"]:
        return by["s2"][ref["s2_id"]]
    if ref.get("doi") and ref["doi"].lower() in by["doi"]:
        return by["doi"][ref["doi"].lower()]
    if ref.get("arxiv_id") and rp.strip_arxiv_version(ref["arxiv_id"]) in by["arxiv"]:
        return by["arxiv"][rp.strip_arxiv_version(ref["arxiv_id"])]
    if ref.get("title_norm") and ref["title_norm"] in by["title"]:
        return by["title"][ref["title_norm"]]
    return None


def resolve_s2(p: dict) -> str | None:
    if p.get("s2"):
        return p["s2"]
    rec = None
    if p.get("arxiv"):
        rec = rp.lookup_by_arxiv(rp.strip_arxiv_version(p["arxiv"]))
        time.sleep(1.1)
    if rec is None and p.get("doi"):
        rec = rp.lookup_by_doi(p["doi"])
        time.sleep(1.1)
    return rec.get("s2_id") if rec else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    load_dotenv()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))

    with driver.session(database=os.environ.get("NEO4J_NEW_DATABASE", "neo4j")) as s:
        papers = s.run("MATCH (p:Paper) RETURN p.id AS id, p.title AS title, p.doi AS doi, "
                       "p.arxiv_id AS arxiv, p.s2_id AS s2").data()
        by = build_indexes(papers)
        print(f"{len(papers)} papers; s2 known for "
              f"{sum(1 for p in papers if p.get('s2'))}", flush=True)
        edges = resolved = 0
        for i, p in enumerate(papers, 1):
            s2 = resolve_s2(p)
            if s2 and not p.get("s2"):
                resolved += 1
                if not args.dry_run:
                    s.run("MATCH (p:Paper {id:$id}) SET p.s2_id = $s2", id=p["id"], s2=s2)
            if not s2:
                print(f"[{i}/{len(papers)}] {p['id'][:50]}: no s2 id resolvable", flush=True)
                continue
            refs = rp.top_reference_records(rp.references(s2), limit=100)
            time.sleep(1.1)  # S2 unauthenticated rate limit
            matched = 0
            for ref in refs:
                target = match_ref(by, ref)
                if target and target != p["id"]:
                    matched += 1
                    if not args.dry_run:
                        s.run(MERGE_CITES, citing=p["id"], cited=target)
            edges += matched
            print(f"[{i}/{len(papers)}] {p['id'][:50]}: {len(refs)} refs, "
                  f"{matched} in-corpus", flush=True)

        total = s.run("MATCH (:Paper)-[c:CITES]->(:Paper) RETURN count(c) AS n").single()["n"]
        print(f"done: {edges} in-corpus links this run; {resolved} s2 ids resolved; "
              f"CITES edges now {total}" + (" (dry-run)" if args.dry_run else ""), flush=True)
    driver.close()


if __name__ == "__main__":
    main()
