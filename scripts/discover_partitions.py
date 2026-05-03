"""One-shot: build data/partitions.json from the legacy alethograph DB + the iCloud vault.

Run after Task 5; output is committed to git so the Dagster code location can load
partitions deterministically without DB access at workspace startup.

Usage:
    uv run python scripts/discover_partitions.py
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

VAULT = Path(os.environ["ALETHOGRAPH_VAULT_PATH"])
OUT = Path("data/partitions.json")
UNRESOLVED = Path("data/partitions_unresolved.json")

PAPERS_QUERY = """
MATCH (p:Paper)
RETURN p.id AS paper_id,
       p.title AS title,
       p.note_path AS note_path,
       p.arxiv_id AS arxiv_id,
       p.doi AS doi,
       p.year AS year
"""


def _slug(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s[:60]


def _index_pdfs(vault: Path) -> dict[str, Path]:
    """Build a stem → path index of every PDF in the vault."""
    return {p.stem.lower(): p for p in vault.rglob("*.pdf")}


def _match_pdf(paper: dict, pdf_index: dict[str, Path]) -> Path | None:
    """Try arxiv id (most reliable), then DOI suffix, then title slug."""
    candidates: list[str] = []
    if paper.get("arxiv_id"):
        candidates.append(paper["arxiv_id"])
        candidates.append(paper["arxiv_id"].replace(".", "_"))
        candidates.append(paper["arxiv_id"].replace(".", ""))
    if paper.get("doi"):
        candidates.append(paper["doi"].split("/")[-1])
    title_slug = _slug(paper["title"]) if paper.get("title") else None

    for stem, path in pdf_index.items():
        for cand in candidates:
            if cand.lower() in stem:
                return path
    if title_slug:
        for stem, path in pdf_index.items():
            if len(title_slug) >= 25 and title_slug[:25] in stem:
                return path
    return None


def main() -> None:
    if not VAULT.exists():
        raise SystemExit(f"vault path does not exist: {VAULT}")

    driver = GraphDatabase.driver(
        os.environ["NEO4J_LEGACY_URI"],
        auth=(os.environ["NEO4J_LEGACY_USERNAME"], os.environ["NEO4J_LEGACY_PASSWORD"]),
    )
    with driver.session(database=os.environ.get("NEO4J_LEGACY_DATABASE", "neo4j")) as s:
        papers = [dict(r) for r in s.run(PAPERS_QUERY)]

    pdf_index = _index_pdfs(VAULT)
    print(f"loaded {len(papers)} papers from legacy DB; {len(pdf_index)} pdfs in vault")

    resolved: list[dict] = []
    unresolved: list[dict] = []
    for p in papers:
        pdf = _match_pdf(p, pdf_index)
        md = (VAULT / p["note_path"]) if p.get("note_path") else None
        md_ok = md is not None and md.exists()
        if pdf is None:
            unresolved.append({**p, "reason": "no pdf match"})
            continue
        if not md_ok:
            unresolved.append({**p, "reason": "note_path missing or invalid", "pdf_path": str(pdf.relative_to(VAULT))})
            continue
        resolved.append({
            "paper_id": p["paper_id"],
            "title": p["title"],
            "pdf_path": str(pdf.relative_to(VAULT)),
            "md_path": str(md.relative_to(VAULT)),
            "arxiv_id": p.get("arxiv_id"),
            "doi": p.get("doi"),
            "year": p.get("year"),
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(sorted(resolved, key=lambda x: x["paper_id"]), indent=2))
    UNRESOLVED.write_text(json.dumps(unresolved, indent=2))

    print(f"resolved: {len(resolved)} → {OUT}")
    print(f"unresolved: {len(unresolved)} → {UNRESOLVED}")
    if unresolved:
        print("Inspect data/partitions_unresolved.json and fix manually if needed.")


if __name__ == "__main__":
    main()
