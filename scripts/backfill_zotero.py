"""One-off backfill: push every ingested paper and book into Zotero under Alethograph.

Resumable — records already carrying a zotero_key are skipped, so an interrupted run can
simply be re-run. Fetches the library index once and reuses it for deduplication across
every record rather than issuing one search per item.

PREREQUISITE: run scripts/backfill_venue.py (without --dry-run) first. Without venue data
every existing paper files as a preprint, and the zotero_key guard prevents a re-run from
correcting it. This script checks for that and refuses rather than trusting the operator.

Run: uv run python scripts/backfill_zotero.py [--apply] [--limit N] [--skip-venue-check]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import botocore.exceptions
from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.assets.zotero_push import fetch_pdf
from pipeline.runtime.resources import minio_from_env
from pipeline.zotero import push as zp
from pipeline.zotero.client import ZoteroClient, ZoteroClientError

# Papers that COULD be enriched but have not been. A non-trivial count means
# backfill_venue.py has not run, and pushing now would file them all as preprints.
UNENRICHED = """
MATCH (p:Paper)
WHERE p.venue IS NULL AND p.journal_name IS NULL
  AND (p.arxiv_id IS NOT NULL OR p.doi IS NOT NULL)
RETURN count(p) AS n
"""
UNENRICHED_THRESHOLD = 10


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write to Zotero (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="process at most N records")
    ap.add_argument("--skip-venue-check", action="store_true",
                    help="proceed even if venue enrichment looks incomplete")
    args = ap.parse_args()

    load_dotenv()
    client = ZoteroClient(api_key=os.environ["ZOTERO_API_KEY"],
                          user_id=os.environ["ZOTERO_USER_ID"])
    s3 = minio_from_env().get_client()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    database = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")

    created = matched = deferred = failed = no_pdf = skipped = 0
    with driver, driver.session(database=database) as s:
        unenriched = s.run(UNENRICHED).single()["n"]
        if unenriched > UNENRICHED_THRESHOLD and not args.skip_venue_check:
            # A dry run writes nothing, to Zotero or to Neo4j, so it carries none of the
            # risk this guard exists for — only --apply can permanently mis-file the
            # corpus as preprints (the zotero_key marker then blocks a correcting re-run).
            # Previewing against unenriched data is exactly how an operator sanity-checks
            # the script before committing to a ~1.28 GB upload, so only --apply refuses.
            if args.apply:
                print(f"REFUSING: {unenriched} papers have an identifier but no venue data.\n"
                      f"Run `uv run python scripts/backfill_venue.py` first, or pass\n"
                      f"--skip-venue-check to proceed anyway. Pushing now would file these as\n"
                      f"preprints permanently — the zotero_key guard stops a re-run fixing it.")
                return 1
            print(f"WARNING: {unenriched} papers have an identifier but no venue data and\n"
                  f"would be filed as preprints. This preview reflects that unenriched state.\n"
                  f"--apply will refuse until `uv run python scripts/backfill_venue.py` has\n"
                  f"run, unless --skip-venue-check is passed.\n")

        collections = client.ensure_collections()
        print(f"collections: Papers={collections['papers']} Books={collections['books']}",
              flush=True)
        print("fetching library index for deduplication...", flush=True)
        index = client.library_index()
        print(f"  {len(index)} existing items indexed", flush=True)

        todo = [("paper", dict(r)) for r in s.run(zp.PAPERS_NEEDING_PUSH)]
        todo += [("book", dict(r)) for r in s.run(zp.BOOKS_NEEDING_PUSH)]
        if args.limit:
            todo = todo[:args.limit]
        print(f"{len(todo)} records to push\n", flush=True)

        for kind, ref in todo:
            query = zp.PAPER_FOR_PUSH if kind == "paper" else zp.BOOK_FOR_PUSH
            row = s.run(query, document_id=ref["document_id"]).single()
            if row is None:
                skipped += 1
                print(f"  SKIP     {ref['id']}  (node vanished)", flush=True)
                continue
            node, authors = dict(row["node"]), row["authors"]

            # Fetch AFTER the dry-run guard: pulling every PDF just to print DRY lines
            # would download the whole ~1.28 GB corpus out of MinIO for nothing.
            if not args.apply:
                print(f"  DRY      {kind:5} {node.get('title')!r}", flush=True)
                continue

            # A non-absent storage failure must not be swallowed as "no PDF" -- that
            # would let push_one return complete=True and strand the record without its
            # attachment, permanently, since zotero_key would then exclude it from repair.
            try:
                pdf = fetch_pdf(s3, ref["document_id"])
            except botocore.exceptions.ClientError as exc:
                deferred += 1
                print(f"  DEFERRED {node.get('title')!r}  (PDF fetch failed: {exc})",
                      flush=True)
                continue
            if pdf is None:
                no_pdf += 1

            try:
                out = zp.push_one(
                    client, collections["papers" if kind == "paper" else "books"],
                    {**node, "kind": kind}, authors, pdf, candidates=index)
            except ZoteroClientError as exc:
                # One malformed record must not abort a 174-item upload run.
                failed += 1
                print(f"  FAILED   {node.get('title')!r}  ({exc})", flush=True)
                continue

            if out["complete"] and out["zotero_key"]:
                s.run(zp.MARK_PAPER_PUSHED if kind == "paper" else zp.MARK_BOOK_PUSHED,
                      id=node["id"], key=out["zotero_key"])

            if not out["complete"]:
                deferred += 1
                print(f"  DEFERRED {node.get('title')!r}  "
                      f"({out['reason'] or out['attachment']})", flush=True)
            elif out["outcome"] == "created":
                created += 1
                print(f"  CREATED  {out['item_type']:16} {out['filename']}", flush=True)
            else:
                matched += 1
                print(f"  MATCHED  {out['zotero_key']}  {node.get('title')!r} "
                      f"[{out['attachment']}]", flush=True)

    print(f"\ncreated: {created}   matched existing: {matched}   "
          f"deferred (retry later): {deferred}   failed: {failed}   skipped: {skipped}   "
          f"missing PDF: {no_pdf} (subset of created/matched above, filed without a file)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
