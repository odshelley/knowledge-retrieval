"""triage_metadata: confirm paper, establish Paper identity, S2-enrich, write Paper+Author,
quarantine duplicate-paper-different-bytes, and stash references for graph_write's backfill."""
from __future__ import annotations

import json

from dagster import MaterializeResult, asset
from openai import OpenAI

from pipeline import research_port as rp
from pipeline.assets.parsed_document import QuarantineError
from pipeline.partitions import documents_partitions_def
from pipeline.storage import PARSED_BUCKET, TRIAGE_BUCKET

FRONTMATTER_PROMPT = (
    "You are extracting bibliographic metadata from the first page of a document. "
    "Return strict JSON: {\"is_paper\": bool, \"title\": str, \"authors\": [str], "
    "\"year\": int|null, \"arxiv_id\": str|null, \"doi\": str|null}. "
    "is_paper is false for non-papers (slides, notes, books)."
)
DUP_CHECK = "MATCH (p:Paper {id:$pid}) RETURN p.document_id AS doc"


def _extract_frontmatter(client, model: str, head: str) -> dict:
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": FRONTMATTER_PROMPT},
                  {"role": "user", "content": head[:6000]}],
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


@asset(partitions_def=documents_partitions_def(), deps=["parsed_document"],
       required_resource_keys={"minio", "neo4j_new", "openai"})
def triage_metadata(context) -> MaterializeResult:
    key = context.partition_key  # document id
    s3 = context.resources.minio.get_client()
    md = s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.md")["Body"].read().decode("utf-8")

    cfg = context.resources.openai
    client = OpenAI(api_key=cfg.api_key)
    fm = _extract_frontmatter(client, cfg.extraction_model, md)
    if not fm.get("is_paper"):
        raise QuarantineError(f"{key}: triage classified this document as not-a-paper — quarantined.")

    rec = None
    if fm.get("arxiv_id"):
        rec = rp.lookup_by_arxiv(fm["arxiv_id"])
    if rec is None and fm.get("doi"):
        rec = rp.lookup_by_doi(fm["doi"])
    rec = rec or {}

    doi = rec.get("doi") or fm.get("doi")
    arxiv = rec.get("arxiv_id") or fm.get("arxiv_id")
    title = rec.get("title") or fm.get("title")
    paper_id = rp.compute_paper_id(doi, arxiv, title)

    paper = {
        "id": paper_id, "document_id": key, "title": title,
        "year": rec.get("year") or fm.get("year"), "arxiv_id": arxiv, "doi": doi,
        "s2_id": rec.get("s2_id"), "abstract": rec.get("abstract"), "tldr": rec.get("tldr"),
        "citation_count": rec.get("citation_count"),
        "influential_citation_count": rec.get("influential_citation_count"),
        "authors": rec.get("authors") or [{"name": n, "s2_author_id": None}
                                          for n in (fm.get("authors") or [])],
    }

    new = context.resources.neo4j_new
    # Safe under the documented single-writer invariant (max_concurrent_runs=1, docker/dagster.yaml);
    # spec §7 defers a Postgres advisory lock to a future step if concurrency is ever restored.
    with new.get_driver().session(database=new.database) as s:
        row = s.run(DUP_CHECK, pid=paper_id).single()
        if row and row["doc"] and row["doc"] != key:
            raise QuarantineError(
                f"{key}: duplicate-paper-different-bytes — paper {paper_id} already "
                f"ingested from document {row['doc']}")
        s.run(rp.WRITE_PAPER, **paper)

    refs = rp.top_reference_records(rp.references(rec["s2_id"]), limit=3) if rec.get("s2_id") else []
    identifiers = {"s2_id": rec.get("s2_id"), "doi": doi, "arxiv_id": arxiv,
                   "title_norm": rp.normalize_title(title) if title else None}
    s3.put_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.json",
                  Body=json.dumps({"paper_id": paper_id, "s2_id": rec.get("s2_id"),
                                   "identifiers": identifiers, "references": refs}).encode("utf-8"))
    return MaterializeResult(metadata={"is_paper": True, "paper_id": paper_id,
                                       "references": len(refs)})
