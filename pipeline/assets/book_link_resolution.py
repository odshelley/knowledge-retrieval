"""book_link_resolution: per-book post-extraction linking pass (spec §5). Reads every
chapter payload from MinIO + the book's Result label index from Neo4j; resolves free-text
depends_on refs and proof locations into DEPENDS_ON / PROVED_IN edges. Deterministic
normalization first; one batched LLM call for the fuzzy residue; unmatched refs are
logged and dropped, never guessed."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.graph_write import WRITE_RESULT_DEPENDS, result_id
from pipeline.books.labels import build_label_index, unique_label_map
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import EXTRACTED_BUCKET, TRIAGE_BUCKET

FETCH_BOOK_RESULTS = """
MATCH (b:Book {id: $book_id})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(r:Result)
RETURN r.id AS id, r.name AS name, r.kind AS kind
"""

WRITE_PROVED_IN = """
UNWIND $rows AS row
  MATCH (r:Result {id: row.result_id})
  MATCH (c:Chunk)-[:PART_OF]->(:Section {id: row.section_id})
  WHERE c.position = row.position
  MERGE (r)-[:PROVED_IN]->(c)
"""

_FUZZY_PROMPT = (
    "Match each unresolved reference to at most one result label from the list, by meaning. "
    "Answer with a JSON object mapping reference string to the EXACT label string, omitting "
    "references that match nothing or are ambiguous. JSON only.\n\n"
    "Result labels:\n{labels}\n\nUnresolved references:\n{refs}"
)


def _fuzzy_resolve(client, model: str, refs: list[str], label_to_id: dict[str, str],
                   timeout: float) -> dict[str, str]:
    if not refs:
        return {}
    try:
        resp = client.messages.create(
            model=model, max_tokens=2048, timeout=timeout,
            messages=[{"role": "user", "content": _FUZZY_PROMPT.format(
                labels="\n".join(label_to_id), refs="\n".join(refs))}])
        text = next(b.text for b in resp.content if b.type == "text")
        raw = json.loads(text[text.index("{"):text.rindex("}") + 1])
        return {ref: label_to_id[lbl] for ref, lbl in raw.items() if lbl in label_to_id}
    except Exception:  # noqa: BLE001 — linking is best-effort; never sink the run
        return {}


@asset(partitions_def=books_partitions_def(),
       required_resource_keys={"minio", "neo4j_new", "anthropic"})
def book_link_resolution(context) -> MaterializeResult:
    sha = context.partition_key
    s3 = context.resources.minio.get_client()
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{sha}.structure.json")["Body"].read())
    book_id = structure["book_id"]

    new = context.resources.neo4j_new
    dep_rows, proved_rows = [], []
    unresolved: list[tuple[str, str]] = []  # (res_id, ref)
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        nodes = [dict(rec) for rec in s.run(FETCH_BOOK_RESULTS, book_id=book_id)]
        idx = build_label_index(nodes)
        label_to_id = unique_label_map(nodes)

        for ch in structure["chapters"]:
            key = ch["key"]
            try:
                payload = json.loads(s3.get_object(
                    Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json")["Body"].read())
            except Exception:  # noqa: BLE001 — chapter skipped by role: no payload
                continue
            owner = payload["chapter_id"]
            for sec in payload.get("sections", []):
                results = sec.get("results", [])
                for r in results:
                    rid = result_id(owner, r["kind"], r["statement"])
                    for ref in r.get("depends_on", []):
                        dep = idx.resolve(ref)
                        if dep and dep != rid:
                            dep_rows.append({"res_id": rid, "dep_id": dep})
                        elif not dep:
                            unresolved.append((rid, ref))
                for pr in sec.get("proof_chunks", []):
                    # result_key is (kind, normalized statement); result_id normalizes its
                    # statement argument internally (idempotently), so this equals the id the
                    # write path computed from the raw statement — no lookup table needed.
                    kind, norm_stmt = pr["result_key"]
                    rid = result_id(owner, kind, norm_stmt)
                    proved_rows.append({"result_id": rid,
                                        "section_id": sec["section_id"],
                                        "position": pr["position"]})

        # fuzzy residue — one batched call
        ar = context.resources.anthropic
        fuzzy = _fuzzy_resolve(ar.get_client(), ar.summary_model,
                               sorted({ref for _, ref in unresolved}),
                               label_to_id, timeout=ar.request_timeout)
        still_dropped = 0
        for rid, ref in unresolved:
            dep = fuzzy.get(ref)
            if dep and dep != rid:
                dep_rows.append({"res_id": rid, "dep_id": dep})
            else:
                still_dropped += 1
                context.log.info(f"link dropped: {ref!r} (no unique match)")

        s.run(WRITE_RESULT_DEPENDS, rows=dep_rows)
        s.run(WRITE_PROVED_IN, rows=proved_rows)

    return MaterializeResult(metadata={
        "depends_on_edges": MetadataValue.int(len(dep_rows)),
        "proved_in_edges": MetadataValue.int(len(proved_rows)),
        "dropped_refs": MetadataValue.int(still_dropped),
    })
