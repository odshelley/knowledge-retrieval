"""zotero_push: file this paper into the user's Zotero library under Alethograph/Papers.

Last asset in ingest_document. A Zotero failure never fails the run — the graph write has
already succeeded, and zotero_key staying null means scripts/backfill_zotero.py retries.
"""
from __future__ import annotations

import botocore.exceptions
from dagster import MaterializeResult, asset

from pipeline.runtime.partitions import documents_partitions_def
from pipeline.runtime.storage import RAW_BUCKET
from pipeline.zotero import push as zp
from pipeline.zotero.client import ZoteroClientError, ZoteroTransientError


def fetch_pdf(s3, key: str) -> bytes | None:
    """Returns None only when the object is genuinely absent. Any other ClientError
    (throttle, connection reset, a 500) is re-raised -- swallowing it here would let
    push_one treat a transient storage failure as "no PDF", write zotero_key on a
    complete=True result, and permanently strand the record without its attachment."""
    try:
        return s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")["Body"].read()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
        return None


def _result(out: dict) -> MaterializeResult:
    return MaterializeResult(metadata={
        "pushed": out["pushed"], "complete": out["complete"],
        "outcome": out["outcome"] or "", "zotero_key": out["zotero_key"] or "",
        "item_type": out["item_type"] or "", "filename": out["filename"] or "",
        "attachment": out["attachment"] or "", "reason": out["reason"] or "",
    })


@asset(partitions_def=documents_partitions_def(), deps=["paper_analysis"],
       required_resource_keys={"minio", "neo4j_new", "zotero"})
def zotero_push(context) -> MaterializeResult:
    key = context.partition_key
    zotero = context.resources.zotero
    if not zotero.configured:
        return MaterializeResult(metadata={"pushed": False,
                                           "reason": "Zotero not configured "
                                                     "(ZOTERO_API_KEY / ZOTERO_USER_ID)"})

    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(zp.PAPER_FOR_PUSH, document_id=key).single()
        if row is None:
            return MaterializeResult(metadata={"pushed": False, "reason": "no Paper node"})
        node, authors = dict(row["node"]), row["authors"]
        if node.get("zotero_key"):
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": "already in Zotero",
                                               "zotero_key": node["zotero_key"]})

        # ensure_collections() is OUTSIDE push_one's error handling and can raise on a
        # revoked key or sustained throttling. Guard it, or a Zotero outage fails the
        # whole ingest run — which this asset exists specifically not to do.
        client = zotero.get_client()
        try:
            collections = client.ensure_collections()
        except (ZoteroClientError, ZoteroTransientError) as exc:
            context.log.warning(f"Zotero unavailable, skipping push: {exc}")
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": f"Zotero unavailable: {exc}"})

        # A non-absent storage failure must not be swallowed as "no PDF" -- that would
        # let push_one return complete=True and strand the record without its
        # attachment, permanently, since zotero_key would then exclude it from repair.
        try:
            pdf = fetch_pdf(context.resources.minio.get_client(), key)
        except botocore.exceptions.ClientError as exc:
            context.log.warning(f"Zotero push: PDF fetch failed for {key}: {exc}")
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": f"PDF fetch failed: {exc}"})

        out = zp.push_one(client, collections["papers"], {**node, "kind": "paper"},
                          authors, pdf)

        # Only mark done when the attachment also landed — otherwise the repair query
        # must be able to find this record again.
        if out["complete"] and out["zotero_key"]:
            s.run(zp.MARK_PAPER_PUSHED, id=node["id"], key=out["zotero_key"])

    return _result(out)
