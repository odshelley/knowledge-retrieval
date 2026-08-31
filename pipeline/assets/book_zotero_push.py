"""book_zotero_push: file this book into the user's Zotero library under Alethograph/Books.

Books flow through a separate asset chain with no Semantic Scholar lookup, so this mirrors
zotero_push rather than sharing an asset. The orchestration itself is shared via push_one.
"""
from __future__ import annotations

from dagster import MaterializeResult, asset

from pipeline.assets.zotero_push import _result, fetch_pdf
from pipeline.runtime.partitions import books_partitions_def
from pipeline.zotero import push as zp
from pipeline.zotero.client import ZoteroClientError, ZoteroTransientError


@asset(partitions_def=books_partitions_def(), deps=["book_structure_write"],
       required_resource_keys={"minio", "neo4j_new", "zotero"})
def book_zotero_push(context) -> MaterializeResult:
    key = context.partition_key
    zotero = context.resources.zotero
    if not zotero.configured:
        return MaterializeResult(metadata={"pushed": False,
                                           "reason": "Zotero not configured "
                                                     "(ZOTERO_API_KEY / ZOTERO_USER_ID)"})

    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(zp.BOOK_FOR_PUSH, document_id=key).single()
        if row is None:
            return MaterializeResult(metadata={"pushed": False, "reason": "no Book node"})
        node, authors = dict(row["node"]), row["authors"]
        if node.get("zotero_key"):
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": "already in Zotero",
                                               "zotero_key": node["zotero_key"]})

        client = zotero.get_client()
        try:
            collections = client.ensure_collections()
        except (ZoteroClientError, ZoteroTransientError) as exc:
            context.log.warning(f"Zotero unavailable, skipping push: {exc}")
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": f"Zotero unavailable: {exc}"})

        pdf = fetch_pdf(context.resources.minio.get_client(), key)
        out = zp.push_one(client, collections["books"], {**node, "kind": "book"},
                          authors, pdf)

        if out["complete"] and out["zotero_key"]:
            s.run(zp.MARK_BOOK_PUSHED, id=node["id"], key=out["zotero_key"])

    return _result(out)
