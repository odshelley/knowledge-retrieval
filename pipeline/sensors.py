from __future__ import annotations

from dagster import (
    AssetSelection,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from pipeline.assets.kg_extracted import kg_extracted
from pipeline.assets.paper_summary import paper_summary
from pipeline.assets.pdf_blob import pdf_blob
from pipeline.assets.structural_overlay import structural_overlay
from pipeline.assets.v1_md_blob import v1_md_blob
from pipeline.partitions import paper_ids
from pipeline.storage import PDFS_BUCKET


def _key_to_partition(key: str) -> str:
    if not key.endswith(".pdf"):
        raise ValueError(f"unexpected key: {key}")
    return key[: -len(".pdf")]


def _list_new_keys(s3_client, since_ts: float) -> tuple[list[str], float]:
    resp = s3_client.list_objects_v2(Bucket=PDFS_BUCKET)
    contents = resp.get("Contents", []) or []
    new = []
    latest = since_ts
    for obj in contents:
        ts = obj["LastModified"].timestamp()
        if ts > since_ts:
            new.append(obj["Key"])
        if ts > latest:
            latest = ts
    return sorted(new), latest


@sensor(
    asset_selection=AssetSelection.assets(pdf_blob, v1_md_blob, kg_extracted, structural_overlay, paper_summary),
    minimum_interval_seconds=30,
    required_resource_keys={"minio"},
)
def minio_pdf_sensor(context: SensorEvaluationContext) -> SensorResult:
    """Watches MinIO `pdfs/`. Per new key, fires a RunRequest for the matching partition."""
    s3 = context.resources.minio.get_client()
    since_ts = float(context.cursor) if context.cursor else 0.0
    new_keys, latest = _list_new_keys(s3, since_ts)

    known = set(paper_ids())
    run_requests: list[RunRequest] = []
    for key in new_keys:
        try:
            partition = _key_to_partition(key)
        except ValueError:
            continue
        if partition not in known:
            context.log.warning(f"key {key} → partition {partition} not in data/partitions.json; skipping")
            continue
        run_requests.append(RunRequest(partition_key=partition, run_key=f"{partition}-{int(latest)}"))

    return SensorResult(run_requests=run_requests, cursor=str(latest))
