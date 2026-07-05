"""Book sensors: discover new book PDFs; auto-enqueue chapter extraction after structure
write. Sensors only submit runs — max_concurrent_runs=1 serializes actual execution."""
from __future__ import annotations

import os

from dagster import AssetKey, RunRequest, SensorEvaluationContext, SensorResult, SkipReason, sensor

from pipeline.ingest.source import books_source_dir, list_pdf_files, file_partition_key
from pipeline.runtime.partitions import BOOK_CHAPTERS_PARTITION, BOOKS_PARTITION


@sensor(job_name="ingest_book", minimum_interval_seconds=300)
def books_sensor(context: SensorEvaluationContext):
    if not os.environ.get("BOOKS_SOURCE_DIR"):
        return SkipReason("BOOKS_SOURCE_DIR not set — book ingestion disabled")
    existing = set(context.instance.get_dynamic_partitions(BOOKS_PARTITION))
    requests, new_keys = [], []
    for pdf in list_pdf_files(books_source_dir()):
        key = file_partition_key(pdf)
        if key in existing or key in new_keys:
            continue
        new_keys.append(key)
        requests.append(RunRequest(partition_key=key, run_key=key))
    if new_keys:
        context.instance.add_dynamic_partitions(BOOKS_PARTITION, new_keys)
        context.log.info(f"registered {len(new_keys)} new book partitions")
    return SensorResult(run_requests=requests)


@sensor(job_name="extract_book_chapter", minimum_interval_seconds=60)
def book_chapters_sensor(context: SensorEvaluationContext):
    instance = context.instance
    ready_books = set(instance.get_materialized_partitions(AssetKey("book_structure_write")))
    done_chapters = set(
        instance.get_materialized_partitions(AssetKey("book_chapter_graph_write")))
    requests = []
    for ck in sorted(instance.get_dynamic_partitions(BOOK_CHAPTERS_PARTITION)):
        book_sha = ck.rpartition(":ch")[0]
        if book_sha in ready_books and ck not in done_chapters:
            # run_key=ck → each chapter auto-requested exactly once; failed chapters are
            # re-run manually from the UI rather than retry-looped by the sensor.
            requests.append(RunRequest(partition_key=ck, run_key=ck))
    if not requests:
        return SkipReason("no chapters awaiting extraction")
    return SensorResult(run_requests=requests)
