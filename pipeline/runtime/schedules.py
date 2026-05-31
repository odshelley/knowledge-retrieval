"""Daily schedule: scan SOURCE_DIR, register new dynamic partitions, request runs."""
from __future__ import annotations

from dagster import RunRequest, ScheduleEvaluationContext, schedule

from pipeline.runtime.partitions import DOCUMENTS_PARTITION
from pipeline.ingest.source import file_partition_key, list_pdf_files, source_dir


@schedule(cron_schedule="0 6 * * *", job_name="ingest_document", execution_timezone="Europe/London")
def daily_ingest_schedule(context: ScheduleEvaluationContext):
    existing = set(context.instance.get_dynamic_partitions(DOCUMENTS_PARTITION))
    requests = []
    new_keys = []
    for pdf in list_pdf_files(source_dir()):
        key = file_partition_key(pdf)
        if key in existing or key in new_keys:
            continue
        new_keys.append(key)
        requests.append(RunRequest(partition_key=key, run_key=key))
    if new_keys:
        context.instance.add_dynamic_partitions(DOCUMENTS_PARTITION, new_keys)
        context.log.info(f"registered {len(new_keys)} new document partitions")
    return requests
