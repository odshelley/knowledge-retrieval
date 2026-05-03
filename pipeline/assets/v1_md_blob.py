from dagster import asset

from pipeline.partitions import partitions_def


@asset(partitions_def=partitions_def())
def v1_md_blob(context) -> dict:
    raise NotImplementedError
