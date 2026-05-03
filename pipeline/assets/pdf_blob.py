from dagster import asset

from pipeline.partitions import partitions_def


@asset(partitions_def=partitions_def())
def pdf_blob(context) -> dict:
    raise NotImplementedError
