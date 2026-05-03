from dagster import asset, AssetIn

from pipeline.partitions import partitions_def


@asset(
    partitions_def=partitions_def(),
    ins={"pdf_blob": AssetIn(), "v1_md_blob": AssetIn()},
)
def kg_extracted(context, pdf_blob, v1_md_blob) -> dict:
    raise NotImplementedError
