from dagster import asset, AssetIn

from pipeline.partitions import partitions_def


@asset(
    partitions_def=partitions_def(),
    ins={"kg_extracted": AssetIn()},
)
def structural_overlay(context, kg_extracted) -> dict:
    raise NotImplementedError
