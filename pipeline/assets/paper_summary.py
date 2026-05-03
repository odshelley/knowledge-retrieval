from dagster import asset, AssetIn

from pipeline.partitions import partitions_def


@asset(
    partitions_def=partitions_def(),
    ins={"kg_extracted": AssetIn(), "structural_overlay": AssetIn()},
)
def paper_summary(context, kg_extracted, structural_overlay) -> dict:
    raise NotImplementedError
