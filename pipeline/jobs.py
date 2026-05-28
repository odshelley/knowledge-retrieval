from dagster import AssetSelection, define_asset_job

from pipeline.assets import parsed_document, raw_blob

ingest_document = define_asset_job(
    name="ingest_document",
    selection=AssetSelection.assets(raw_blob.raw_blob, parsed_document.parsed_document),
    description="Per-document ingestion across the asset graph (extended in later tasks).",
)
