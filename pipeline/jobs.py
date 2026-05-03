# pipeline/jobs.py
from __future__ import annotations

from dagster import AssetSelection, define_asset_job

from pipeline.assets.kg_extracted import kg_extracted
from pipeline.assets.paper_summary import paper_summary
from pipeline.assets.pdf_blob import pdf_blob
from pipeline.assets.structural_overlay import structural_overlay
from pipeline.assets.v1_md_blob import v1_md_blob

bulk_reingest = define_asset_job(
    name="bulk_reingest",
    selection=AssetSelection.assets(
        pdf_blob, v1_md_blob, kg_extracted, structural_overlay, paper_summary
    ),
    description="Materialize the entire pipeline across all partitions. Used for the initial bulk run.",
)
