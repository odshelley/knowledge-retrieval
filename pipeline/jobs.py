# pipeline/jobs.py
from __future__ import annotations

from dagster import AssetSelection, define_asset_job

from pipeline.assets.kg_extracted import kg_extracted
from pipeline.assets.legacy_mirror import legacy_graph_mirror
from pipeline.assets.paper_summary import paper_summary
from pipeline.assets.pdf_blob import pdf_blob
from pipeline.assets.structural_overlay import structural_overlay
from pipeline.assets.v1_md_blob import v1_md_blob

bulk_reingest = define_asset_job(
    name="bulk_reingest",
    selection=AssetSelection.assets(
        pdf_blob, v1_md_blob, legacy_graph_mirror,
        kg_extracted, structural_overlay, paper_summary,
    ),
    description="Materialize the entire pipeline across all partitions. Used for the initial bulk run.",
)

legacy_mirror_job = define_asset_job(
    name="legacy_mirror_job",
    selection=AssetSelection.assets(legacy_graph_mirror),
    description="One-shot: mirror the curated graph (Books, Papers, Concepts, Topics, "
                "Researchers, Ideas, Authors and all relationships) from the legacy DB into the new DB.",
)
