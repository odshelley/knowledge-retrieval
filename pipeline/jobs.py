from dagster import AssetSelection, define_asset_job

from pipeline.assets import chunks, extracted_graph, parsed_document, raw_blob, triage_metadata
from pipeline.assets import resolved_entities
from pipeline.assets import graph_write
from pipeline.assets import paper_analysis

ingest_document = define_asset_job(
    name="ingest_document",
    selection=AssetSelection.assets(raw_blob.raw_blob, parsed_document.parsed_document, chunks.chunks, triage_metadata.triage_metadata, extracted_graph.extracted_graph, resolved_entities.resolved_entities, graph_write.graph_write, paper_analysis.paper_analysis),
    description="Per-document ingestion across the asset graph (extended in later tasks).",
)
