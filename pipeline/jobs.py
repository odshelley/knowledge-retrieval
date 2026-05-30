from dagster import AssetSelection, define_asset_job

from pipeline.assets import (
    raw_blob, parsed_document, triage_metadata, chunks,
    extracted_graph, resolved_entities, graph_write, paper_analysis,
)

ingest_document = define_asset_job(
    name="ingest_document",
    selection=AssetSelection.assets(
        raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
        chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
        graph_write.graph_write, paper_analysis.paper_analysis,
    ),
    description="Full per-document build: raw → parse → triage → chunk → extract → resolve → write → analyse.",
)
