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

from pipeline.assets import (  # noqa: E402
    book_raw_blob, book_parsed, book_metadata, book_structure, book_chunks,
    book_structure_write, book_chapter_extraction, book_chapter_resolved,
    book_chapter_graph_write,
)

ingest_book = define_asset_job(
    name="ingest_book",
    selection=AssetSelection.assets(
        book_raw_blob.book_raw_blob, book_parsed.book_parsed, book_metadata.book_metadata,
        book_structure.book_structure, book_chunks.book_chunks,
        book_structure_write.book_structure_write,
    ),
    description="Book structure build: raw → parse(pages+toc) → metadata → structure → "
                "chunk+embed → write. RAG-ready; extraction follows per chapter.",
)

extract_book_chapter = define_asset_job(
    name="extract_book_chapter",
    selection=AssetSelection.assets(
        book_chapter_extraction.book_chapter_extraction,
        book_chapter_resolved.book_chapter_resolved,
        book_chapter_graph_write.book_chapter_graph_write,
    ),
    description="Per-chapter extraction: extract → resolve (shared ladder) → write.",
)
