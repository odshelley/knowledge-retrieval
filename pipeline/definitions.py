from dagster import Definitions

from pipeline.assets import (
    raw_blob, parsed_document, triage_metadata, chunks,
    extracted_graph, resolved_entities, graph_write, paper_analysis,
    book_raw_blob, book_parsed, book_metadata, book_structure, book_chunks,
    book_structure_write, book_chapter_extraction, book_chapter_resolved,
    book_chapter_graph_write,
)
from pipeline.runtime.jobs import ingest_document, ingest_book, extract_book_chapter
from pipeline.runtime.schedules import daily_ingest_schedule
from pipeline.runtime.sensors import books_sensor, book_chapters_sensor
from pipeline.runtime.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env, postgres_from_env,
)

defs = Definitions(
    assets=[
        raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
        chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
        graph_write.graph_write, paper_analysis.paper_analysis,
        book_raw_blob.book_raw_blob, book_parsed.book_parsed, book_metadata.book_metadata,
        book_structure.book_structure, book_chunks.book_chunks,
        book_structure_write.book_structure_write,
        book_chapter_extraction.book_chapter_extraction,
        book_chapter_resolved.book_chapter_resolved,
        book_chapter_graph_write.book_chapter_graph_write,
    ],
    jobs=[ingest_document, ingest_book, extract_book_chapter],
    schedules=[daily_ingest_schedule],
    sensors=[books_sensor, book_chapters_sensor],
    resources={
        "neo4j_new": new_neo4j_from_env(),
        "minio": minio_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
        "postgres": postgres_from_env(),
    },
)
