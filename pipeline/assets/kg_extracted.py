from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import botocore.exceptions
from dagster import AssetIn, MaterializeResult, MetadataValue, asset
from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.experimental.components.text_splitters.fixed_size_splitter import (
    FixedSizeSplitter,
)
from neo4j_graphrag.experimental.pipeline.kg_builder import SimpleKGPipeline
from neo4j_graphrag.llm import OpenAILLM

from pipeline.partitions import get_partition, partitions_def
from pipeline.schema import NODE_TYPES, PATTERNS, RELATIONSHIP_TYPES


def _download(s3_client, bucket: str, key: str, dest: Path) -> bool:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return False
        raise
    with dest.open("wb") as f:
        for chunk in obj["Body"].iter_chunks(chunk_size=1 << 20):
            f.write(chunk)
    return True


async def _run_pipeline_for_file(
    driver, embedder, llm, text_splitter, file_path: Path, from_pdf: bool, database: str
) -> dict:
    kg = SimpleKGPipeline(
        llm=llm,
        driver=driver,
        neo4j_database=database,
        embedder=embedder,
        from_pdf=from_pdf,
        text_splitter=text_splitter,
        schema={
            "node_types": NODE_TYPES,
            "relationship_types": RELATIONSHIP_TYPES,
            "patterns": PATTERNS,
        },
    )
    result = await kg.run_async(file_path=str(file_path))
    return {"chunks": getattr(result, "chunks", None), "raw": str(result)[:200]}


@asset(
    partitions_def=partitions_def(),
    ins={"pdf_blob": AssetIn(), "v1_md_blob": AssetIn()},
    required_resource_keys={"minio", "neo4j_new", "openai"},
)
def kg_extracted(context, pdf_blob, v1_md_blob) -> MaterializeResult:
    """Runs SimpleKGPipeline on the PDF + v1 md (if present) for this paper."""
    paper_id = context.partition_key
    part = get_partition(paper_id)
    if part is None:
        raise ValueError(f"unknown partition: {paper_id}")

    s3 = context.resources.minio.get_client()
    new = context.resources.neo4j_new
    openai_cfg = context.resources.openai

    embedder = OpenAIEmbeddings(model=openai_cfg.embedding_model)
    llm = OpenAILLM(
        model_name=openai_cfg.extraction_model,
        model_params={"reasoning_effort": "minimal"},
    )
    splitter = FixedSizeSplitter(chunk_size=500, chunk_overlap=100)

    driver = new.get_driver()
    runs: list[dict] = []
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        pdf_local = td_path / f"{paper_id}.pdf"
        if not _download(s3, "pdfs", f"{paper_id}.pdf", pdf_local):
            raise RuntimeError(f"PDF missing in MinIO: {paper_id}.pdf")
        runs.append(asyncio.run(_run_pipeline_for_file(
            driver, embedder, llm, splitter, pdf_local, from_pdf=True, database=new.database
        )))

        md_local = td_path / f"{paper_id}.md"
        if _download(s3, "legacy-summaries", f"{paper_id}.md", md_local):
            runs.append(asyncio.run(_run_pipeline_for_file(
                driver, embedder, llm, splitter, md_local, from_pdf=False, database=new.database
            )))

    # MERGE the canonical Paper node here so structural_overlay can rely on it existing.
    with driver.session(database=new.database) as s:
        s.run(
            """
            MERGE (p:Paper {id: $paper_id})
            SET p.title = $title,
                p.arxiv_id = $arxiv_id,
                p.doi = $doi,
                p.year = $year
            """,
            paper_id=paper_id,
            title=part["title"],
            arxiv_id=part.get("arxiv_id"),
            doi=part.get("doi"),
            year=part.get("year"),
        )

    return MaterializeResult(
        metadata={
            "paper_id": paper_id,
            "files_processed": MetadataValue.int(len(runs)),
            "schema_version": "v1",
        },
    )
