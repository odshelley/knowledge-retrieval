"""paper_analysis: Claude writes the structured analysis; stored as Summary node + MinIO JSON."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.analysis.analysis import PaperAnalysis, SYSTEM_PROMPT
from pipeline.runtime.partitions import documents_partitions_def
from pipeline.runtime.storage import ANALYSIS_BUCKET, PARSED_BUCKET, TRIAGE_BUCKET

WRITE_SUMMARY = """
MATCH (p:Paper {id:$paper_id})
MERGE (sm:Summary {id: $paper_id})
SET sm.json = $json
MERGE (p)-[:HAS_SUMMARY]->(sm)
"""

# Max markdown chars fed to the summary model. ~480k chars ≈ 120-140k tokens, which fits
# claude-sonnet-4-6's ~200k window with headroom for the system prompt and the 4k-token
# output — so all but the longest papers go in whole. Overflow is logged and flagged in
# metadata (never silent); a paper past this is truncated head-first, losing its tail.
MAX_ANALYSIS_CHARS = 480000


@asset(partitions_def=documents_partitions_def(),
       deps=["parsed_document", "extracted_graph", "triage_metadata"],
       required_resource_keys={"minio", "neo4j_new", "anthropic"})
def paper_analysis(context) -> MaterializeResult:
    key = context.partition_key  # document id
    s3 = context.resources.minio.get_client()
    md = s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.md")["Body"].read().decode("utf-8")
    paper_id = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.json")["Body"].read())["paper_id"]

    truncated = len(md) > MAX_ANALYSIS_CHARS
    if truncated:
        context.log.warning(
            f"{key}: analysis input truncated {len(md)}→{MAX_ANALYSIS_CHARS} chars "
            f"(paper tail dropped); summary may miss later sections.")

    client = context.resources.anthropic.get_client()
    resp = client.messages.parse(
        model=context.resources.anthropic.summary_model,
        max_tokens=16000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": md[:MAX_ANALYSIS_CHARS]}],
        output_format=PaperAnalysis,
        timeout=context.resources.anthropic.request_timeout,
    )
    # .parse() validates the json_schema text block into PaperAnalysis; store as the same
    # canonical note-template dict the downstream Summary node/MinIO blob expect.
    analysis = resp.parsed_output.model_dump()

    s3.put_object(Bucket=ANALYSIS_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(analysis).encode("utf-8"))
    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        s.run(WRITE_SUMMARY, paper_id=paper_id, json=json.dumps(analysis))
    return MaterializeResult(metadata={
        "analysis_key": f"{ANALYSIS_BUCKET}/{key}.json",
        "paper_id": paper_id,
        "input_chars": MetadataValue.int(len(md)),
        "truncated": truncated,
    })
