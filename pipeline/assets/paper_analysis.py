"""paper_analysis: Claude writes the structured analysis; stored as Summary node + MinIO JSON."""
from __future__ import annotations

import json

from dagster import MaterializeResult, asset

from pipeline.analysis import SYSTEM_PROMPT, strip_to_json, validate_analysis
from pipeline.partitions import documents_partitions_def
from pipeline.storage import ANALYSIS_BUCKET, PARSED_BUCKET, TRIAGE_BUCKET

WRITE_SUMMARY = """
MATCH (p:Paper {id:$paper_id})
MERGE (sm:Summary {id: $paper_id})
SET sm.json = $json
MERGE (p)-[:HAS_SUMMARY]->(sm)
"""


@asset(partitions_def=documents_partitions_def(),
       deps=["parsed_document", "extracted_graph", "triage_metadata"],
       required_resource_keys={"minio", "neo4j_new", "anthropic"})
def paper_analysis(context) -> MaterializeResult:
    key = context.partition_key  # document id
    s3 = context.resources.minio.get_client()
    md = s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.md")["Body"].read().decode("utf-8")
    paper_id = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.json")["Body"].read())["paper_id"]

    client = context.resources.anthropic.get_client()
    msg = client.messages.create(
        model=context.resources.anthropic.summary_model,
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": md[:120000]}],
        timeout=context.resources.anthropic.request_timeout,
    )
    text_blocks = [b.text for b in msg.content if getattr(b, "type", None) == "text"]
    if not text_blocks:
        raise ValueError("anthropic response did not include a text content block")
    raw = "".join(text_blocks)
    analysis = validate_analysis(json.loads(strip_to_json(raw)))

    s3.put_object(Bucket=ANALYSIS_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(analysis).encode("utf-8"))
    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        s.run(WRITE_SUMMARY, paper_id=paper_id, json=json.dumps(analysis))
    return MaterializeResult(metadata={"analysis_key": f"{ANALYSIS_BUCKET}/{key}.json",
                                       "paper_id": paper_id})
