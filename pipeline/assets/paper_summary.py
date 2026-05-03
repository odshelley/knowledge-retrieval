from __future__ import annotations

import json
import re

import anthropic
from dagster import AssetIn, MaterializeResult, MetadataValue, asset

from pipeline.partitions import get_partition, partitions_def

PROMPT_TEMPLATE = """\
You are summarising an academic paper from extracted text chunks.

Paper title: {title}
Paper id: {paper_id}

Extracted chunks (in source order):
---
{chunks}
---

Produce a JSON object with EXACTLY these six keys, each a plain-text paragraph:
- motivation: why this paper exists; what problem it addresses
- contributions: bullet-style enumeration of the headline contributions
- method: the core technical approach, in technical-but-accessible prose
- key_results: the empirical or theoretical results that justify the contributions
- limitations: what the paper itself acknowledges as open / unresolved
- related_work: how this paper situates itself relative to prior literature

Return ONLY the JSON object. No prose, no markdown fences.
"""


def build_summary_prompt(title: str, paper_id: str, chunks: list[str]) -> str:
    body = "\n\n".join(chunks)
    return PROMPT_TEMPLATE.format(title=title, paper_id=paper_id, chunks=body)


def parse_claude_response(text: str) -> dict:
    text = text.strip()
    fence = re.match(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    return json.loads(text)


FETCH_CHUNKS = """
MATCH (p:Paper {id: $paper_id})<-[:FROM_DOCUMENT]-(d:Document)-[:HAS_CHUNK]->(c:Chunk)
RETURN c.text AS text ORDER BY id(c)
"""

WRITE_SUMMARY = """
MATCH (p:Paper {id: $paper_id})
MERGE (s:Summary {paper_id: $paper_id})
SET s.motivation = $motivation,
    s.contributions = $contributions,
    s.method = $method,
    s.key_results = $key_results,
    s.limitations = $limitations,
    s.related_work = $related_work,
    s.model = $model,
    s.generated_at = datetime()
MERGE (p)-[:HAS_SUMMARY]->(s)
"""


@asset(
    partitions_def=partitions_def(),
    ins={"kg_extracted": AssetIn(), "structural_overlay": AssetIn()},
    required_resource_keys={"neo4j_new", "anthropic"},
)
def paper_summary(context, kg_extracted, structural_overlay) -> MaterializeResult:
    paper_id = context.partition_key
    part = get_partition(paper_id)
    if part is None:
        raise ValueError(f"unknown partition: {paper_id}")

    new = context.resources.neo4j_new
    a_cfg = context.resources.anthropic

    with new.get_driver().session(database=new.database) as s:
        chunks = [r["text"] for r in s.run(FETCH_CHUNKS, paper_id=paper_id) if r["text"]]
    if not chunks:
        raise RuntimeError(f"no chunks found for {paper_id}; did kg_extracted run?")

    prompt = build_summary_prompt(part["title"], paper_id, chunks[:80])
    client = anthropic.Anthropic(api_key=a_cfg.api_key)
    msg = client.messages.create(
        model=a_cfg.summary_model,
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text
    parsed = parse_claude_response(raw)

    with new.get_driver().session(database=new.database) as s:
        s.run(
            WRITE_SUMMARY,
            paper_id=paper_id,
            motivation=parsed["motivation"],
            contributions=parsed["contributions"],
            method=parsed["method"],
            key_results=parsed["key_results"],
            limitations=parsed["limitations"],
            related_work=parsed["related_work"],
            model=a_cfg.summary_model,
        )

    return MaterializeResult(
        metadata={
            "paper_id": paper_id,
            "model": a_cfg.summary_model,
            "chunk_count": MetadataValue.int(len(chunks)),
            "motivation_preview": parsed["motivation"][:200],
        },
    )
