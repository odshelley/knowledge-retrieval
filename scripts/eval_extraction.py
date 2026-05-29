"""Gate-B helper: run BOTH extraction providers over a parsed-markdown file and print a
side-by-side comparison of the concepts/definitions/results each surfaces.

Reads OPENAI_API_KEY and ANTHROPIC_API_KEY from the environment (and uses the default
extraction models on each resource). Usage:

    uv run python scripts/eval_extraction.py path/to/parsed.md
"""
from __future__ import annotations

import json
import sys

from pipeline.chunking import split_markdown
from pipeline.extraction import extract_from_chunk, merge_results
from pipeline.extraction_anthropic import extract_from_chunk_anthropic
from pipeline.resources import AnthropicResource, OpenAILLMResource


def _summary(res) -> dict:
    return {
        "concepts": [f"{c.name} ({c.kind})" for c in res.concepts],
        "definitions": [d.term for d in res.definitions],
        "results": [f"{r.name} [{r.kind}]" for r in res.results],
    }


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: eval_extraction.py <parsed_markdown_path>")
        sys.exit(1)
    with open(sys.argv[1], encoding="utf-8") as f:
        md = f.read()
    chunks = split_markdown(md)
    print(f"{len(chunks)} chunks\n")

    oa, an = OpenAILLMResource(), AnthropicResource()
    oclient, aclient = oa.get_client(), an.get_client()

    openai_res = merge_results(
        [extract_from_chunk(oclient, oa.extraction_model, c, timeout=oa.request_timeout) for c in chunks]
    )
    claude_res = merge_results(
        [extract_from_chunk_anthropic(aclient, an.extraction_model, c, timeout=an.request_timeout) for c in chunks]
    )

    print(f"=== OpenAI ({oa.extraction_model}) ===")
    print(json.dumps(_summary(openai_res), indent=2, ensure_ascii=False))
    print(f"\n=== Claude ({an.extraction_model}) ===")
    print(json.dumps(_summary(claude_res), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
