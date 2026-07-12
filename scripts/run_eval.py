"""Benchmark harness: Cypher ground truth + hybrid retrieval + two LLM judges.
Manual tool — not wired into CI (judge calls cost money and scores are noisy).
Usage: uv run python scripts/run_eval.py [--limit N]"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from pydantic import BaseModel

from server.graph import GraphClient
from server.retrieve import search_chunks_core
from server.settings import Settings

ANSWER_MODEL = os.environ.get("EVAL_ANSWER_MODEL", "gpt-5-nano")
JUDGE_MODEL = os.environ.get("EVAL_JUDGE_MODEL", "gpt-5-nano")


def settings_from_env() -> Settings:
    """Bridge: prefer the server's KG_* names, fall back to the builder .env's NEO4J_NEW_*."""
    load_dotenv()
    if "KG_NEO4J_URI" in os.environ:
        return Settings.from_env()
    return Settings(
        neo4j_uri=os.environ["NEO4J_NEW_URI"],
        neo4j_user=os.environ["NEO4J_NEW_USERNAME"],
        neo4j_password=os.environ["NEO4J_NEW_PASSWORD"],
        neo4j_database=os.environ.get("NEO4J_NEW_DATABASE", "neo4j"),
        openai_api_key=os.environ["OPENAI_API_KEY"],
    )


class Judgment(BaseModel):
    verdict: Literal["pass", "fail"]
    reason: str


ANSWER_SYSTEM = (
    "Answer the question using ONLY the provided context chunks. "
    "If the context does not contain the answer, say exactly: "
    "'The corpus does not contain this information.' Do not use outside knowledge."
)

RECALL_SYSTEM = (
    "You judge retrieval quality. Given a ground-truth answer and retrieved context, "
    "verdict='pass' iff the context contains the information needed to produce the "
    "ground truth. Judge the CONTEXT, not any generated answer."
)

CORRECTNESS_SYSTEM = (
    "You judge answer correctness. verdict='pass' iff the generated answer agrees with "
    "the ground truth. For refuse-questions (ground truth says info is unavailable/out of "
    "scope), pass iff the answer clearly declines rather than fabricating."
)


def judge(client, system: str, payload: str) -> Judgment:
    resp = client.chat.completions.parse(
        model=JUDGE_MODEL,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": payload}],
        response_format=Judgment,
        timeout=60,
    )
    return resp.choices[0].message.parsed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    bench = json.loads(Path("evals/benchmark.json").read_text())
    if args.limit:
        bench = bench[: args.limit]
    settings = settings_from_env()
    graph = GraphClient(settings)
    from openai import OpenAI
    oai = OpenAI(api_key=settings.openai_api_key)

    rows = []
    for item in bench:
        t0 = time.monotonic()
        gt_rows = graph.read(item["ground_truth_cypher"])
        retrieved = search_chunks_core(graph, item["question"], top_k=8, expand="local")
        context = "\n---\n".join(
            f"[{c['paper_title']} chunk {c['position']}] {c['text']}"
            for c in retrieved["chunks"])
        answer = oai.chat.completions.create(
            model=ANSWER_MODEL,
            messages=[{"role": "system", "content": ANSWER_SYSTEM},
                      {"role": "user",
                       "content": f"Context:\n{context}\n\nQuestion: {item['question']}"}],
            timeout=120,
        ).choices[0].message.content
        gt = json.dumps(gt_rows, default=str)
        recall = judge(oai, RECALL_SYSTEM,
                       f"Ground truth: {gt}\n\nRetrieved context:\n{context[:20000]}")
        correct = judge(oai, CORRECTNESS_SYSTEM,
                        f"Question: {item['question']}\nGround truth: {gt}\n"
                        f"Expected behavior: {item['expected_behavior']}\n"
                        f"Generated answer: {answer}")
        rows.append({
            "id": item["id"], "question": item["question"],
            "ground_truth": gt_rows, "answer": answer,
            "context_recall": recall.model_dump(),
            "answer_correctness": correct.model_dump(),
            "latency_s": round(time.monotonic() - t0, 1),
        })
        print(f"{item['id']:<20} recall={recall.verdict:<5} "
              f"correct={correct.verdict:<5} {rows[-1]['latency_s']}s")

    n = len(rows)
    summary = {
        "n": n,
        "context_recall": sum(r["context_recall"]["verdict"] == "pass" for r in rows) / n,
        "answer_correctness": sum(r["answer_correctness"]["verdict"] == "pass" for r in rows) / n,
        "answer_model": ANSWER_MODEL, "judge_model": JUDGE_MODEL,
    }
    out_dir = Path("evals/results")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y-%m-%d-%H%M%S")
    (out_dir / f"{stamp}.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, indent=2, default=str))
    print(json.dumps(summary, indent=2))
    graph.close()


if __name__ == "__main__":
    main()
