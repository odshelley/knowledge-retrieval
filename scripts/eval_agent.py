"""Agent-mode answering for the eval harness: a bounded OpenAI tool-calling loop that
gives the answer model the same capabilities MCP clients have — search_chunks, get_schema,
and guarded run_cypher — implemented against server internals (no HTTP, hermetic to local
code + live graph). See docs/superpowers/plans/2026-07-12-eval-agent-mode-run-cypher.md."""
from __future__ import annotations

import json

from server import queries as q
from server.retrieve import search_chunks_core

MAX_TOOL_CALLS = 6

AGENT_ANSWER_SYSTEM = (
    "Answer the question using ONLY the provided tools. For counts, rankings, aggregates, "
    "or relationship questions, call get_schema then run_cypher rather than answering from "
    "chunk text. For definitional or descriptive questions, use search_chunks. If the tools "
    "cannot provide the answer, say exactly: 'The corpus does not contain this information.' "
    "Do not use outside knowledge."
)

TOOL_DEFS = [
    {"type": "function", "function": {
        "name": "search_chunks",
        "description": "Hybrid (vector + keyword) search over paper chunks.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"},
            "top_k": {"type": "integer", "default": 8},
        }, "required": ["query"]},
    }},
    {"type": "function", "function": {
        "name": "get_schema",
        "description": "The graph's node labels, relationship patterns, and key properties. "
                       "Read this before writing a run_cypher query.",
        "parameters": {"type": "object", "properties": {}},
    }},
    {"type": "function", "function": {
        "name": "run_cypher",
        "description": "Read-only Cypher for aggregations and questions no other tool covers "
                       "(counts, rankings, filters, multi-hop). Rows capped at 100, 15s timeout.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"},
        }, "required": ["query"]},
    }},
]


def execute_tool(graph, name: str, args: dict) -> str:
    """Run one eval tool against server internals; errors return as strings (the model
    sees them and can adapt), mirroring MCP tool-error semantics."""
    try:
        if name == "search_chunks":
            out = search_chunks_core(graph, args["query"], int(args.get("top_k", 8)), "local")
            return json.dumps(out, default=str)[:20000]
        if name == "get_schema":
            return q.render_schema()
        if name == "run_cypher":
            q.check_read_only(args["query"])
            rows, truncated = graph.read_limited(args["query"])
            return json.dumps({"rows": rows, "truncated": truncated}, default=str)[:20000]
        return f"error: unknown tool {name}"
    except Exception as exc:  # guard violations, timeouts, bad cypher — all surfaced to the model
        return f"error: {type(exc).__name__}: {exc}"


def answer_with_tools(oai, model: str, graph, question: str,
                      max_tool_calls: int = MAX_TOOL_CALLS) -> tuple[str, list[dict], str]:
    """Bounded tool loop. Returns (answer, tool_trace, evidence) where evidence is the
    concatenation of all tool outputs (recall is judged over it)."""
    messages = [{"role": "system", "content": AGENT_ANSWER_SYSTEM},
                {"role": "user", "content": f"Question: {question}"}]
    trace: list[dict] = []
    evidence_parts: list[str] = []
    calls = 0
    while True:
        force_answer = calls >= max_tool_calls
        resp = oai.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOL_DEFS,
            tool_choice="none" if force_answer else "auto",
            timeout=120,
        )
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return (msg.content or "", trace, "\n---\n".join(evidence_parts))
        messages.append({"role": "assistant", "content": msg.content,
                         "tool_calls": [tc.model_dump() for tc in msg.tool_calls]})
        for tc in msg.tool_calls:
            calls += 1
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            result = execute_tool(graph, tc.function.name, args)
            trace.append({"tool": tc.function.name, "args": args,
                          "result_chars": len(result),
                          "error": result[:200] if result.startswith("error:") else None})
            evidence_parts.append(f"[{tc.function.name}] {result}")
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
