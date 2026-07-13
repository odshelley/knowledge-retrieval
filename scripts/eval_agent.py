"""Agent-mode answering for the eval harness: a bounded OpenAI tool-calling loop that
gives the answer model the same capabilities MCP clients have — search_chunks, get_schema,
and guarded run_cypher — implemented against server internals (no HTTP, hermetic to local
code + live graph). See docs/superpowers/plans/2026-07-12-eval-agent-mode-run-cypher.md."""
from __future__ import annotations

import json
import re

from server import queries as q
from server.retrieve import search_chunks_core

MAX_TOOL_CALLS = 6

_LABEL_TOKEN = re.compile(r":`?([A-Za-z_][A-Za-z0-9_]*)`?")
_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")
_KNOWN_TOKENS = set(q.NODE_TYPES) | {r for _, r, _ in q.PATTERNS}

BUDGET_EXHAUSTED = ("error: tool-call budget exhausted — answer now with the evidence "
                    "you already have.")

EMPTY_ROWS_HINT = (
    "0 rows returned. Check your labels, relationship types, and filters against the "
    "get_schema output (they are case-sensitive) and retry with a corrected query."
)


def validate_cypher_labels(cypher: str) -> str | None:
    """Reject queries referencing labels/relationship types the schema does not define —
    the dominant wrong-Cypher failure mode is a plausible-but-wrong label (e.g. :Theorem)
    silently matching nothing. Returns an error message, or None if all tokens are known."""
    unknown = sorted(
        {t for t in _LABEL_TOKEN.findall(_QUOTED.sub("''", cypher)) if t not in _KNOWN_TOKENS}
    )
    if unknown:
        return (f"error: unknown label(s)/relationship(s) {unknown} — the schema defines "
                f"labels {sorted(q.NODE_TYPES)} and relationships "
                f"{sorted({r for _, r, _ in q.PATTERNS})}. Fix the query and retry.")
    return None

AGENT_ANSWER_SYSTEM = (
    "Answer the question using ONLY the provided tools. For counts, rankings, aggregates, "
    "or relationship questions, call get_schema then run_cypher rather than answering from "
    "chunk text. For definitions of a named concept, call get_concept first (formal "
    "definitions live in Definition nodes, not chunks); for other descriptive questions, "
    "use search_chunks. For set questions (e.g. papers discussing BOTH X and Y), use "
    "run_cypher — get_concept paper lists are truncated and will miss members. If the tools "
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
        "name": "get_concept",
        "description": "A concept with its verbatim definitions (with source papers), papers "
                       "that discuss it, and related concepts. Use this for 'what is the "
                       "definition of X' questions — formal definitions live here, not in chunks.",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string"},
        }, "required": ["name"]},
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
        if name == "get_concept":
            rows = graph.read(q.GET_CONCEPT, name=args["name"])
            return json.dumps(rows[0] if rows else {"found": False, "name": args["name"]},
                              default=str)[:20000]
        if name == "get_schema":
            return q.render_schema()
        if name == "run_cypher":
            q.check_read_only(args["query"])
            label_err = validate_cypher_labels(args["query"])
            if label_err:
                return label_err
            rows, truncated = graph.read_limited(args["query"])
            if not rows:
                return json.dumps({"rows": [], "truncated": truncated,
                                   "hint": EMPTY_ROWS_HINT})
            return json.dumps({"rows": rows, "truncated": truncated}, default=str)[:20000]
        return f"error: unknown tool {name}"
    except Exception as exc:  # guard violations, timeouts, bad cypher — all surfaced to the model
        return f"error: {type(exc).__name__}: {exc}"


def answer_with_tools_anthropic(client, model: str, graph, question: str,
                                max_tool_calls: int = MAX_TOOL_CALLS
                                ) -> tuple[str, list[dict], str]:
    """Claude as the answer model — measures the production stack (MCP clients are Claude).
    Manual bounded loop: same tools, trace, and evidence semantics as the OpenAI path."""
    tools = [{"name": t["function"]["name"],
              "description": t["function"]["description"],
              "input_schema": t["function"]["parameters"]} for t in TOOL_DEFS]
    messages = [{"role": "user", "content": f"Question: {question}"}]
    trace: list[dict] = []
    evidence_parts: list[str] = []
    calls = 0
    while True:
        force_answer = calls >= max_tool_calls
        resp = client.messages.create(
            model=model,
            max_tokens=8192,
            thinking={"type": "adaptive"},
            system=AGENT_ANSWER_SYSTEM,
            tools=tools,
            tool_choice={"type": "none"} if force_answer else {"type": "auto"},
            messages=messages,
        )
        tool_uses = [b for b in resp.content if b.type == "tool_use"]
        if resp.stop_reason != "tool_use" or not tool_uses:
            text = next((b.text for b in resp.content if b.type == "text"), "")
            return (text, trace, "\n---\n".join(evidence_parts))
        messages.append({"role": "assistant", "content": resp.content})
        results = []
        for tu in tool_uses:
            calls += 1
            # A multi-tool-call response near the cap must still answer every tool_use id
            # (protocol requirement), but over-budget calls get a stub instead of executing.
            result = (BUDGET_EXHAUSTED if calls > max_tool_calls
                      else execute_tool(graph, tu.name, dict(tu.input)))
            trace.append({"tool": tu.name, "args": dict(tu.input),
                          "result_chars": len(result),
                          "error": result[:200] if result.startswith("error:") else None})
            evidence_parts.append(f"[{tu.name}] {result}")
            results.append({"type": "tool_result", "tool_use_id": tu.id, "content": result,
                            "is_error": result.startswith("error:")})
        messages.append({"role": "user", "content": results})


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
            # Over-budget calls in a multi-call response get a stub (every tool_call_id
            # still needs a tool message) instead of executing.
            result = (BUDGET_EXHAUSTED if calls > max_tool_calls
                      else execute_tool(graph, tc.function.name, args))
            trace.append({"tool": tc.function.name, "args": args,
                          "result_chars": len(result),
                          "error": result[:200] if result.startswith("error:") else None})
            evidence_parts.append(f"[{tc.function.name}] {result}")
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
