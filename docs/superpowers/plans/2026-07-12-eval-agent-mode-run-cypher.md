# Eval agent mode: wire run_cypher into the benchmark harness

**Status: implemented** (branch `feat/eval-agent-mode`). Follow-up to PR #18.
First live comparison (2026-07-13, same corpus, gpt-5-nano): answer_correctness
0.20 (retrieval) → 0.40 (agent); graph-only slice 0.0 → 0.40.

## Problem

13 of 16 benchmark items are `retrieval_answerable: false`: aggregates ("how many
papers"), rankings ("most-cited paper"), and multi-hop graph questions. The harness
answers every question from `search_chunks` context only, so these 13 structurally
cannot pass — the answer model never sees a path to the graph. That is why the
baseline recall is measured over only 3 definitional items and why answer
correctness is capped low. The server already ships the missing capability
(`get_schema` + guarded `run_cypher`); the eval just doesn't exercise it.

## Proposal

Add `--mode {retrieval,agent}` to `scripts/run_eval.py` (default `retrieval`,
preserving trend continuity with the existing baseline).

### Agent mode

Replace the single answer call with an OpenAI tool-calling loop. Expose exactly
three functions, mirroring the MCP tools but calling server internals directly
(no HTTP, hermetic to local code + live graph):

| eval tool | implementation |
|---|---|
| `search_chunks(query, top_k, expand)` | `search_chunks_core(graph, ...)` |
| `get_schema()` | `q.render_schema()` |
| `run_cypher(query)` | `q.check_read_only(query)` then `graph.read_limited(query)` |

Loop mechanics:

- Max **6 tool calls** per item, then a forced final answer (tool_choice="none").
- `run_cypher` guard violations (write clauses, timeout) are returned to the model
  as tool-result error strings, not raised — the eval then also exercises the
  guard path end to end.
- Each item's JSONL row gains a `tool_trace`: list of `{tool, args, result_chars,
  error}` for debugging which path answered the question.
- `ANSWER_SYSTEM` in agent mode: same grounding rule, plus "for counts, rankings,
  or relationship questions, call get_schema then run_cypher rather than
  answering from chunk text."

### Metrics

- `answer_correctness`: unchanged judge, now reported over **all** items in agent
  mode — this is the headline number the change is meant to move.
- `context_recall`: judged over the concatenated outputs of *all* tool calls
  (not just chunks). In agent mode this is meaningful for every item, so report
  it over all items; keep the `retrieval_answerable` split as a slice.
- Summary JSON gains `mode` and a per-slice breakdown:
  `{retrieval_answerable: {...}, graph_only: {...}, refuse: {...}}`.

### Non-goals

- No change to `evals/benchmark.json` (flags stay as metadata; refuse items keep
  `expected_behavior: refuse` and must still be declined even with cypher access).
- No CI wiring — stays a manual paid tool.
- Not exposing all 11 MCP tools: three keeps variance and cost down; widen later
  if traces show a need (`get_concept` is the likeliest fourth).

## Testing

Unit tests with a fake OpenAI client (scripted tool-call sequences) and a fake
graph: dispatch, 6-call cap + forced answer, guard-violation surfaced as tool
error, trace recorded, retrieval mode untouched. No judge calls in tests.

## Cost

gpt-5-nano, 16 items x <=7 LLM calls + 2 judges: cents per run, same order as the
current harness.

## Sequencing

Implement after the post-migration retrieval baseline is recorded (step 4 of the
2026-07-12 ops plan), so the before/after comparison is: same corpus, retrieval
mode vs agent mode.
