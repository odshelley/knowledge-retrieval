# GraphRAG Augmentations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the five augmentations agreed after the Essential-GraphRAG comparison: (1) hybrid keyword+vector chunk search, (2) a Cypher-ground-truth eval benchmark, (3) chunk-level provenance edges, (4) concept descriptions with an entity vector index and a `search_concepts` tool, (5) a guarded read-only `run_cypher` escape hatch.

**Architecture:** All changes (builder: schema, extraction, graph_write, backfill scripts; server: queries, tools, eval harness) land on ONE branch off `main` — the kg v1 MCP server merged to main in PR #14 (2026-07-12), so `server/` and `tests/server/` now live there. Nothing changes the single-writer invariant: `graph_write` stays the sole pipeline writer; backfill scripts are manual, run-once, and documented as such.

**Tech Stack:** Python 3.12 / uv, Dagster, Neo4j Aura (Cypher, native vector + full-text indexes), Postgres+pgvector, OpenAI SDK (structured outputs via `.parse()`), FastMCP, pytest.

## Global Constraints

- Run all tests with `uv run pytest <path> -v` from the relevant checkout root.
- Embedding model is pinned: `text-embedding-3-small`, 1536 dims, cosine. Do not introduce a second model or dimension anywhere.
- NEVER run `scripts/reset_graph.py` (wipes the live Aura DB). `scripts/init_neo4j.py` is safe: every statement is `IF NOT EXISTS`.
- Neo4j here is live Aura (credentials via `.env`). Integration tests are gated behind `--run-integration`; plain `pytest` must pass with no network.
- `graph_write` is the SOLE pipeline writer of the derived graph (spec §5.9). Backfill scripts in this plan write to Neo4j but are manual one-off scripts run while the Dagster daily schedule is idle; say so in each script's docstring.
- All tasks run on ONE branch, `feat/graphrag-augmentations`, off `origin/main` (which includes the kg v1 server merge, PR #14). Execute tasks in rollout order — later tasks edit files earlier tasks touched.
- Task 1 must be deployed to the live DB (index exists) before Task 2's integration test or any production use of hybrid search.
- The MCP server deploys to Fly.io (`kg-graph.fly.dev`); redeploy (`fly deploy` from the worktree) is a human/ops step, note it in PR descriptions rather than running it.
- Commit messages: conventional prefixes (`feat:`, `fix:`, `test:`, `docs:`) matching repo history.

---

### Task 1: Full-text index on Chunk.text (builder)

**Files:**
- Modify: `pipeline/graph/schema.py` (INIT_CYPHER, lines 141–164 region)
- Test: `tests/test_schema.py`

**Interfaces:**
- Produces: a Neo4j full-text index named `chunk_text` on `Chunk.text`. Task 2 queries it by exactly this name.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_schema.py`:

```python
def test_fulltext_chunk_index_in_init():
    from pipeline.graph.schema import iter_init_statements
    stmts = iter_init_statements()
    assert any("FULLTEXT INDEX chunk_text" in s for s in stmts), (
        "INIT_CYPHER must create the chunk_text full-text index (hybrid search depends on it)"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_schema.py::test_fulltext_chunk_index_in_init -v`
Expected: FAIL (assertion — no statement contains "FULLTEXT INDEX chunk_text")

- [ ] **Step 3: Add the index to INIT_CYPHER**

In `pipeline/graph/schema.py`, inside the `INIT_CYPHER` string, immediately after the `chunk_embedding` vector-index statement (after its closing `};`), insert:

```
CREATE FULLTEXT INDEX chunk_text IF NOT EXISTS
  FOR (c:Chunk) ON EACH [c.text];
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_schema.py -v`
Expected: all PASS (existing statement-splitting tests must still pass — the new statement ends with `;` like the others, so `iter_init_statements` picks it up).

- [ ] **Step 5: Apply to the live DB**

Run: `uv run python scripts/init_neo4j.py`
Expected: exits 0. Verify with a one-off check:

```bash
uv run python - <<'EOF'
from pipeline.resources import *  # noqa
# simplest: reuse the script's own connection pattern
EOF
```

If a quick programmatic check is awkward, verify in Neo4j Browser / Aura console: `SHOW INDEXES YIELD name WHERE name = 'chunk_text'` returns one row with state ONLINE. (Population of an index over existing chunks takes a moment; ONLINE means done.)

- [ ] **Step 6: Commit**

```bash
git add pipeline/graph/schema.py tests/test_schema.py
git commit -m "feat(schema): add chunk_text full-text index for hybrid search"
```

---

### Task 2: Hybrid search in `search_chunks` (server)

**Files:**
- Modify: `server/queries.py`
- Modify: `server/tools.py` (search_chunks, lines 15–35)
- Test: `tests/server/test_queries.py`

**Interfaces:**
- Consumes: `chunk_text` full-text index from Task 1.
- Produces: `queries.lucene_escape(q: str) -> str`; `queries.FULLTEXT_SEARCH` (params `$q, $paper_id, $top_k`); `queries.merge_chunk_hits(vector_rows: list[dict], fulltext_rows: list[dict], top_k: int) -> list[dict]`. Task 3's eval harness reuses the updated `search_chunks` behaviour.

- [ ] **Step 1: Write the failing tests**

Append to `tests/server/test_queries.py`:

```python
from server import queries as q


def test_lucene_escape_neutralizes_operators():
    assert q.lucene_escape("a+b (c) OR d/e") == "a\\+b \\(c\\) OR d\\/e"
    assert q.lucene_escape("") == ""
    assert q.lucene_escape("plain words") == "plain words"


def _row(cid, score):
    return {"chunk_id": cid, "score": score, "text": "t", "position": 0,
            "paper_id": "p", "paper_title": "T", "year": 2024}


def test_merge_chunk_hits_normalizes_and_dedups():
    vec = [_row("a", 0.90), _row("b", 0.45)]
    ft = [_row("b", 12.0), _row("c", 6.0)]
    out = q.merge_chunk_hits(vec, ft, top_k=3)
    ids = [r["chunk_id"] for r in out]
    # a: 0.90/0.90 = 1.0; b: max(0.45/0.90, 12/12) = 1.0; c: 6/12 = 0.5
    assert set(ids[:2]) == {"a", "b"}
    assert ids[2] == "c"
    assert out[2]["score"] == 0.5
    assert len(out) == 3


def test_merge_chunk_hits_handles_empty_sides():
    assert q.merge_chunk_hits([], [], 5) == []
    only_vec = q.merge_chunk_hits([_row("a", 0.8)], [], 5)
    assert only_vec[0]["score"] == 1.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/server/test_queries.py -v -k "lucene or merge_chunk"`
Expected: FAIL with `AttributeError: module 'server.queries' has no attribute 'lucene_escape'`

- [ ] **Step 3: Implement in `server/queries.py`**

Add after `merge_paper_hits`:

```python
_LUCENE_SPECIAL = set('+-&|!(){}[]^"~*?:\\/')


def lucene_escape(q: str) -> str:
    """Escape Lucene query operators so a natural-language question is a literal term query."""
    return "".join("\\" + ch if ch in _LUCENE_SPECIAL else ch for ch in q)


def merge_chunk_hits(vector_rows: list[dict], fulltext_rows: list[dict],
                     top_k: int) -> list[dict]:
    """Hybrid merge: normalize each list by its own max score (vector scores are 0..1 cosine,
    Lucene scores are unbounded, so raw scores are incomparable), union, dedup by chunk_id
    keeping the best normalized score, sort descending, cut to top_k."""
    def normalized(rows: list[dict]) -> list[dict]:
        if not rows:
            return []
        mx = max(r["score"] for r in rows) or 1.0
        return [{**r, "score": r["score"] / mx} for r in rows]

    best: dict[str, dict] = {}
    for r in normalized(vector_rows) + normalized(fulltext_rows):
        cur = best.get(r["chunk_id"])
        if cur is None or r["score"] > cur["score"]:
            best[r["chunk_id"]] = r
    return sorted(best.values(), key=lambda r: -r["score"])[:top_k]


FULLTEXT_SEARCH = """
CALL db.index.fulltext.queryNodes('chunk_text', $q) YIELD node, score
MATCH (node)-[:BELONGS_TO]->(:Document)<-[:HAS_DOCUMENT]-(p:Paper)
WHERE $paper_id IS NULL OR p.id = $paper_id
RETURN node.id AS chunk_id, node.text AS text, node.position AS position, score,
       p.id AS paper_id, p.title AS paper_title, p.year AS year
ORDER BY score DESC
LIMIT $top_k
"""
```

- [ ] **Step 4: Wire into `search_chunks` in `server/tools.py`**

Replace the body between `emb = graph.embed(query)` and `out: dict = {"chunks": hits}` so both searches run and merge:

```python
        emb = graph.embed(query)
        k = top_k * 4 if paper_id else top_k
        vec_hits = graph.read(q.VECTOR_SEARCH, k=k, top_k=top_k,
                              embedding=emb, paper_id=paper_id)
        ft_hits = graph.read(q.FULLTEXT_SEARCH, q=q.lucene_escape(query),
                             paper_id=paper_id, top_k=top_k)
        hits = q.merge_chunk_hits(vec_hits, ft_hits, top_k)
```

Update the tool docstring first line to: `"""Hybrid (vector + keyword) search over paper chunks; ..."""` — the rest unchanged.

- [ ] **Step 5: Run the full server unit suite**

Run: `uv run pytest tests/server -v`
Expected: all PASS. (`test_app.py` uses a fake driver; the extra `graph.read` call must not break it — the fake returns `[]` for unknown queries, and `merge_chunk_hits([], [], k)` handles that. If a fake asserts call counts, update it to expect the second read.)

- [ ] **Step 6: Integration check (requires Task 1 deployed)**

Run: `uv run pytest tests/server/test_integration_server.py -v --run-integration -k search_chunks`
Expected: PASS; non-empty chunks for a known query.

- [ ] **Step 7: Commit**

```bash
git add server/queries.py server/tools.py tests/server/test_queries.py
git commit -m "feat(server): hybrid vector+fulltext search_chunks with normalized score merge"
```

---

### Task 3: Eval benchmark with Cypher ground truth (server)

**Files:**
- Create: `evals/benchmark.json`
- Create: `evals/README.md`
- Create: `server/retrieve.py`
- Create: `scripts/run_eval.py`
- Modify: `server/tools.py` (delegate search_chunks core to retrieve.py)
- Test: `tests/server/test_retrieve.py`

**Interfaces:**
- Consumes: `merge_chunk_hits`, `FULLTEXT_SEARCH`, `VECTOR_SEARCH`, `EXPAND_LOCAL` from `server/queries.py`; `GraphClient` from `server/graph.py`.
- Produces: `retrieve.search_chunks_core(graph, query: str, top_k: int, expand: str, paper_id: str | None) -> dict` used by both the MCP tool and the eval harness; `evals/benchmark.json` schema: list of `{id, question, ground_truth_cypher, expected_behavior}` where `expected_behavior` is `"answer"` or `"refuse"`.

- [ ] **Step 1: Extract the retrieval core (refactor, tests stay green)**

Create `server/retrieve.py`:

```python
"""Retrieval core shared by the MCP tools and the eval harness (scripts/run_eval.py).
Keeping this out of tools.py lets the harness exercise the EXACT production path
without standing up FastMCP."""
from __future__ import annotations

from server import queries as q
from server.graph import GraphClient


def search_chunks_core(graph: GraphClient, query: str, top_k: int = 8,
                       expand: str = "local", paper_id: str | None = None) -> dict:
    top_k = q.validate_top_k(top_k)
    expand = q.validate_expand(expand)
    emb = graph.embed(query)
    k = top_k * 4 if paper_id else top_k
    vec_hits = graph.read(q.VECTOR_SEARCH, k=k, top_k=top_k,
                          embedding=emb, paper_id=paper_id)
    ft_hits = graph.read(q.FULLTEXT_SEARCH, q=q.lucene_escape(query),
                         paper_id=paper_id, top_k=top_k)
    hits = q.merge_chunk_hits(vec_hits, ft_hits, top_k)
    out: dict = {"chunks": hits}
    paper_ids = sorted({h["paper_id"] for h in hits})
    if expand == "local" and paper_ids:
        out["papers"] = graph.read(q.EXPAND_LOCAL, paper_ids=paper_ids)
    elif expand == "concepts" and paper_ids:
        top = graph.read(q.TOP_CONCEPTS_FOR_PAPERS, paper_ids=paper_ids)
        out["concepts"] = graph.read(q.EXPAND_CONCEPTS,
                                     names=[t["name"] for t in top])
    return out
```

In `server/tools.py`, replace the `search_chunks` body with a delegation (keep the docstring and the validation-free signature):

```python
    @mcp.tool()
    def search_chunks(query: str, top_k: int = 8, expand: str = "local",
                      paper_id: str | None = None) -> dict:
        """Hybrid (vector + keyword) search over paper chunks; expand='local' adds each hit
        paper's concepts, definitions, results, and CITES neighbours; expand='concepts'
        pivots to the top concepts across hits. Cite results as (paper_title, chunk position)."""
        return search_chunks_core(graph, query, top_k, expand, paper_id)
```

with the import `from server.retrieve import search_chunks_core` at the top of `tools.py`.

- [ ] **Step 2: Run server suite to confirm the refactor is invisible**

Run: `uv run pytest tests/server -v`
Expected: all PASS.

- [ ] **Step 3: Write `tests/server/test_retrieve.py`**

```python
from server.retrieve import search_chunks_core


class FakeGraph:
    """Returns canned rows per query constant; records calls."""
    def __init__(self, rows_by_query):
        self.rows_by_query = rows_by_query
        self.calls = []

    def embed(self, text):
        return [0.0] * 1536

    def read(self, cypher, **params):
        self.calls.append(cypher)
        for key, rows in self.rows_by_query.items():
            if key in cypher:
                return rows
        return []


def _chunk(cid, pid, score):
    return {"chunk_id": cid, "score": score, "text": "t", "position": 1,
            "paper_id": pid, "paper_title": "T", "year": 2024}


def test_core_merges_and_expands_local():
    g = FakeGraph({
        "db.index.vector.queryNodes('chunk_embedding'": [_chunk("c1", "p1", 0.9)],
        "db.index.fulltext.queryNodes('chunk_text'": [_chunk("c2", "p2", 8.0)],
        "UNWIND $paper_ids AS pid": [{"paper_id": "p1"}, {"paper_id": "p2"}],
    })
    out = search_chunks_core(g, "girsanov theorem", top_k=5, expand="local")
    assert {c["chunk_id"] for c in out["chunks"]} == {"c1", "c2"}
    assert "papers" in out


def test_core_expand_none_skips_expansion():
    g = FakeGraph({"db.index.vector.queryNodes('chunk_embedding'": [_chunk("c1", "p1", 0.9)]})
    out = search_chunks_core(g, "q", top_k=3, expand="none")
    assert list(out.keys()) == ["chunks"]
```

Run: `uv run pytest tests/server/test_retrieve.py -v` — Expected: PASS.

- [ ] **Step 4: Author the benchmark**

Create `evals/benchmark.json`. Ten structural questions are fully specified below; then add five corpus-content questions by the documented procedure in `evals/README.md` (step 5). Structural entries:

```json
[
  {"id": "count-papers", "question": "How many papers are in the corpus?",
   "ground_truth_cypher": "MATCH (p:Paper) RETURN count(p) AS answer",
   "expected_behavior": "answer"},
  {"id": "most-cited", "question": "Which paper in the corpus is cited by the most other papers in the corpus?",
   "ground_truth_cypher": "MATCH (:Paper)-[:CITES]->(p:Paper) RETURN p.title AS answer, count(*) AS n ORDER BY n DESC LIMIT 1",
   "expected_behavior": "answer"},
  {"id": "top-concept", "question": "Which concept is discussed by the most papers?",
   "ground_truth_cypher": "MATCH (p:Paper)-[:DISCUSSES]->(c:Concept) RETURN c.name AS answer, count(p) AS n ORDER BY n DESC LIMIT 1",
   "expected_behavior": "answer"},
  {"id": "newest-paper", "question": "What is the most recent paper in the corpus?",
   "ground_truth_cypher": "MATCH (p:Paper) RETURN p.title AS answer ORDER BY coalesce(p.year,0) DESC LIMIT 1",
   "expected_behavior": "answer"},
  {"id": "theorem-count", "question": "How many theorems (as opposed to lemmas or propositions) have been extracted across the corpus?",
   "ground_truth_cypher": "MATCH (r:Result {kind:'theorem'}) RETURN count(r) AS answer",
   "expected_behavior": "answer"},
  {"id": "case-variant", "question": "which papers discuss brownian motion?",
   "ground_truth_cypher": "MATCH (p:Paper)-[:DISCUSSES]->(c:Concept) WHERE toLower(c.name) = 'brownian motion' RETURN collect(p.title) AS answer",
   "expected_behavior": "answer"},
  {"id": "multi-hop-deps", "question": "Pick any theorem that depends on at least one other result and name what it depends on.",
   "ground_truth_cypher": "MATCH (r:Result)-[:DEPENDS_ON]->(d:Result) RETURN r.name AS theorem, collect(d.name) AS answer LIMIT 1",
   "expected_behavior": "answer"},
  {"id": "missing-oscars", "question": "Which paper in the corpus won a Nobel prize?",
   "ground_truth_cypher": "RETURN 'This information is not in the corpus' AS answer",
   "expected_behavior": "refuse"},
  {"id": "missing-offtopic", "question": "What is the current price of NVDA stock?",
   "ground_truth_cypher": "RETURN 'Out of scope for this corpus' AS answer",
   "expected_behavior": "refuse"},
  {"id": "author-most", "question": "Which author has the most papers in the corpus?",
   "ground_truth_cypher": "MATCH (a:Author)-[:AUTHORED]->(:Paper) RETURN a.name AS answer, count(*) AS n ORDER BY n DESC LIMIT 1",
   "expected_behavior": "answer"}
]
```

- [ ] **Step 5: Write `evals/README.md`** (includes the procedure for the five content questions)

```markdown
# Retrieval/answer benchmark

Ground truth is a Cypher query executed against the live graph at eval time
(the book's "graph as oracle" pattern), so the benchmark stays valid as papers land.

Run: `uv run python scripts/run_eval.py` (needs .env with NEO4J_* and OPENAI_API_KEY).
Results land in `evals/results/<timestamp>.json` and print as a table.

## Adding content questions (do once, then extend freely)
1. `uv run python -c "..."` or Neo4j browser: run the OVERVIEW_TOP_CONCEPTS query
   (server/queries.py) and take the top 3 concepts.
2. For each concept add one entry:
   - question: "What is the definition of <concept>?"
   - ground_truth_cypher:
     MATCH (d:Definition)-[:DEFINES]->(c:Concept) WHERE toLower(c.name)=toLower('<concept>')
     RETURN collect(d.statement)[..3] AS answer
   - expected_behavior: "answer"
3. Add two cross-paper questions of the form
   "Which papers discuss both <concept A> and <concept B>?" with
   MATCH (p:Paper)-[:DISCUSSES]->(a:Concept), (p)-[:DISCUSSES]->(b:Concept)
   WHERE toLower(a.name)=toLower('<A>') AND toLower(b.name)=toLower('<B>')
   RETURN collect(p.title) AS answer

## Metrics (LLM-as-judge; treat scores as noisy, compare trends not absolutes)
- context_recall: is the info needed for the ground truth present in retrieved chunks?
- answer_correctness: does the generated answer agree with the ground-truth rows?
A refuse-question passes when the answer clearly states the info is unavailable.
```

- [ ] **Step 6: Write `scripts/run_eval.py`**

```python
"""Benchmark harness: Cypher ground truth + hybrid retrieval + two LLM judges.
Manual tool — not wired into CI (judge calls cost money and scores are noisy).
Usage: uv run python scripts/run_eval.py [--limit N]"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from server.graph import GraphClient
from server.retrieve import search_chunks_core
from server.settings import Settings

ANSWER_MODEL = os.environ.get("EVAL_ANSWER_MODEL", "gpt-5-nano")
JUDGE_MODEL = os.environ.get("EVAL_JUDGE_MODEL", "gpt-5-nano")


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
    settings = Settings()
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
```

Note for the implementer: `graph.read` runs the ground-truth Cypher through the same READ_ACCESS session as everything else, so a malformed benchmark query cannot write.

- [ ] **Step 7: Run the harness end-to-end (live, cheap)**

Run: `uv run python scripts/run_eval.py --limit 3`
Expected: three lines printed with verdicts, a summary dict, and a JSON file under `evals/results/`. This is the BASELINE — keep the file; later tasks should move recall up on symbol-heavy questions.

- [ ] **Step 8: Add `evals/results/` to `.gitignore`, commit**

```bash
echo "evals/results/" >> .gitignore
git add server/retrieve.py server/tools.py tests/server/test_retrieve.py \
        evals/benchmark.json evals/README.md scripts/run_eval.py .gitignore
git commit -m "feat(evals): Cypher-ground-truth benchmark + retrieval/answer judge harness"
```

---

### Task 4: Chunk-level provenance — MENTIONS + EXTRACTED_FROM (builder)

**Files:**
- Modify: `pipeline/graph/schema.py` (RELATIONSHIP_TYPES, PATTERNS)
- Modify: `pipeline/extraction/extraction.py` (add `merge_results_with_provenance`)
- Modify: `pipeline/assets/extracted_graph.py` (thread chunk ids; emit `provenance`)
- Modify: `pipeline/assets/graph_write.py` (row builders + Cypher + wiring)
- Create: `scripts/backfill_mentions.py`
- Test: `tests/test_extraction.py`, `tests/test_graph_write.py`, `tests/test_schema.py`

**Interfaces:**
- Consumes: chunk artifact rows `{id, text, position, embedding}` (CHUNKS_BUCKET); resolved payload passthrough (resolved_entities preserves unknown payload keys — verified: it only rewrites `concepts` and adds `alias_registrations`).
- Produces: payload key `"provenance"`: `{"concepts": {<surface_lower>: [chunk_id]}, "definitions": {<normalize_statement(stmt)>: [chunk_id]}, "results": {"<kind>|<normalize_statement(stmt)>": [chunk_id]}}`. Graph edges `(Chunk)-[:MENTIONS]->(Concept)`, `(Definition)-[:EXTRACTED_FROM]->(Chunk)`, `(Result)-[:EXTRACTED_FROM]->(Chunk)`. Task 5b's `GET_CONCEPT` addition reads MENTIONS.

- [ ] **Step 1: Schema vocabulary (test first)**

Append to `tests/test_schema.py`:

```python
def test_provenance_patterns_present():
    from pipeline.graph.schema import PATTERNS, RELATIONSHIP_TYPES
    assert "MENTIONS" in RELATIONSHIP_TYPES
    assert "EXTRACTED_FROM" in RELATIONSHIP_TYPES
    assert ("Chunk", "MENTIONS", "Concept") in PATTERNS
    assert ("Definition", "EXTRACTED_FROM", "Chunk") in PATTERNS
    assert ("Result", "EXTRACTED_FROM", "Chunk") in PATTERNS
```

Run: `uv run pytest tests/test_schema.py::test_provenance_patterns_present -v` — Expected: FAIL.

Then in `pipeline/graph/schema.py`: append `"MENTIONS"` and `"EXTRACTED_FROM"` to `RELATIONSHIP_TYPES`, and append to `PATTERNS`:

```python
    ("Chunk",      "MENTIONS",       "Concept"),
    ("Definition", "EXTRACTED_FROM", "Chunk"),
    ("Result",     "EXTRACTED_FROM", "Chunk"),
]
```

Re-run — Expected: PASS. Commit: `git add -A && git commit -m "feat(schema): MENTIONS + EXTRACTED_FROM provenance vocabulary"`

- [ ] **Step 2: `merge_results_with_provenance` (test first)**

Append to `tests/test_extraction.py`:

```python
def test_merge_results_with_provenance_tracks_chunk_ids():
    from pipeline.extraction.extraction import (
        Concept, Definition, ExtractionResult, Result, merge_results_with_provenance)
    p1 = ExtractionResult(
        concepts=[Concept(name="Brownian motion")],
        definitions=[Definition(term="BM", statement="A process with...")],
        results=[Result(kind="theorem", statement="Every martingale...")])
    p2 = ExtractionResult(concepts=[Concept(name="brownian motion")])  # dup, differing case
    merged, prov = merge_results_with_provenance([p1, p2], ["doc:0", "doc:1"])
    assert len(merged.concepts) == 1
    assert prov["concepts"]["brownian motion"] == ["doc:0", "doc:1"]
    from pipeline.text_norm import normalize_statement
    assert prov["definitions"][normalize_statement("A process with...")] == ["doc:0"]
    assert prov["results"]["theorem|" + normalize_statement("Every martingale...")] == ["doc:0"]
```

Run: `uv run pytest tests/test_extraction.py::test_merge_results_with_provenance_tracks_chunk_ids -v` — Expected: FAIL (ImportError).

Implement in `pipeline/extraction/extraction.py` after `merge_results`:

```python
def merge_results_with_provenance(
    parts: list[ExtractionResult], chunk_ids: list[str],
) -> tuple[ExtractionResult, dict]:
    """merge_results plus per-item source-chunk ids. `chunk_ids` aligns 1:1 with `parts`.
    Provenance keys mirror the dedup keys merge_results/graph_write use: lowercased concept
    name; normalize_statement(statement) for definitions; '<kind>|<normalized>' for results."""
    assert len(parts) == len(chunk_ids), "chunk_ids must align 1:1 with parts"
    merged = merge_results(parts)
    kept_c = {c.name.lower() for c in merged.concepts}
    kept_d = {normalize_statement(d.statement) for d in merged.definitions}
    kept_r = {f"{r.kind}|{normalize_statement(r.statement)}" for r in merged.results}
    prov: dict = {"concepts": {}, "definitions": {}, "results": {}}

    def _add(bucket: dict, key: str, cid: str) -> None:
        lst = bucket.setdefault(key, [])
        if cid not in lst:
            lst.append(cid)

    for part, cid in zip(parts, chunk_ids):
        for c in part.concepts:
            if c.name.lower() in kept_c:
                _add(prov["concepts"], c.name.lower(), cid)
        for d in part.definitions:
            k = normalize_statement(d.statement)
            if k in kept_d:
                _add(prov["definitions"], k, cid)
        for r in part.results:
            k = f"{r.kind}|{normalize_statement(r.statement)}"
            if k in kept_r:
                _add(prov["results"], k, cid)
    return merged, prov
```

Re-run — Expected: PASS. Note: dropped notation-only concepts are absent from `kept_c`, so they get no provenance rows — correct.

- [ ] **Step 3: Thread through `extracted_graph`**

In `pipeline/assets/extracted_graph.py`:
- Change the import to `from pipeline.extraction.extraction import extract_from_chunk, merge_results_with_provenance`.
- Replace the `texts = ...` line with:

```python
    ordered = [c for c in sorted(chunk_rows, key=lambda c: c["position"]) if c["text"]]
    texts = [c["text"] for c in ordered]
    ids = [c["id"] for c in ordered]
```

- Replace `merged = merge_results(parts)` with `merged, provenance = merge_results_with_provenance(parts, ids)`.
- Add `"provenance": provenance,` to the `payload` dict.

Run: `uv run pytest tests/ -v -k "extract"` — Expected: PASS (existing extraction tests untouched; `merge_results` still exists and behaves identically).

- [ ] **Step 4: graph_write row builders (test first)**

Append to `tests/test_graph_write.py`:

```python
def test_mention_rows_maps_surface_to_canonical():
    from pipeline.assets.graph_write import mention_rows
    prov = {"concepts": {"bm": ["d:0", "d:2"], "unknown thing": ["d:1"]}}
    rows, skipped = mention_rows(prov, {"bm": "Brownian motion"})
    assert rows == [{"chunk_id": "d:0", "canonical": "Brownian motion"},
                    {"chunk_id": "d:2", "canonical": "Brownian motion"}]
    assert skipped == 1


def test_extracted_from_rows_recomputes_ids():
    from pipeline.assets.graph_write import def_id, extracted_from_rows, result_id
    defs = [{"term": "BM", "statement": "A process with...", "defines": []}]
    results = [{"kind": "theorem", "statement": "Every martingale...", "name": "", "uses": [], "depends_on": []}]
    from pipeline.text_norm import normalize_statement
    prov = {"definitions": {normalize_statement("A process with..."): ["d:0"]},
            "results": {"theorem|" + normalize_statement("Every martingale..."): ["d:3"]}}
    drows, rrows = extracted_from_rows("paper1", defs, results, prov)
    assert drows == [{"node_id": def_id("paper1", "A process with..."), "chunk_id": "d:0"}]
    assert rrows == [{"node_id": result_id("paper1", "theorem", "Every martingale..."), "chunk_id": "d:3"}]
```

Run: FAIL (ImportError). Implement in `pipeline/assets/graph_write.py` after `depends_on_edge_rows`:

```python
def mention_rows(provenance: dict, surface_to_canon: dict[str, str]) -> tuple[list[dict], int]:
    """Chunk-MENTIONS->Concept rows from extraction provenance, mapped through the resolver's
    surface->canonical table (lowercased keys). Skips surfaces with no canonical."""
    rows, skipped = [], 0
    for surface_l, cids in provenance.get("concepts", {}).items():
        canon = surface_to_canon.get(surface_l)
        if canon is None:
            skipped += 1
            continue
        rows.extend({"chunk_id": cid, "canonical": canon} for cid in cids)
    return rows, skipped


def extracted_from_rows(paper_id: str, raw_defs: list[dict], raw_results: list[dict],
                        provenance: dict) -> tuple[list[dict], list[dict]]:
    """Definition/Result -EXTRACTED_FROM-> Chunk rows; node ids recomputed with the same
    content-hash scheme the node writers use, provenance looked up by the same keys."""
    drows = []
    for d in raw_defs:
        k = normalize_statement(d["statement"])
        drows.extend({"node_id": def_id(paper_id, d["statement"]), "chunk_id": cid}
                     for cid in provenance.get("definitions", {}).get(k, []))
    rrows = []
    for r in raw_results:
        k = f"{r['kind']}|{normalize_statement(r['statement'])}"
        rrows.extend({"node_id": result_id(paper_id, r["kind"], r["statement"]), "chunk_id": cid}
                     for cid in provenance.get("results", {}).get(k, []))
    return drows, rrows
```

And the Cypher constants after `WRITE_RESULT_DEPENDS`:

```python
WRITE_MENTIONS = """
UNWIND $rows AS row
  MATCH (ch:Chunk {id: row.chunk_id})
  MATCH (c:Concept {name: row.canonical})
  MERGE (ch)-[:MENTIONS]->(c)
"""

WRITE_DEF_PROVENANCE = """
UNWIND $rows AS row
  MATCH (d:Definition {id: row.node_id})
  MATCH (ch:Chunk {id: row.chunk_id})
  MERGE (d)-[:EXTRACTED_FROM]->(ch)
"""

WRITE_RESULT_PROVENANCE = """
UNWIND $rows AS row
  MATCH (r:Result {id: row.node_id})
  MATCH (ch:Chunk {id: row.chunk_id})
  MERGE (r)-[:EXTRACTED_FROM]->(ch)
"""
```

Wire into the asset body, right after `s.run(WRITE_RESULT_DEPENDS, rows=dep_edges)`:

```python
        provenance = resolved.get("provenance", {})  # absent on pre-change artifacts
        m_rows, sk_mention = mention_rows(provenance, surface_to_canon)
        dprov_rows, rprov_rows = extracted_from_rows(paper_id, raw_defs, raw_results, provenance)
        s.run(WRITE_MENTIONS, rows=m_rows)
        s.run(WRITE_DEF_PROVENANCE, rows=dprov_rows)
        s.run(WRITE_RESULT_PROVENANCE, rows=rprov_rows)
```

and add to the MaterializeResult metadata dict:

```python
        "mentions": MetadataValue.int(len(m_rows)),
        "extracted_from": MetadataValue.int(len(dprov_rows) + len(rprov_rows)),
```

Run: `uv run pytest tests/test_graph_write.py tests/test_extraction.py -v` — Expected: PASS. Commit:

```bash
git add pipeline tests
git commit -m "feat(provenance): thread chunk ids through extraction; write MENTIONS + EXTRACTED_FROM"
```

- [ ] **Step 5: Backfill script for the existing corpus**

Create `scripts/backfill_mentions.py`:

```python
"""One-off backfill: Chunk-MENTIONS->Concept for papers ingested before provenance existed.
Approximate by design: a chunk MENTIONS a concept iff the concept's name appears verbatim
(case-insensitive) in the chunk text of a paper that DISCUSSES it. Definitions/Results are
NOT backfilled (statement matching is unreliable); they gain EXTRACTED_FROM on new ingests.

WRITES TO NEO4J. Run manually while the Dagster schedule is idle (it is idempotent MERGE,
but stay within the single-writer convention). Usage: uv run python scripts/backfill_mentions.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

PAPERS = "MATCH (p:Paper) RETURN p.id AS id ORDER BY p.id"

BACKFILL_ONE = """
MATCH (p:Paper {id: $paper_id})-[:DISCUSSES]->(c:Concept)
WHERE size(c.name) >= 4
MATCH (p)-[:HAS_DOCUMENT]->(:Document)<-[:BELONGS_TO]-(ch:Chunk)
WHERE toLower(ch.text) CONTAINS toLower(c.name)
MERGE (ch)-[:MENTIONS]->(c)
RETURN count(*) AS edges
"""


def main() -> None:
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    with driver.session(database=db) as s:
        papers = [r["id"] for r in s.run(PAPERS)]
        total = 0
        for i, pid in enumerate(papers, 1):
            edges = s.run(BACKFILL_ONE, paper_id=pid).single()["edges"]
            total += edges
            print(f"[{i}/{len(papers)}] {pid}: {edges} MENTIONS")
    driver.close()
    print(f"done: {total} MENTIONS edges")


if __name__ == "__main__":
    main()
```

(The env-var/dotenv bootstrap mirrors `scripts/init_neo4j.py` exactly. The `size(c.name) >= 4` guard stops junk matches from very short names.)

Run: `uv run python scripts/backfill_mentions.py` — Expected: per-paper lines, non-zero total. Spot-check in Browser: `MATCH (ch:Chunk)-[:MENTIONS]->(c:Concept) RETURN c.name, count(ch) ORDER BY count(ch) DESC LIMIT 10`.

- [ ] **Step 6: Commit**

```bash
git add scripts/backfill_mentions.py
git commit -m "feat(provenance): one-off MENTIONS backfill for pre-provenance papers"
```

---

### Task 5a: Concept descriptions at extraction + write time (builder)

**Files:**
- Modify: `pipeline/extraction/extraction.py` (Concept model, merge_results)
- Modify: `pipeline/assets/graph_write.py` (concept_rows, WRITE_CONCEPTS, embedding write)
- Modify: `pipeline/graph/schema.py` (concept_embedding vector index)
- Test: `tests/test_extraction.py`, `tests/test_graph_write.py`, `tests/test_schema.py`

**Interfaces:**
- Consumes: `embed_texts(client, texts, model=..., timeout=...)` from `pipeline/embedding.py`; openai Dagster resource (`get_client()`, `.embedding_model`, `.request_timeout`).
- Produces: `Concept.description: str` (default `""`); `Concept.description` and `Concept.embedding` properties in Neo4j; vector index `concept_embedding` (1536, cosine). Task 5b queries the index by exactly this name.

- [ ] **Step 1: Model + merge (test first)**

Append to `tests/test_extraction.py`:

```python
def test_concept_description_first_nonempty_wins_on_merge():
    from pipeline.extraction.extraction import Concept, ExtractionResult, merge_results
    p1 = ExtractionResult(concepts=[Concept(name="Rectified flow", description="")])
    p2 = ExtractionResult(concepts=[Concept(
        name="rectified flow", description="A method that straightens transport paths.")])
    merged = merge_results([p1, p2])
    assert len(merged.concepts) == 1
    assert merged.concepts[0].description == "A method that straightens transport paths."
```

Run: FAIL (`description` unexpected / empty). Implement:

In `Concept`, after `kind`:

```python
    description: str = Field(
        default="",
        description="One sentence (at most ~40 words) saying what this concept IS, grounded "
        "ONLY in this chunk's text — no outside knowledge. Plain prose; render math as LaTeX "
        "in $...$. Empty string if the chunk gives no basis for a description.",
    )
```

In `merge_results`, replace the concept loop's dedup block so the kept model backfills an empty description from a later duplicate:

```python
    seen_c: dict[str, Concept] = {}
    concepts = []
    for p in parts:
        for c in p.concepts:
            if _is_notation_only(c.name):
                continue  # bare notation is never a concept (backstop; primary fix is the prompt)
            kept = seen_c.get(c.name.lower())
            if kept is None:
                seen_c[c.name.lower()] = c
                concepts.append(c)
            elif not kept.description and c.description:
                kept.description = c.description
```

(Also update the `seen_c = set(), []` initialization line accordingly.) Run the test — PASS.

- [ ] **Step 2: Schema index (test first)**

Append to `tests/test_schema.py`:

```python
def test_concept_vector_index_in_init():
    from pipeline.graph.schema import iter_init_statements
    assert any("VECTOR INDEX concept_embedding" in s for s in iter_init_statements())
```

FAIL, then add to `INIT_CYPHER` after the `chunk_text` full-text statement:

```
CREATE VECTOR INDEX concept_embedding IF NOT EXISTS
  FOR (c:Concept) ON c.embedding
  OPTIONS {
    indexConfig: {
      `vector.dimensions`: 1536,
      `vector.similarity_function`: 'cosine'
    }
  };
```

PASS, then apply live: `uv run python scripts/init_neo4j.py`.

- [ ] **Step 3: Write path (test first)**

Append to `tests/test_graph_write.py`:

```python
def test_concept_rows_carry_description():
    from pipeline.assets.graph_write import concept_rows
    rows = concept_rows([{"name": "Rectified flow", "kind": "method",
                          "description": "Straightens transport paths."}])
    assert rows == [{"name": "Rectified flow", "tags": ["method"],
                     "description": "Straightens transport paths."}]
```

FAIL, then in `pipeline/assets/graph_write.py`:

```python
def concept_rows(concepts: list[dict]) -> list[dict]:
    return [{"name": c["name"], "tags": [c["kind"]],
             "description": c.get("description", "")} for c in concepts]
```

and make `WRITE_CONCEPTS` first-wins on description (idempotent — re-runs never overwrite):

```python
WRITE_CONCEPTS = """
MATCH (p:Paper {id:$paper_id})
UNWIND $rows AS row
  MERGE (c:Concept {name: row.name})
  SET c.tags = row.tags
  SET c.description = coalesce(c.description,
        CASE WHEN row.description = '' THEN NULL ELSE row.description END)
  MERGE (p)-[:DISCUSSES]->(c)
  MERGE (c)-[:DERIVED_FROM]->(p)
"""
```

IMPORTANT: `resolved_entities` passes concept dicts through `resolved_concept_row`, which drops unknown keys. Add `description` there too — in `pipeline/assets/resolved_entities.py` extend `resolved_concept_row` to accept and emit `description` (take it from the ORIGINAL surface concept dict, since the canonical may come from another paper):

```python
def resolved_concept_row(surface: str, canonical: str, kind: str, action: str,
                         embedding: list[float], description: str = "") -> dict:
    ...existing docstring...
    return {"surface": surface, "name": canonical, "kind": kind,
            "action": action, "embedding": embedding, "description": description}
```

and at the call site: `resolved_concept_row(r.surface, r.canonical, r.kind, r.action, r.embedding, description=next((c.get("description","") for c in concepts if c["name"] == r.surface), ""))`.

Run: `uv run pytest tests/test_graph_write.py tests/test_resolved_entities.py -v` — Expected: PASS (fix any resolved_entities test fixtures that assert exact dict shape by adding the new key).

- [ ] **Step 4: Embed descriptions in graph_write**

In `pipeline/assets/graph_write.py`, add `"openai"` to `required_resource_keys`, import `from pipeline.embedding import embed_texts`, add the Cypher pair:

```python
CONCEPTS_NEEDING_EMBEDDING = """
UNWIND $names AS name
MATCH (c:Concept {name: name})
WHERE c.embedding IS NULL AND c.description IS NOT NULL
RETURN c.name AS name, c.description AS description
"""

SET_CONCEPT_EMBEDDINGS = """
UNWIND $rows AS row
MATCH (c:Concept {name: row.name})
CALL db.create.setNodeVectorProperty(c, 'embedding', row.embedding)
"""
```

and after the `s.run(WRITE_RESULT_PROVENANCE, ...)` line (inside the same session):

```python
        # Retrieval embedding: name+description, only for concepts that don't have one yet
        # (first-wins, mirrors the description coalesce; keeps re-runs idempotent).
        need = s.run(CONCEPTS_NEEDING_EMBEDDING,
                     names=[c["name"] for c in crows]).data()
        if need:
            cfg = context.resources.openai
            vecs = embed_texts(cfg.get_client(),
                               [f"{r['name']}: {r['description']}" for r in need],
                               model=cfg.embedding_model, timeout=cfg.request_timeout)
            s.run(SET_CONCEPT_EMBEDDINGS,
                  rows=[{"name": r["name"], "embedding": v} for r, v in zip(need, vecs)])
```

plus metadata `"concept_embeddings": MetadataValue.int(len(need)),`. NOTE: this is a distinct embedding from the pgvector NAME embedding used by the resolver — do not touch the resolver; its 0.60/0.90 thresholds are calibrated on name embeddings.

Run: `uv run pytest tests/test_graph_write.py -v` — Expected: PASS (unit tests don't execute the asset body; if `tests/integration` has a graph_write test, run it with `--run-integration` and update its resource fixtures to provide `openai`).

- [ ] **Step 5: Commit**

```bash
git add pipeline tests
git commit -m "feat(concepts): one-line descriptions at extraction; description embeddings + concept_embedding index"
```

---

### Task 5b: `search_concepts` MCP tool (server)

**Files:**
- Modify: `server/queries.py` (SEARCH_CONCEPTS; extend GET_CONCEPT with MENTIONS chunks)
- Modify: `server/tools.py` (new tool)
- Test: `tests/server/test_queries.py`

**Interfaces:**
- Consumes: `concept_embedding` index (Task 5a) and MENTIONS edges (Task 4).
- Produces: MCP tool `search_concepts(query: str, top_k: int = 8) -> dict`.

- [ ] **Step 1: Queries**

Add to `server/queries.py`:

```python
SEARCH_CONCEPTS = """
CALL db.index.vector.queryNodes('concept_embedding', $k, $embedding)
YIELD node, score
RETURN node.name AS name, node.description AS description,
       node.tags AS tags, score
ORDER BY score DESC
LIMIT $top_k
"""
```

In `GET_CONCEPT`, before the final `RETURN`, add supporting chunks (and add the field to the RETURN):

```
OPTIONAL MATCH (ch:Chunk)-[:MENTIONS]->(c)
WITH c, definitions, papers, related_concepts,
     collect(DISTINCT {chunk_id: ch.id, position: ch.position,
                       text: left(ch.text, 600)})[..5] AS supporting_chunks
RETURN c.name AS name, c.tags AS tags, c.description AS description,
       definitions, papers, related_concepts, supporting_chunks
```

(Implementer: the existing GET_CONCEPT builds `related_concepts` via a `collect(oname)[..10]` in the final RETURN — restructure carefully: collect related_concepts into a WITH before the MENTIONS OPTIONAL MATCH, exactly as sketched, and verify against the fake-driver test plus one `--run-integration` call.)

- [ ] **Step 2: Tool**

Add to `server/tools.py` after `get_concept`:

```python
    @mcp.tool()
    def search_concepts(query: str, top_k: int = 8) -> dict:
        """Vector-search Concept nodes by their descriptions (entity-anchored entry point).
        Follow up with get_concept(name) for definitions, papers, and supporting chunks."""
        top_k = q.validate_top_k(top_k)
        hits = graph.read(q.SEARCH_CONCEPTS, k=top_k * 2, top_k=top_k,
                          embedding=graph.embed(query))
        return {"concepts": hits}
```

- [ ] **Step 3: Test + run**

Append to `tests/server/test_queries.py`:

```python
def test_search_concepts_query_targets_concept_index():
    assert "concept_embedding" in q.SEARCH_CONCEPTS
    assert "supporting_chunks" in q.GET_CONCEPT
```

Run: `uv run pytest tests/server -v` — Expected: PASS (update `test_app.py`'s registered-tool-count assertion if it enumerates tools: there are now 9).

- [ ] **Step 4: Commit**

```bash
git add server tests
git commit -m "feat(server): search_concepts entity-anchored retrieval + supporting chunks on get_concept"
```

---

### Task 5c: Backfill descriptions for existing concepts (builder)

**Files:**
- Create: `scripts/backfill_concept_descriptions.py`

**Interfaces:**
- Consumes: MENTIONS edges (Task 4 backfill), `embed_texts`, openai env config.
- Produces: `description` + `embedding` on every pre-existing Concept.

- [ ] **Step 1: Write the script**

```python
"""One-off backfill: generate a one-sentence description (+ embedding) for Concepts that
predate Task 5a. Grounding = the concept's definitions and up to 3 MENTIONS chunk excerpts.
Resumable: only touches WHERE c.description IS NULL. WRITES TO NEO4J — run while the
Dagster schedule is idle. Cost: one gpt-5-nano call + one embedding per concept.
Usage: uv run python scripts/backfill_concept_descriptions.py [--limit N]"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from openai import OpenAI
from pydantic import BaseModel, Field

from pipeline.embedding import embed_texts

load_dotenv()

EMBED_MODEL = "text-embedding-3-small"  # pinned corpus-wide (1536 dims)
DESCRIBE_MODEL = os.environ.get("EXTRACTION_MODEL", "gpt-5-nano")

MISSING = """
MATCH (c:Concept) WHERE c.description IS NULL
OPTIONAL MATCH (d:Definition)-[:DEFINES]->(c)
WITH c, collect(d.statement)[..3] AS defs
OPTIONAL MATCH (ch:Chunk)-[:MENTIONS]->(c)
WITH c, defs, collect(left(ch.text, 900))[..3] AS excerpts
OPTIONAL MATCH (p:Paper)-[:DISCUSSES]->(c)
RETURN c.name AS name, defs, excerpts, collect(p.title)[..5] AS papers
LIMIT $limit
"""

SET_ONE = """
MATCH (c:Concept {name: $name})
SET c.description = $description
WITH c
CALL db.create.setNodeVectorProperty(c, 'embedding', $embedding)
"""


class Description(BaseModel):
    description: str = Field(description="One sentence, at most ~40 words, saying what the "
                             "concept IS. Grounded only in the provided material; LaTeX for math.")


def describe(client, model: str, name: str, defs, excerpts, papers) -> str:
    material = "\n".join(
        ["Definitions:", *defs, "Text excerpts:", *excerpts, "Papers:", *papers])
    resp = client.chat.completions.parse(
        model=model,
        messages=[{"role": "system",
                   "content": "Write a one-sentence description of the given research concept "
                              "using ONLY the provided material."},
                  {"role": "user", "content": f"Concept: {name}\n\n{material[:12000]}"}],
        response_format=Description, timeout=60)
    return resp.choices[0].message.parsed.description


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10000)
    args = ap.parse_args()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    client = OpenAI()  # OPENAI_API_KEY from .env
    with driver.session(database=db) as s:
        rows = s.run(MISSING, limit=args.limit).data()
        print(f"{len(rows)} concepts missing descriptions")
        for i, row in enumerate(rows, 1):
            desc = describe(client, DESCRIBE_MODEL, row["name"],
                            row["defs"], row["excerpts"], row["papers"])
            vec = embed_texts(client, [f"{row['name']}: {desc}"], model=EMBED_MODEL)[0]
            s.run(SET_ONE, name=row["name"], description=desc, embedding=vec)
            print(f"[{i}/{len(rows)}] {row['name']}: {desc[:70]}")
    driver.close()


if __name__ == "__main__":
    main()
```

(Bootstrap mirrors `scripts/init_neo4j.py`. The `WHERE c.description IS NULL` clause makes re-runs resume automatically after any interruption.)

- [ ] **Step 2: Dry-run then full run**

Run: `uv run python scripts/backfill_concept_descriptions.py --limit 5`
Expected: 5 progress lines. Spot-check one in Browser: `MATCH (c:Concept) WHERE c.description IS NOT NULL RETURN c.name, c.description LIMIT 5` — descriptions read as grounded single sentences.
Then: full run (no --limit). Cost note: a corpus with ~1–2k concepts ≈ a few dollars on gpt-5-nano + embeddings.

- [ ] **Step 3: Commit**

```bash
git add scripts/backfill_concept_descriptions.py
git commit -m "feat(concepts): backfill descriptions + embeddings for pre-existing concepts"
```

---

### Task 6: `run_cypher` + `get_schema` tools (server)

**Files:**
- Modify: `server/graph.py` (read_limited)
- Modify: `server/queries.py` (write-clause guard regex + schema renderer)
- Modify: `server/tools.py` (two new tools)
- Test: `tests/server/test_queries.py`, `tests/server/test_app.py`

**Interfaces:**
- Consumes: `NODE_TYPES`, `PATTERNS` from `pipeline.graph.schema` (dagster-free — extend the "only pipeline import" note in `server/graph.py`'s docstring to include it).
- Produces: MCP tools `get_schema() -> dict` and `run_cypher(query: str) -> dict` (rows capped at 100, 15s timeout, truncated flag).

- [ ] **Step 1: Tests first**

Append to `tests/server/test_queries.py`:

```python
import pytest


def test_write_guard_rejects_write_clauses():
    for bad in ["CREATE (n)", "MATCH (n) SET n.x=1", "MERGE (n:X)",
                "MATCH (n) DETACH DELETE n", "DROP INDEX foo",
                "LOAD CSV FROM 'x' AS row RETURN row"]:
        with pytest.raises(ValueError):
            q.check_read_only(bad)


def test_write_guard_allows_reads():
    q.check_read_only("MATCH (p:Paper) RETURN count(p)")
    q.check_read_only("CALL db.index.vector.queryNodes('x', 5, $e) YIELD node RETURN node")


def test_render_schema_lists_patterns():
    text = q.render_schema()
    assert "(:Paper)-[:DISCUSSES]->(:Concept)" in text
    assert "Chunk" in text
```

Run: FAIL. Implement in `server/queries.py`:

```python
import re

from pipeline.graph.schema import NODE_TYPES, PATTERNS

_WRITE_CLAUSE = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|FOREACH|LOAD\s+CSV)\b", re.IGNORECASE)


def check_read_only(cypher: str) -> None:
    """Courtesy guard for clearer errors; the real enforcement is the driver-level
    READ_ACCESS session (see GraphClient), which the integration suite verifies."""
    m = _WRITE_CLAUSE.search(cypher)
    if m:
        raise ValueError(f"run_cypher is read-only; found write clause {m.group(0)!r}")


def render_schema() -> str:
    lines = ["Node labels: " + ", ".join(NODE_TYPES), "", "Relationships:"]
    lines += [f"(:{s})-[:{r}]->(:{e})" for s, r, e in PATTERNS]
    lines += ["", "Key properties: Paper{id,title,year,doi,arxiv_id,abstract,tldr,citation_count}, "
              "Concept{name,description,tags}, Definition{id,term,statement}, "
              "Result{id,kind,name,statement}, Chunk{id,text,position}, "
              "Book{id,title}, Chapter/Section{id,title}.",
              "Only Topic/Researcher/Idea are in the vocabulary but not yet populated."]
    return "\n".join(lines)
```

- [ ] **Step 2: `read_limited` on GraphClient**

In `server/graph.py`:

```python
from neo4j import READ_ACCESS, GraphDatabase, Query
```

and add the method:

```python
    def read_limited(self, cypher: str, timeout: float = 15.0,
                     max_rows: int = 100, **params) -> tuple[list[dict], bool]:
        """Guarded read for run_cypher: server-side tx timeout + row cap.
        Returns (rows, truncated)."""
        with self._driver.session(
            database=self.settings.neo4j_database, default_access_mode=READ_ACCESS
        ) as s:
            result = s.run(Query(cypher, timeout=timeout), **params)
            rows: list[dict] = []
            for record in result:
                rows.append(record.data())
                if len(rows) >= max_rows:
                    return rows, True
            return rows, False
```

- [ ] **Step 3: Tools**

Add to `server/tools.py`:

```python
    @mcp.tool()
    def get_schema() -> dict:
        """The graph's node labels, relationship patterns, and key properties.
        Read this before writing a run_cypher query."""
        return {"schema": q.render_schema()}

    @mcp.tool()
    def run_cypher(query: str) -> dict:
        """Read-only Cypher escape hatch for aggregations and questions no typed tool
        covers (counts, rankings, filters, multi-hop). The session is read-only at the
        driver level; rows are capped at 100 with a 15s timeout. Call get_schema first
        and use only the labels/relationships it lists."""
        q.check_read_only(query)
        rows, truncated = graph.read_limited(query)
        return {"rows": rows, "row_count": len(rows), "truncated": truncated}
```

- [ ] **Step 4: Run the suites**

Run: `uv run pytest tests/server -v`
Expected: PASS (again bump any registered-tool-count assertion — now 11 tools). Then live:
`uv run pytest tests/server/test_integration_server.py -v --run-integration`
Expected: PASS, including the existing `test_write_attempt_fails_readonly` (now also add its twin through the tool path if quick: `run_cypher("CREATE (n)")` must raise ValueError from the guard before ever reaching the driver).

- [ ] **Step 5: Commit**

```bash
git add server tests
git commit -m "feat(server): get_schema + guarded read-only run_cypher escape hatch"
```

---

## Rollout order & measurement

1. Task 1 (index live) → Task 2 → Task 3. Run `run_eval.py` — this is the **baseline** record.
2. Task 4 (+ backfill) → Task 5a → 5c → 5b. Re-run `run_eval.py`; recall on symbol/name questions should rise.
3. Task 6. Add 2–3 aggregation questions to the benchmark that only run_cypher can serve; re-run.
4. Ops (human): `fly deploy` from the repo root once merged; update `~/Projects/kg` ask-skill docs to mention `search_concepts`, `get_schema`, `run_cypher` (separate repo, three-line change, out of this plan's scope).

## Out of scope (deliberately)

- Switching the RESOLVER to description embeddings (would invalidate the calibrated 0.60/0.90 thresholds; revisit with data from `resolution_decisions` after 5c).
- Community detection / global summaries (spec/07 rationale stands; the Summary-sweep idea can be a later two-line tool).
- EXTRACTED_FROM backfill for pre-existing Definitions/Results (statement matching too unreliable; new ingests only).
