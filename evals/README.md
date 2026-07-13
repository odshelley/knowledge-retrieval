# Retrieval/answer benchmark

Ground truth is a Cypher query executed against the live graph at eval time
(the book's "graph as oracle" pattern), so the benchmark stays valid as papers land.

Run: `uv run python scripts/run_eval.py` (needs .env with NEO4J_* and OPENAI_API_KEY).
Results land in `evals/results/<timestamp>.json` and print as a table.

Two modes (`--mode`, default `retrieval`):
- `retrieval` — the answer model sees only `search_chunks` context (legacy baseline).
- `agent` — a bounded tool loop (max 6 calls) with `search_chunks`, `get_schema`, and the
  guarded `run_cypher`, mirroring what MCP clients can do. Recall is judged over the union
  of tool outputs and scored on all items; per-slice correctness is reported in the summary.

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

The five content questions currently in `evals/benchmark.json` were added by this
procedure on 2026-07-12 against the live corpus. `OVERVIEW_TOP_CONCEPTS` returned
(top 3): Brownian motion (37 papers), Stochastic Differential Equation (23 papers),
Gaussian distribution (22 papers) — hence `define-brownian-motion`, `define-sde`,
`define-gaussian-distribution`. The two cross-paper questions pair Brownian motion
with the other two top concepts (`cross-brownian-sde`, `cross-brownian-gaussian`),
each verified non-empty against the live graph before being committed.

## Metrics (LLM-as-judge; treat scores as noisy, compare trends not absolutes)
- context_recall: is the info needed for the ground truth present in retrieved chunks?
- answer_correctness: does the generated answer agree with the ground-truth rows?
A refuse-question passes when the answer clearly states the info is unavailable.
