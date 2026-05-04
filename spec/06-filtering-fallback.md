# 06 — Filtering and Parametric-Memory Fallback

The single most impactful quality intervention. Per Zeng et al. 2025 ([[knowledge-filtering-graphrag]], [[knowledge-integration-graphrag]]): on WebQSP, ~17 % of GraphRAG queries are *actively broken* — the model produces a confidently wrong answer because retrieval surfaced misleading paths. In a bank, that's the difference between a useful tool and a compliance incident. This stage is mandatory.

## 1. Two-stage filter

### 1.1 Stage 1 — small-LLM attention scoring (cheap)

For each retrieved entity-with-provenance triple `(entity, edge, neighbour, chunk)`, ask a small LLM (e.g. Llama-3-8B, GPT-4o-mini) to rate path relevance. Per Zeng et al., **middle-layer attention scores** beat similarity rerankers; if you have access to attention activations, use them. Otherwise, a single-token logit-bias yes/no prompt is the practical equivalent:

```
Path: {entity.name} --[{edge.type}]--> {neighbour.name}
Provenance: "{chunk.text}"

Is this path RELEVANT to the query "{query}"?
Reply with one token: YES or NO.
```

Read the `YES` / `NO` logits; relevance score = `sigmoid(logit_yes - logit_no)`. Drop paths with score < `tau_stage1` (default `0.3`).

This stage is cheap (~80 input tokens per path, 1 output token) but kills 60–80 % of low-quality paths before the expensive stage.

### 1.2 Stage 2 — full-LLM scoring (expensive, optional)

Only on stage-1 survivors. A full LLM scores each path against the query with structured output:

```
You are evaluating retrieval quality for a quant-finance question-answering system.

Query: {query}

Candidate evidence path:
  Subject: {entity.name} ({entity.label}) — {entity.description}
  Relation: {edge.type} (confidence {edge.confidence})
  Object: {neighbour.name} ({neighbour.label}) — {neighbour.description}
  Source text: "{chunk.text}"
  Source: {chunk.parent.title} ({chunk.parent.url})

Score on three dimensions, 0–10 each:
  - relevance: does this path help answer the query?
  - faithfulness: is the relation supported by the source text?
  - specificity: does this add information beyond what the LLM would already know?

Reply JSON: {"relevance": int, "faithfulness": int, "specificity": int, "verdict": "keep"|"drop", "reason": "<one sentence>"}.
```

Apply hard drops first: discard any path with `verdict = drop` or with any dimension < 4. Then, over the surviving paths, compute the adaptive threshold per Zeng et al. §3.2: `tau_stage2 = mean(scores_composite) - 0.5 * std(scores_composite)`, where `scores_composite = relevance + 0.5 * specificity` is the same ranking score used for the §1.3 cap. Keep above-threshold paths, drop the rest.

### 1.3 Cap retrieved paths

Per Zeng et al. §4.4, F1 vs path-count is **non-monotonic and falls past ~30 paths**. Cap final filtered set at 30. If more survive, take top-30 by `relevance + 0.5*specificity`.

## 2. Parametric-memory fallback (asymmetric)

The GraphRAG pipeline is run in parallel with an **LLM-only branch**. Per Zeng et al. §3.3:

```python
def answer(query: str, user_groups: set[str]) -> Answer:
    # Branch 1: graph-grounded
    graph_ctx, graph_meta = graph_pipeline(query, user_groups)  # router + filter from §1
    graph_ans = llm.answer(query, context=graph_ctx, mode="strict")     # must cite provenance
    graph_conf = graph_meta["mean_filtered_relevance"]                    # 0..1

    # Branch 2: pure parametric memory (no retrieval)
    llm_ans = llm.answer(query, context=None, mode="parametric")
    llm_conf = sigmoid(llm.last_token_logit_diff)                          # logprobs of top vs runner-up

    return _integrate(query, graph_ans, graph_conf, llm_ans, llm_conf)
```

### 2.1 Asymmetric thresholds

Per Zeng et al. Tab 6, the integration is asymmetric — `tau_G` (graph branch) is in `[0.4, 0.5]`, `tau_L` (LLM-only branch) is `1.0` (= "must be highly confident"):

```python
def _integrate(query, gA, gC, lA, lC, tau_G=0.45, tau_L=1.0) -> Answer:
    if gC >= tau_G:
        return Answer(text=gA.text, source="graph", citations=gA.citations, confidence=gC)
    if gC < tau_G and lC >= tau_L:
        # graph branch failed; LLM-only branch is highly confident → use it but flag
        return Answer(text=lA.text, source="llm-only", citations=[], confidence=lC,
                      flag="No internal documentation found for this query. The answer below is from the model's general knowledge and is not grounded in any internal source.")
    # both failed — refuse
    return _refuse(query, gA, lA)


def _refuse(query, gA, lA) -> Answer:
    closest = top_k_chunks_by_query(query, k=5)        # pattern A retrieval, no filter
    return Answer(
        text=("I don't have enough confidence to answer this from internal documentation, "
              "and I won't guess. Here are the closest internal pages I found — please look "
              "directly:\n\n" + "\n".join(f"- {c.title} ({c.url})" for c in closest)),
        source="refuse",
        citations=[c.url for c in closest],
        confidence=0.0,
        flag="LOW_CONFIDENCE_REFUSE")
```

Why asymmetric: a confident-but-wrong LLM-only answer is worse than a confident-but-wrong graph-grounded answer because the latter at least cites a checkable source. So the bar for "trust LLM-only over graph" is high (`tau_L = 1.0`).

### 2.2 Why refuse-mode matters in a bank

This is the operational difference between a research demo and a production tool. **Hallucinating a non-existent model name, a wrong calibration date, or a misattributed methodology in a regulated context is a compliance event.** Refuse-and-link is always safer than guess.

User-experience implication: the UI must make the refuse case feel useful, not like failure. Surface the closest matched pages, the user's classified query type, and a "rephrase as multi-hop / global / lookup" hint.

## 3. Caching and observability

### 3.1 Cache the filter outputs

Stage-1 and Stage-2 verdicts are deterministic-ish given fixed `(query, path)`. Cache for ~1 hour keyed on `hash(query, entity_id, edge_id, chunk_id)`. Hot queries amortise across users.

### 3.2 Log everything

Every answered query writes a row:

```cypher
CREATE (q:Query {
  id: $query_id,
  text: $query,
  classifier_class: $cls,
  pattern: $pattern,
  candidate_count: $n_candidates,
  filtered_count: $n_after_stage2,
  graph_confidence: $gC,
  llm_only_confidence: $lC,
  source: $source,         // "graph" | "llm-only" | "refuse"
  ts: datetime()
});
WITH q
UNWIND $cited_entities AS e
MATCH (n) WHERE n.canonical = true AND n.name = e.name
MERGE (q)-[:CITED]->(n);
```

These logs are the input to:
- **Quality dashboards** — refuse rate by query class is the headline metric.
- **Threshold tuning** — `tau_G`, `tau_L`, `tau_stage1` retuned monthly from a held-out human-labelled set.
- **Drift detection** — sudden refuse-rate spikes on a query class indicate ingestion regression or a new hot topic the graph hasn't covered yet.

## 4. What this stage doesn't cover

- **Adversarial queries** designed to extract data the user shouldn't see — handled by the `:VISIBLE_TO` overlay at retrieval time, not here.
- **Prompt injection from page content** — defence is at the LLM layer (system-prompt isolation, input sanitisation). Out of scope for this spec.
- **Long-form conversations / follow-ups** — assume each query is independent. Multi-turn context-carrying is a UI concern.
