# 08 — MVP Build Plan

Phased so each phase delivers measurable value and can be evaluated before committing to the next.

## Phase 0 — Plumbing (1 week)

- Neo4j cluster (5.x with vector indexes + GDS) provisioned.
- Constraints and vector indexes from [01-schema.md](01-schema.md) §5 applied.
- Embedding model selected (e.g. `text-embedding-3-large` or an internal model). Dimensions fixed across the schema.
- LLM endpoint(s) chosen for extraction (strong) and filter-stage-1 (cheap).
- Logging / metrics scaffolding (every Cypher query, every LLM call, latency, token cost).
- Skeleton service: HTTP `/query` endpoint that accepts `(query, user_groups)` and returns a stub answer. Wire end-to-end before adding intelligence.

**Exit criterion**: a no-op `/query` round-trips through the stack with full observability.

## Phase 1 — Wiki ingest, vector-only retrieval (2 weeks)

- Wiki crawler + webhook handler from [02-ingestion.md](02-ingestion.md) §1.
- Page upsert + chunk + embed only — **no LLM extraction yet**.
- Native `:LINKS_TO` edges from wiki hyperlinks.
- Pattern A (vanilla vector) wired through the router.
- `:VISIBLE_TO` overlay implemented end-to-end (don't bolt this on later — too easy to leak via dev shortcuts).

**Exit criterion**: useful for FACT queries; baseline metrics established. Compare against existing search tool.

## Phase 2 — Entity extraction (3 weeks)

- Extraction prompts tuned against a labelled dev set of ~100 chunks ([03-extraction-prompts.md](03-extraction-prompts.md)). Aim ≥ 80% precision and ≥ 65% recall on entities, ≥ 70% precision on relations.
- Alias resolver per [04-alias-resolution.md](04-alias-resolution.md), with manual-review queue UI for ambiguous cases.
- Pattern B (entity lookup + 1-hop) added to the router.

**Exit criterion**: ENTITY_LOOKUP queries return typed-context answers that beat the Phase-1 baseline on a 50-question evaluation set.

## Phase 3 — Multi-hop and dual-level (2 weeks)

- GDS graph projection set up; PPR query (Pattern C) implemented.
- Dual-level (Pattern D) query implemented.
- Query classifier added.

**Exit criterion**: MULTI_HOP queries (the hardest class for vector RAG) demonstrate clear lift. Refusal rate stays low because patterns degrade gracefully — if PPR returns nothing, Pattern A still answers.

## Phase 4 — Filter and fallback (1 week)

- Two-stage filter from [06-filtering-fallback.md](06-filtering-fallback.md) wired between router and answer generator.
- Asymmetric LLM-only branch + `_integrate` + refuse mode.
- Confidence thresholds tuned against held-out eval set; refuse-rate dashboard live.

**Exit criterion**: hallucination rate (measured by an LLM-judge on a 100-question eval set) drops to < 5%. Operators can read the refuse-rate dashboard.

## Phase 5 — Model-doc ingest and cross-source resolution (2 weeks)

- ModelDoc ingestor from [02-ingestion.md](02-ingestion.md) §2.
- Alias resolver tuned for cross-source preference rules ([04-alias-resolution.md](04-alias-resolution.md) §6).
- Versioning per [01-schema.md](01-schema.md) §7.

**Exit criterion**: "Show me the documentation for X model" works across both wiki and external doc store; doesn't surface stale model-doc versions by default.

## Phase 6 — Lazy global summaries (1-2 weeks, optional)

- Pattern E implemented per [05-query-router.md](05-query-router.md) §6.
- Cache layer with TTL + invalidation per [07-updates.md](07-updates.md) §8.

**Exit criterion**: GLOBAL_SUMMARY queries return useful multi-page sensemaking output without timing out or blowing the LLM budget.

## Phase 7 — Hardening (ongoing)

- Drift detection (refuse-rate spikes by class).
- Threshold retuning cadence (monthly).
- Adversarial / prompt-injection red-teaming.
- Quality eval set growth (target 500 labelled queries by month 6).

## Evaluation methodology (run continuously from Phase 1)

Build a query eval set from day one. Per Edge et al. 2024 §3, **adaptive question generation** is a good seed:

1. Sample 50 wiki pages stratified by topic.
2. Per page, ask an LLM to generate one query for each class (FACT, ENTITY_LOOKUP, MULTI_HOP, COMPARATIVE, GLOBAL_SUMMARY).
3. A quant SME labels the gold answer.

Metrics per class:

- **Faithfulness**: did the answer make claims supported by the cited chunks?
- **Coverage**: did the answer include the gold-answer key facts?
- **Hallucination**: any unsupported claim → 0; otherwise faithfulness score.
- **Latency**: p50, p95.
- **Cost**: tokens in + out per answered query.

Refuse-rate is tracked separately (it's a feature, not a failure — but spiking refuse-rate without a content explanation indicates regression).

## Cost ballpark

Order-of-magnitude only:

- **Indexing** (one-off + incremental): dominant cost is LLM extraction. ~$0.01–0.10 per page depending on length and which model. A 50k-page wiki costs $500–5000 to index initially; incremental updates are negligible.
- **Per-query**: 
  - Pattern A (vector only): ~1500 input tokens + 500 output → $0.005 with `gpt-4o`, $0.001 with `gpt-4o-mini`.
  - Pattern C (PPR + 30 paths through stage-1 filter): ~3500 input tokens + 800 output → $0.02.
  - Pattern E (global summary, uncached): 10–30 LLM calls → $0.20–1.00. Cache makes the amortised cost ~10× lower.

Plan for $0.02 average per query at steady state.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Extraction quality plateau on quant terminology | Continuous few-shot exemplar curation by SMEs; per-domain fine-tune later |
| Alias resolver over-merging | Bias prompt toward "new"; audit log + reversibility ([04-alias-resolution.md](04-alias-resolution.md) §7) |
| Entitlement leak | All retrieval Cypher injects `:VISIBLE_TO` filter inline; no post-hoc filtering; pen-test before launch |
| Wiki structure changes break native-link parser | Parser unit tests on captured page samples; fallback degrades gracefully (no `:LINKS_TO` ≠ broken graph, just less precision) |
| LLM-only fallback misused as primary | Threshold `tau_L = 1.0` is intentionally tight; UI must surface "not from internal source" warning prominently |
| Compliance audit on a generated answer | Every `Query` row stores citations + LLM model version + threshold values used. Auditable replay possible. |
