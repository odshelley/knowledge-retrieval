# 05 — Query Router

Five retrieval patterns, each suited to a different query type. Per [[graphrag-bench]] (Xiang et al. 2025), the wrong pattern *hurts* — vanilla vector RAG beats GraphRAG on Level-1 fact retrieval, while GraphRAG dominates on multi-hop and global queries. Routing matters as much as retrieving.

## 1. Query classification

A small classifier (cheap LLM call or fine-tuned classifier) tags the query with one of:

| Class | Example query | Pattern |
|---|---|---|
| `FACT` | "What is the formula for d1 in Black-Scholes?" | A — vanilla vector |
| `ENTITY_LOOKUP` | "What is SABR-LMM?" | B — vector seed + 1-hop |
| `MULTI_HOP` | "Which models depend on the SVI calibration?" | C — Personalized PageRank |
| `COMPARATIVE` | "How does Heston compare to SABR for variance swap pricing?" | D — dual-level |
| `GLOBAL_SUMMARY` | "What's our overall approach to FX volatility modelling?" | E — lazy Leiden + map-reduce |

Classifier prompt:

```
Classify this query into one of: FACT, ENTITY_LOOKUP, MULTI_HOP, COMPARATIVE, GLOBAL_SUMMARY.
Return JSON: {"class": <one of>, "entities": [<surface forms of named entities found in query>], "reasoning": "<one sentence>"}.

Query: {query}
```

The `entities` field is reused by patterns B, C, D as seed inputs.

If the classifier returns low confidence (or the user explicitly asks "compare" / "summarise" / "list" with mixed intent), default to **D** (dual-level) — it's the most balanced and works acceptably across types per Guo et al. 2024.

## 2. Pattern A — Vanilla vector (FACT)

```cypher
CALL db.index.vector.queryNodes('chunk_emb', $k, $q_embedding)
YIELD node AS chunk, score
MATCH (chunk)-[:PART_OF]->(parent)
WHERE EXISTS { (parent)-[:VISIBLE_TO]->(g:Group) WHERE g.name IN $user_groups }  // entitlements
RETURN chunk.text, chunk.id, parent.title, parent.url, score
ORDER BY score DESC
LIMIT $k;
```

No graph traversal at all. The graph still helps via embedding quality (entity-typed pages have richer text), but retrieval is pure vector. `$k` typically 5-10.

## 3. Pattern B — Entity lookup + 1-hop (ENTITY_LOOKUP)

Resolve the named entity → vector match → expand 1 hop to grab connected typed entities → return as a small structured context.

```cypher
// Step 1: resolve query entity to canonical
WITH $query_entities AS qents
UNWIND qents AS qe
CALL db.index.vector.queryNodes('model_emb', 1, $qe_embedding)
  YIELD node AS m, score WHERE score > 0.85
WITH collect(m) AS seeds

// Step 2: 1-hop expansion across typed relations
UNWIND seeds AS s
MATCH (s)-[r]-(neighbour)
WHERE type(r) IN ['USES','DEPENDS_ON','CALIBRATES_WITH','APPLIES_TO',
                  'HEDGES_WITH','SUPERSEDES','OWNED_BY','SUBJECT_TO']
  AND r.confidence > 0.6
RETURN s, type(r) AS rel, neighbour, r.confidence AS conf
ORDER BY conf DESC
LIMIT 50;

// Step 3: attach provenance chunks (top 3 mentions per entity)
UNWIND seeds + collect(neighbour) AS n
MATCH (n)-[m:MENTIONED_IN]->(c:Chunk)
WITH n, c, m.confidence AS mc
ORDER BY mc DESC
WITH n, collect({chunk: c, conf: mc})[0..3] AS chunks
RETURN n, chunks;
```

The LLM gets: the seed entity, its description, its typed neighbours, and 3 provenance chunks per entity. Compact and high-signal.

## 4. Pattern C — Personalized PageRank (MULTI_HOP)

Per [[personalized-pagerank-retrieval]] / Gutiérrez et al. 2024 §4. The single most useful retrieval pattern for "what depends on X" / "what uses Y" / "what's connected to Z".

```cypher
// Project the typed-entity graph (cached as a named GDS graph; refresh on schema changes)
CALL gds.graph.project.cypher(
  'entity_graph',
  'MATCH (n) WHERE n.canonical = true RETURN id(n) AS id, labels(n) AS labels',
  'MATCH (a)-[r]->(b)
   WHERE a.canonical = true AND b.canonical = true
     AND r.confidence > 0.6
   RETURN id(a) AS source, id(b) AS target, r.confidence AS weight, type(r) AS rel'
);

// Resolve query entities to graph node ids
MATCH (n) WHERE n.canonical = true AND n.name IN $query_entity_names
WITH collect(id(n)) AS seedIds

// Run PPR
CALL gds.pageRank.stream('entity_graph', {
  sourceNodes: seedIds,
  dampingFactor: 0.5,
  relationshipWeightProperty: 'weight'
})
YIELD nodeId, score
WHERE score > $ppr_threshold
WITH gds.util.asNode(nodeId) AS entity, score
ORDER BY score DESC
LIMIT 30

// Attach provenance
MATCH (entity)-[m:MENTIONED_IN]->(c:Chunk)
WITH entity, c, m.confidence AS mc, score
ORDER BY entity, mc DESC
WITH entity, score, collect({chunk: c, conf: mc})[0..2] AS chunks
RETURN entity, score, chunks;
```

Per HippoRAG: dampening 0.5, rescore by node-specificity (IDF analogue) computed as `1 / log(1 + mention_count)` — pre-compute and store on each canonical node. Score weighting:

```python
final_score = ppr_score * node_specificity
```

The LLM gets the top-K weighted entities + their provenance chunks, framed with the typed relations between them.

## 5. Pattern D — Dual-level retrieval (COMPARATIVE)

Per [[dual-level-retrieval]] / Guo et al. 2024. Two parallel retrievals on the same query:

- **Low-level**: vector match against `Entity.embedding` (entity descriptions). Captures specifics.
- **High-level**: vector match against a synthesised "theme" embedding from the entire query. Captures abstract intent.

```cypher
// Low-level: entity vector match across typed labels
CALL {
  CALL db.index.vector.queryNodes('model_emb',     5, $q_embedding) YIELD node, score RETURN node, score
  UNION
  CALL db.index.vector.queryNodes('method_emb',    5, $q_embedding) YIELD node, score RETURN node, score
  UNION
  CALL db.index.vector.queryNodes('product_emb',   5, $q_embedding) YIELD node, score RETURN node, score
}
WITH collect(DISTINCT node) AS low_seeds

// High-level: theme retrieval — match against page-level embeddings (pages encode themes)
CALL db.index.vector.queryNodes('page_emb', 10, $q_theme_embedding) YIELD node AS page, score
WITH low_seeds, collect({page: page, score: score}) AS high_pages

// Union the subgraphs
UNWIND low_seeds AS seed
MATCH (seed)-[r]-(n) WHERE r.confidence > 0.6
WITH low_seeds, high_pages, collect(DISTINCT n) AS expanded_low
RETURN low_seeds, expanded_low, high_pages;
```

`$q_theme_embedding` is the embedding of `"Theme: " + LLM_summarise_query_intent(query)` — a one-line LLM call, cheap. Per Guo et al., this is what separates dual-level from naive vector RAG.

## 6. Pattern E — Lazy Leiden + map-reduce (GLOBAL_SUMMARY)

For genuinely global queries. Skip MS GraphRAG's eager pre-build; run on demand. Cache results.

```python
def global_summary(query: str, query_entities: list[str]) -> str:
    # 1. Identify the relevant subgraph (2-hop neighbourhood of seed entities)
    cache_key = hash((sorted(query_entities), date.today()))
    if (summaries := cache.get(cache_key)):
        return _map_reduce(query, summaries)

    seeds = resolve_entities(query_entities)
    subgraph = run_cypher("""
        MATCH (s) WHERE s.name IN $seeds
        MATCH p = (s)-[*1..2]-(n) WHERE all(rel IN relationships(p) WHERE rel.confidence > 0.6)
        WITH collect(DISTINCT n) + [s] AS nodes
        RETURN nodes
    """, seeds=query_entities)

    # 2. Project to GDS, run Leiden
    project_subgraph_to_gds(subgraph, name="qsub")
    communities = run_cypher("""
        CALL gds.leiden.stream('qsub', {gamma: 1.0})
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId) AS n, communityId
    """)

    # 3. For each community, generate a summary (bottom-up if depth > 1)
    summaries = []
    for cid, members in group_by_community(communities):
        provenance_chunks = fetch_chunks_for(members, top_n=10)
        s = llm_summarise(members, provenance_chunks)
        summaries.append(s)

    cache.set(cache_key, summaries, ttl=hours(24))

    # 4. Map-reduce
    return _map_reduce(query, summaries)


def _map_reduce(query: str, summaries: list[str]) -> str:
    # Map: each community summary -> partial answer + helpfulness score
    partials = []
    for s in summaries:
        out = llm.json({
            "system": "Given the community summary, produce a partial answer to the query. Score helpfulness 0-100.",
            "user": f"Query: {query}\n\nSummary:\n{s}",
            "schema": {"answer": "string", "helpfulness": "int"}
        })
        if out["helpfulness"] > 0:
            partials.append(out)

    # Reduce: concatenate top-helpfulness partials, generate final
    partials.sort(key=lambda p: -p["helpfulness"])
    context = "\n\n".join(p["answer"] for p in partials[: _fits_in_8k(partials)])
    return llm.complete(
        f"Query: {query}\n\nPartial answers from analysis of the corpus:\n{context}\n\nFinal answer:"
    )
```

TTL of 24h is a reasonable starting default: the wiki changes daily, but a one-day-stale global summary is still useful. Invalidate the cache on entity-set churn (e.g. if a seed entity's `:MENTIONED_IN` count changes by > 20%).

## 7. Cross-cutting: entitlements

Every Cypher in this file omits the `:VISIBLE_TO` filter for clarity. In production, **add it inline to every retrieval clause** — never filter post-hoc, that leaks node existence via response timing. Wrap retrieval in a thin Python layer that injects `WHERE EXISTS { (n)-[:VISIBLE_TO]->(:Group) WHERE g.name IN $user_groups }` into every node match.

## 8. Result shape

The router returns a uniform structure regardless of pattern, so the downstream filter ([06-filtering-fallback.md](06-filtering-fallback.md)) doesn't need to know which path was taken:

```json
{
  "pattern": "MULTI_HOP",
  "seeds": [{"label": "Calibration", "name": "SVI Swaption Vol Cube", "score": 0.91}],
  "entities": [
    {
      "label": "Model", "name": "SABR-LMM", "score": 0.87,
      "edges": [{"type": "CALIBRATES_WITH", "src": "SABR-LMM", "dst": "SVI Swaption Vol Cube", "confidence": 0.95}],
      "provenance": [
        {"chunk_id": "wiki:fx-vol-surface#3", "text": "...", "confidence": 0.92, "page_url": "..."}
      ]
    }
  ],
  "communities": null,
  "raw_chunks": []
}
```

`communities` is populated only by Pattern E; `raw_chunks` only by Pattern A.
