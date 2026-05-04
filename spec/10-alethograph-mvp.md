# 10 — Alethograph GraphRAG MVP (and the quant-wiki swap)

A minimal-viable GraphRAG layer for the alethograph research vault, written against the schema that's already in Neo4j. Pitched as the **reference implementation** the quant-wiki version should later mirror — three named "swap points" mark the only places where the quant-wiki diverges. Replaces files 01–08 for the alethograph use case; files 04 (alias resolution), 06 (filter / fallback), and 07 (incremental updates) become deferred Phase-2 work, not Phase-1 scaffolding.

**Grounding** (literature in `~/Projects/academic-research-system/vault/`):
- [[LightRAG - Simple and Fast Retrieval-Augmented Generation|LightRAG]] — entity graph + dual-level vector retrieval + 1-hop expand. The simplest design that's still GraphRAG.
- [[HippoRAG - Neurobiologically inspired long-term memory for large language models|HippoRAG]] — Personalized PageRank over an entity graph for multi-hop. One Cypher call.
- [[When to use Graphs in RAG - A Comprehensive Analysis for Graph Retrieval-Augmented Generation|GraphRAG-Bench]] — vector RAG ties or wins on FACT queries; graph traversal pays off only on multi-hop / global summary. Default to vector, escalate to graph.
- [[From Local to Global - A Graph RAG Approach to Query-Focused Summarization|MS GraphRAG]] (community summaries) and [[Empowering GraphRAG with Knowledge Filtering and Integration|GraphRAG-FI]] (filter / fallback) are deliberately **deferred** — both are real wins but neither belongs in Phase 1.

## 1. Current alethograph state (what we're augmenting)

Live Neo4j schema (from `migrate_to_neo4j.py` + `research_tools.py`):

- **Nodes**: `Paper{id, title, authors, year, venue, doi, arxiv_id, tldr, note_path, …}`, `Concept{id, name, note_path}`, `Researcher{name}`, `Book{id, title, …, note_path}`, `Idea{id, title, query, score, note_path}`, `Topic{name, display_name, description, aliases}`, `Author{name}`.
- **Relations**: `HAS_TOPIC`, `AUTHORED`, `CITES`, `DERIVED_FROM`, `STUDIES`, `KNOWS`, `PROPOSED`, `EVIDENCED_BY`, `INVOLVES`, `USES_BOOK`, `COVERED_IN`, `BROADER_THAN` (Topic→Topic DAG, with `confidence/provenance/reasoning`), `RELATED_TO` (Topic⟷Topic).
- **Indexes**: `Topic.name` unique constraint + a btree on `Topic.display_name`. **No vector indexes anywhere yet.** No embeddings on any node.
- **Text storage**: graph is metadata-only — body text lives in vault markdown at `note_path`. The graph already encodes most of the structure that matters; what it lacks is *content* and *similarity*.
- **Current retrieval** (e.g. researcher skills): hard-coded Cypher (`db-get-researcher-notes`) returns paths, then the caller `Read`s the markdown verbatim. This is effectively "dump-all-for-topic" — Pattern A from spec/05 scoped by topic. Fine for small subtrees, breaks for "what's connected to X across topics?" or "what does the literature say about Y?".

## 2. The MVP — three additions, no removals

### 2.1 Schema additions

Two new node labels, three new edge types, and embeddings on existing nodes:

```cypher
// New: chunked vault content as first-class nodes
CREATE CONSTRAINT chunk_id IF NOT EXISTS
  FOR (c:Chunk) REQUIRE c.id IS UNIQUE;
CREATE VECTOR INDEX chunk_embedding IF NOT EXISTS
  FOR (c:Chunk) ON c.embedding OPTIONS {indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'cosine'}};

// :Chunk {id, source_note_path, ord, text, embedding, content_hash}

// New: edges from chunk to whatever entities it mentions
// (:Chunk)-[:MENTIONS]->(:Paper|:Concept|:Book|:Idea|:Topic|:Researcher|:Author)

// New: optional explicit "this chunk lives in this note"
// (:Chunk)-[:PART_OF]->(:Paper|:Concept|:Book|:Idea)  // by note_path
```

Add `embedding` (1536-dim, OpenAI `text-embedding-3-small` is fine; pick once, lock the dim) plus a vector index on the *natural retrieval key* of each existing label:

- `Paper.title` + `Paper.tldr` concatenated → `Paper.embedding` (vector index `paper_embedding`).
- `Concept.name` + first-paragraph-of-note → `Concept.embedding`.
- `Topic.display_name` + `Topic.description` → `Topic.embedding`.
- `Idea.title` + `Idea.query` → `Idea.embedding`.
- Skip `Author`/`Researcher` for now (names aren't a meaningful semantic key).

That's it for schema. No new typed entity labels. No `:Alias`. No `:VISIBLE_TO`. No community-summary nodes.

### 2.2 Ingestion — native links first, LLM second (or never)

Two pipelines, idempotent on `content_hash`:

**A. Vault chunker** (replaces "dump-all-for-topic"):
1. For each markdown file under `$ALETHOGRAPH_VAULT`, hash the body. If unchanged, skip.
2. Strip frontmatter; split body on heading boundaries with a 1500-char target / 200-char overlap (any cheap chunker — `langchain.text_splitter` or hand-rolled).
3. For each chunk: `MERGE` a `(:Chunk {id: f"{note_path}#{ord}"})` and `SET` text + embedding + `content_hash`.
4. **Native-link extraction** (the load-bearing simplification): regex `\[\[([^\]|#]+)(?:\|[^\]]+)?\]\]` over the chunk text → look up the target's canonical name in Neo4j, `MERGE (:Chunk)-[:MENTIONS]->(:TargetLabel)`. The same regex already runs in `migrate_to_neo4j.py` line 449 for backfilling `CITES`; reuse it. **No LLM call needed** — vault wikilinks are already curated.
5. On reingest: `MATCH (c:Chunk {source_note_path: $p}) DETACH DELETE c` before re-creating, so deleted chunks don't linger. (LightRAG's set-union semantics; spec/07's `content_hash` skip is preserved.)

**B. Optional LLM extraction** (Phase 1.5, only if A leaves gaps):
- Run an entity-extraction prompt over chunks where the wikilink coverage is below some threshold (say, fewer than 2 `:MENTIONS` edges per 1500 chars).
- Prompt produces JSON `[{name, label, span}]`; for each, `MATCH` an existing entity by name (case-folded) or skip if no exact match. **Never auto-create entities here** — the vault is the source of truth for what concepts exist.
- This is genuinely optional for alethograph because the wikilink density is already high. It becomes mandatory for the quant-wiki (see §4).

### 2.3 Retrieval — two patterns, run in parallel, rerank

Replace `db-get-researcher-notes` with a `retrieve(query, k=10) -> list[Chunk]` function that runs both patterns and merges:

**Pattern V (vector)**: top-k cosine over `(:Chunk).embedding`. Standard RAG. Cypher:
```cypher
CALL db.index.vector.queryNodes('chunk_embedding', $k, $query_vec)
YIELD node AS chunk, score
RETURN chunk, score;
```

**Pattern G (entity 1-hop)**: extract query entities, match the graph, expand 1 hop, return their chunks. This is the LightRAG low-level path:
```cypher
// 1. Find seed entities via vector match on the query
CALL db.index.vector.queryNodes('paper_embedding', 5, $query_vec) YIELD node, score
WITH collect(node) AS papers
CALL db.index.vector.queryNodes('concept_embedding', 5, $query_vec) YIELD node AS c, score AS s
WITH papers + collect(c) AS seeds

// 2. 1-hop expand across the meaningful relations
UNWIND seeds AS seed
MATCH (seed)-[r:DERIVED_FROM|HAS_TOPIC|CITES|INVOLVES|EVIDENCED_BY|BROADER_THAN|RELATED_TO|COVERED_IN|KNOWS*1..1]-(neighbor)
WITH collect(DISTINCT seed) + collect(DISTINCT neighbor) AS entities

// 3. Pull mentioning chunks
UNWIND entities AS e
MATCH (chunk:Chunk)-[:MENTIONS]->(e)
RETURN DISTINCT chunk LIMIT $k;
```

Merge V's and G's results, dedup, rerank by `cosine(chunk.embedding, query_vec)`, return top-k. That's it.

For multi-hop queries where 1-hop isn't enough (rare in alethograph), add **Pattern PPR** later — one `gds.pageRank.stream` call seeded on the matched entities, dampingFactor 0.5 per [[personalized-pagerank-retrieval|HippoRAG §3]]. Score chunks by sum of PPR weight × incidence. Don't build this until you observe a query class that V+G fails on.

No query classifier in MVP. Always run V and G; let the reranker sort it out. Add a classifier when you have measurement data showing it pays.

## 3. Phasing

| Phase | Scope | Exit |
|---|---|---|
| 1 (1 week) | Vault chunker + embeddings + Pattern V + Pattern G + reranker | Replaces `db-get-researcher-notes`; same notes still loaded plus more relevant ones |
| 1.5 (optional, 0.5 week) | Optional LLM extraction for low-wikilink-density chunks | Coverage measurement: % of chunks with ≥2 `:MENTIONS` edges |
| 2 (deferred) | PPR pattern (HippoRAG-style) | Only if a measured query class needs >1-hop |
| 3 (deferred) | Filter / fallback per [[Empowering GraphRAG with Knowledge Filtering and Integration|GraphRAG-FI]] | Only if hallucination eval crosses threshold |
| 4 (deferred) | Lazy community summaries per [[From Local to Global - A Graph RAG Approach to Query-Focused Summarization|MS GraphRAG]] | Only for explicit GLOBAL_SUMMARY queries |

Phase 1 is the whole MVP. Everything after is on-demand.

## 4. The quant-wiki swap (three things, in order)

The architecture above is domain-agnostic if you isolate exactly three things behind a thin `DomainConfig`:

**Swap 1 — Source + native-link extractor.** Alethograph: walk `$ALETHOGRAPH_VAULT`, parse markdown, regex `[[wikilinks]]`. Quant-wiki: BeautifulSoup over wiki HTML, parse `<a href>` to internal pages, hash on rendered content. Same pipeline shape; different fetcher and parser.

**Swap 2 — Entity-label vocabulary.** Alethograph already has its labels (`Paper`, `Concept`, `Topic`, `Idea`, `Book`, `Researcher`, `Author`) and they are the right ones. Quant-wiki needs a different set (`Model`, `Methodology`, `Product`, `RiskFactor`, `Desk`, `Person` — see spec/01). The query in Pattern G changes only in *which labels carry vector indexes* and *which relation types it expands across*; the Cypher template is identical.

**Swap 3 — Extraction prompt + few-shots.** Same JSON output schema (`[{name, label, span}]`); different label vocabulary and ~5 few-shot exemplars per domain. Lives at `domains/<name>/exemplars/`. The prompt template (system text + gleaning loop) is shared.

Everything else — chunker, embedding call, vector index DDL, MERGE-on-id, reranker, PPR if added later — is shared verbatim. Refactor toward a richer `DomainConfig` (spec/09) only when a *second* domain reveals a coupling the first didn't predict. Don't preemptively design for a third domain you haven't built yet.

## 5. What this MVP deliberately does not include

- **Alias resolution** (spec/04). Alethograph wikilinks resolve unambiguously; the quant-wiki's cross-source resolution is a real problem but a separate one. Use exact + cosine ≥ 0.9 fallback in the meantime; log ambiguous and queue manually.
- **Filter / fallback** (spec/06). Real concern per [[Empowering GraphRAG with Knowledge Filtering and Integration|GraphRAG-FI]] (~17% broken-call rate without filtering), but you can measure first and add when justified. The "refuse-mode + compliance message" framing is bank-specific and irrelevant for alethograph.
- **Entitlements** (spec/01 §2 `:VISIBLE_TO`). Not applicable to alethograph.
- **Community summaries** (MS GraphRAG). Expensive, doesn't update incrementally, only buys GLOBAL_SUMMARY queries. Per [[When to use Graphs in RAG - A Comprehensive Analysis for Graph Retrieval-Augmented Generation|GraphRAG-Bench]] this isn't where the win lives for most query classes.
- **Query classifier + five-pattern router** (spec/05). Two patterns in parallel + rerank is the simplest thing that could beat the current dump-all behavior. Add routing when measurement justifies the complexity.
- **Versioning / decay** (spec/01 §7, spec/07 decay). Vault notes don't have a version concept; quant-wiki versioning is a Phase-N+1 problem.

## 6. Concrete next steps

1. Add `Chunk` constraint + vector indexes per §2.1 (one Cypher script, ~30 lines).
2. Write `vault_chunker.py` that walks `$ALETHOGRAPH_VAULT`, chunks by heading, calls the embedding API, MERGEs `Chunk` nodes and `MENTIONS` edges from existing wikilink regex. Reuse the regex in `migrate_to_neo4j.py:449`.
3. Backfill embeddings on existing `Paper`/`Concept`/`Topic`/`Idea` nodes (one batch job, ~$1–5 of OpenAI credit for the current vault).
4. Write `retrieve.py` exposing `retrieve(query, k) -> list[Chunk]` running Pattern V + Pattern G + cosine rerank.
5. Wire the researcher skills' note-loading step to `retrieve()` instead of `db-get-researcher-notes` for question-driven loads. Keep `db-get-researcher-notes` for "load all notes for this topic" sweeps.

End of MVP. Everything beyond this is contingent on measured failure modes, not prospective design.

## 7. Dagster wrapping (optional, recommended for quant-wiki)

The chunker / embedder / Neo4j-upsert pipeline is a textbook fit for Dagster — file-level dependencies, expensive idempotent ops, content-hash-based skip, observability, partition-level retries. The recommendation is **don't build it as a Dagster job from day one**. Build the chunker as a plain Python module with a single `materialise(paths: list[Path])` entrypoint; wrap as Dagster assets when the operational benefit clears the operational cost. For alethograph alone (hundreds of notes, low edit rate) the script-plus-cron path is fine. For the quant-wiki (spec/08 cites 50k pages, webhooks, audit needs) Dagster is the right answer.

### Asset graph

When the time comes, the natural shape is one `DynamicPartitionsDefinition` keyed on source path, with these assets:

| Asset | Partitions | Inputs | What it does |
|---|---|---|---|
| `source_files` | dynamic, per `note_path` (or per wiki page URL) | — | Walks the source, emits `(path, content_hash)`. New paths trigger new partitions; deleted paths trigger partition retirement. |
| `chunks` | same | `source_files` | Reads file, chunks on heading boundaries, returns `[(id, text, ord, content_hash)]`. |
| `chunk_embeddings` | same | `chunks` | Embedding API call for chunks whose `content_hash` changed since last materialisation. |
| `chunk_mentions` | same | `chunks` | Runs the wikilink regex (or BeautifulSoup `<a>` parse for the quant-wiki), resolves targets against Neo4j, returns `[(chunk_id, target_label, target_id)]`. |
| `neo4j_chunk_upsert` | same | `chunk_embeddings`, `chunk_mentions` | `MERGE` chunks + `:MENTIONS` edges, `DETACH DELETE` chunks no longer in the partition. |
| `entity_embeddings` | unpartitioned, scheduled (e.g. nightly) | — | Refreshes embeddings on `Paper/Concept/Topic/Idea`. Small relative to chunks. |

`content_hash` becomes Dagster's asset version; unchanged partitions short-circuit automatically. Failures (API rate-limit, transient Neo4j error) are retried at partition granularity, not job granularity. Backfilling embeddings on a corpus-wide change (e.g. embedding-model upgrade) is one CLI call.

### Sensors and schedules

- **Alethograph**: a `MultiAssetSensor` watching the vault root with file-mtime polling, plus a daily safety-net schedule. If you sync the vault via git, a post-commit hook firing a Dagster sensor cursor advance is even cleaner.
- **Quant-wiki**: replace the file sensor with a webhook receiver (the wiki's edit webhook → Dagster asset materialisation request). Same asset graph, different sensor.

### Cross-domain reuse

The asset graph is **shared core**; the swap points from §4 plug in as constructor arguments to a small number of ops:

- `source_files` takes a `Source` adapter (vault walker vs wiki crawler).
- `chunk_mentions` takes a `NativeLinkExtractor` (wikilink regex vs BeautifulSoup `<a>`).
- `neo4j_chunk_upsert` takes a `Domain` config supplying the entity-label set and resolution rules.

So the Dagster code repository ends up structured as `core/assets.py` (the six assets above, parameterised) plus `domains/research_vault/`, `domains/quant_wiki/`, each contributing the three adapters. One Dagster deployment can run both pipelines side-by-side against one Neo4j; partition keys are namespaced by domain to avoid collisions.

### Phase ordering

- **Phase 1**: plain Python `materialise()` function. No Dagster.
- **Phase 1.5** (alethograph) or **Phase 1** (quant-wiki): wrap as the asset graph above. Same `materialise()` body, now invoked from `@asset` decorators. No re-architecture; the function signature was designed to be wrappable.
- **Phase 2+**: Dagster's run-history table becomes the audit log spec/06 wanted (every materialisation is timestamped, versioned, attributable). The `:Query` audit-node idea from spec/01 §6 may not be needed at all if Dagster's metadata covers it.
