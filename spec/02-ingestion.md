# 02 — Ingestion Pipelines

Two ingestors: **Wiki** (the source of truth for entity definitions) and **ModelDoc** (external documentation that must align to wiki entities).

## 1. Wiki ingestor

### 1.1 Stages

```
fetch → parse → upsert Page → emit native links → chunk → embed
                                                    ↓
                                            extract entities/relations (LLM)
                                                    ↓
                                            alias-resolve to canonical
                                                    ↓
                                            MERGE entities + edges
```

### 1.2 Page upsert

Idempotent on `id`. Compute `content_hash` from the canonical text. If hash unchanged from the previous ingest, **skip extraction** entirely — saves the dominant cost item (LLM extraction).

```cypher
MERGE (p:Page {id: $id})
SET p.source = $source,
    p.title = $title,
    p.url = $url,
    p.text = $text,
    p.embedding = $embedding,
    p.last_edited_ts = $last_edited_ts,
    p.content_hash = $content_hash,
    p.ingested_ts = datetime();
```

### 1.3 Native link edges

Parse the wiki page's HTML/markdown for outbound hyperlinks. For each that resolves to another known page id (or one that *will* be ingested in the same run):

```cypher
MATCH (src:Page {id: $src_id})
MERGE (dst:Page {id: $dst_id})              // creates a stub if not yet ingested
ON CREATE SET dst.source = $dst_source, dst.ingested_ts = datetime()
MERGE (src)-[r:LINKS_TO {anchor_text: $anchor}]->(dst)
SET r.last_seen_ts = datetime();
```

Stubs are filled in when the destination page is ingested (its later `MERGE (p:Page {id: $id})` finds the stub).

**Garbage-collect stale links**: after ingest of page `P`, delete any `(P)-[r:LINKS_TO]->()` where `r.last_seen_ts` is older than this ingest run — the link no longer appears in the page text.

### 1.4 Chunking

Per Edge et al. 2024 §6.1 (chunk-size sensitivity): default to **600 tokens with 100-token overlap**. Larger chunks halve entity recall unless self-reflection (gleaning) is used; for a quant wiki, 600/100 is a reasonable starting point because pages are typically short anyway.

```python
chunks = sliding_window(page.text, size=600, overlap=100)  # tokens, not chars
for i, chunk_text in enumerate(chunks):
    upsert_chunk(
        id=f"{page.id}#{i}",
        parent_id=page.id, parent_kind="Page",
        text=chunk_text, position=i,
        embedding=embed(chunk_text),
    )
```

### 1.5 Entity & relation extraction

See [03-extraction-prompts.md](03-extraction-prompts.md). The extractor returns a JSON payload of typed entities and typed relations. Every entity carries its raw surface form *and* a proposed canonical name — the alias resolver in [04-alias-resolution.md](04-alias-resolution.md) decides whether to merge or create.

### 1.6 Self-reflection ("gleaning")

Per Edge et al. 2024 §2.1: after the first extraction pass, send a single yes/no continuation prompt asking whether the extractor missed anything. If "yes", run a second pass on the same chunk with a `previously_extracted` block in the prompt to avoid duplicates. Skip if cost is a concern; chunks of 600 tokens are below the recall cliff and gleaning may be unnecessary at this size.

## 2. ModelDoc ingestor

### 2.1 Stages

```
fetch → parse (PDF/Word/HTML) → upsert ModelDoc → chunk → embed
                                              ↓
                                  extract entities/relations
                                              ↓
                                  alias-resolve (must match existing wiki canonical)
                                              ↓
                                  MERGE entities + DOCUMENTS edge
```

### 2.2 Critical difference: alias resolution is one-way preferred

When a model doc mentions "SABR-LMM v3.2", we want the entity to bind to the existing canonical `(:Model {name: "SABR-LMM"})` from the wiki, **not** create a new node. The alias resolver biases strongly toward matching existing canonicals — see [04-alias-resolution.md](04-alias-resolution.md) §3.

### 2.3 Linking to model

After extraction, attempt to identify the *primary subject* of the doc (typically from title + abstract + first chunk's named entities). Once resolved to a canonical `(:Model)`:

```cypher
MATCH (d:ModelDoc {id: $doc_id})
MATCH (m:Model {name: $primary_model_name})
MERGE (d)-[r:DOCUMENTS]->(m)
SET r.version_match = ($doc_version = m.version),
    r.confidence = $confidence;
```

### 2.4 Versioning

Each ingest creates a new `(:ModelDoc)` node (do not overwrite). When an updated version arrives:

```cypher
MATCH (old:ModelDoc {id: $old_id})
SET old.effective_to = $new_effective_from
WITH old
CREATE (new:ModelDoc {id: $new_id, ..., effective_from: $new_effective_from});
```

The `(:Model)` node is unchanged — only the `:ModelDoc` history grows.

## 3. Run modes

**Initial bulk** — full corpus crawl, no skip.

**Incremental** (recommended cron / webhook trigger) — for each source page:
1. Fetch.
2. Compute `content_hash`.
3. If hash matches DB, **skip everything** (no embed, no extract).
4. Else: re-embed, re-extract, re-merge.

The MERGE-everything-on-name semantics from [01-schema.md](01-schema.md) §3 makes incremental safe.

## 4. Failure modes and recovery

- **LLM extraction times out** — keep the `:Page` upsert, mark `extraction_status = "failed"`, retry next run. Don't drop the page from the graph.
- **Alias resolver ambiguous** — create the alias as `:Alias {needs_review:true}`, do not merge automatically. See [04-alias-resolution.md](04-alias-resolution.md) §4.
- **Page deleted from wiki** — webhook handler runs `DETACH DELETE` on the `:Page`. Native `:LINKS_TO` edges from other pages will be GC'd in their own next ingest. **Do not** delete `:Entity` nodes whose only `:MENTIONED_IN` was this page — keep them and set `n.orphan_flagged_ts = datetime()` per [07-updates.md](07-updates.md) §7 so they surface in the orphan-review queue.
