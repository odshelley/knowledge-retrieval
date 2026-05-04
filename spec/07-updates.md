# 07 — Incremental Updates

Per [[incremental-graph-update]] (LightRAG / Guo et al. 2024): the only graph-RAG variant in the literature that addresses online updates rigorously is the schema-light flat-graph approach. MS GraphRAG-style hierarchical communities don't update incrementally — you must rebuild. This spec **defers** community building to lazy/on-demand precisely so the rest of the pipeline can update cleanly.

## 1. Triggers

Three update modes:

1. **Webhook on edit** (preferred) — wiki / model-doc system pushes an event with `{source, page_id, last_edited_ts}`. Latency: seconds.
2. **Polling crawl** (fallback) — cron job walks the source listing API every N minutes, compares `last_edited_ts` against the DB. Latency: tens of minutes.
3. **Manual reindex** (rare) — operator forces a re-extract on a page (e.g. after a prompt update).

All three converge on the same handler.

## 2. The update handler

```python
def handle_update(source: str, page_id: str, force: bool = False) -> UpdateResult:
    raw = fetch_from_source(source, page_id)
    if raw is None:
        return _delete(page_id)

    new_hash = sha256(_canonicalise_text(raw.text))

    rows = run_cypher("MATCH (p:Page {id: $id}) RETURN p.content_hash AS h", id=page_id)
    # run_cypher may return None, [], or a list of row dicts; normalise to a single row.
    existing = rows[0] if rows else None
    if existing and existing.get("h") == new_hash and not force:
        return UpdateResult(skipped=True)

    # Re-embed the page
    new_emb = embed(raw.text)

    # Idempotent upsert
    run_cypher("""
        MERGE (p:Page {id: $id})
        SET p.title = $title, p.url = $url, p.text = $text,
            p.embedding = $emb, p.last_edited_ts = $ts,
            p.content_hash = $hash, p.ingested_ts = datetime()
    """, id=page_id, title=raw.title, url=raw.url, text=raw.text, emb=new_emb,
         ts=raw.last_edited_ts, hash=new_hash)

    # Re-emit native links (with GC of stale ones)
    _refresh_links(page_id, raw.parsed_outbound_links)

    # Re-chunk + re-embed chunks (only changed positions)
    _refresh_chunks(page_id, raw.text)

    # Re-extract entities + relations from chunks whose hash changed
    _refresh_extractions(page_id)

    return UpdateResult(updated=True)
```

## 3. Refreshing native links (GC of stale)

```cypher
// Step 1: bump last_seen_ts on links present in this revision
MATCH (src:Page {id: $src_id})
UNWIND $current_links AS lnk
MERGE (dst:Page {id: lnk.dst_id})
ON CREATE SET dst.source = lnk.dst_source, dst.ingested_ts = datetime()  // stub
MERGE (src)-[r:LINKS_TO]->(dst)
ON CREATE SET r.anchor_text = lnk.anchor
SET r.last_seen_ts = $ingest_ts;

// Step 2: GC links not refreshed in this run
MATCH (src:Page {id: $src_id})-[r:LINKS_TO]->()
WHERE r.last_seen_ts < $ingest_ts
DELETE r;
```

## 4. Refreshing chunks

For each new chunk position `i`, compute `chunk_hash = sha256(chunk_text)`. Only re-extract if the hash differs from the existing `Chunk {id: page_id#i}`.

```cypher
MERGE (c:Chunk {id: $chunk_id})
ON CREATE SET c.created_ts = datetime()
SET c.text = $text, c.embedding = $embedding,
    c.position = $position, c.parent_id = $parent_id,
    c.parent_kind = $parent_kind, c.content_hash = $chunk_hash,
    c.last_updated_ts = datetime()
WITH c
MATCH (p) WHERE p.id = $parent_id AND (p:Page OR p:ModelDoc)
MERGE (c)-[:PART_OF]->(p);
```

When a page shrinks (fewer chunks), delete the orphan chunks:

```cypher
MATCH (c:Chunk) WHERE c.parent_id = $parent_id AND c.position >= $new_chunk_count
WITH c
OPTIONAL MATCH (e)-[m:MENTIONED_IN]->(c)
WITH c, collect({entity: e, m: m}) AS mentions
DETACH DELETE c;
// Note: entities are NOT deleted, only their MENTIONED_IN edge to this chunk
```

## 5. Refreshing extractions

```python
def _refresh_extractions(page_id: str):
    chunks = run_cypher("""
        MATCH (c:Chunk {parent_id: $pid})
        OPTIONAL MATCH (c)<-[m:MENTIONED_IN]-(e)
        WITH c, collect(DISTINCT e) AS prev_entities
        RETURN c.id AS id, c.content_hash AS hash, c.text AS text,
               c.last_extracted_hash AS last_hash, prev_entities
    """, pid=page_id)

    for ch in chunks:
        if ch["hash"] == ch["last_hash"]:
            continue                              # text unchanged — keep existing entities

        # 1. Detach old MENTIONED_IN edges from this chunk
        run_cypher("""
            MATCH (e)-[m:MENTIONED_IN]->(c:Chunk {id: $cid})
            DELETE m
        """, cid=ch["id"])

        # 2. Run extraction
        out = llm_extract(ch["text"])

        # 3. Resolve aliases and MERGE entities + relations + new MENTIONED_IN
        for ent in out["entities"]:
            canonical = alias_resolve(ent)
            run_cypher("""
                MATCH (n) WHERE n.canonical = true AND n.name = $name
                MATCH (c:Chunk {id: $cid})
                MERGE (n)-[m:MENTIONED_IN]->(c)
                SET m.confidence = $conf, m.span_start = $ss, m.span_end = $se,
                    m.extracted_by = $model, m.extracted_at = datetime()
            """, name=canonical.name, cid=ch["id"], conf=ent["confidence"], ...)

        # Predicate is interpolated into the Cypher string (Neo4j doesn't allow
        # parametrised relationship types), so it MUST be allowlisted to prevent
        # Cypher injection. Keep this list in sync with §03-extraction-prompts.
        VALID_PREDICATES = {
            "USES", "DEPENDS_ON", "CALIBRATES_WITH", "APPLIES_TO", "HEDGES_WITH",
            "HAS_RISK_FACTOR", "SUPERSEDES", "APPROVED_BY", "OWNED_BY",
            "SUBJECT_TO", "DOCUMENTS",
        }
        for rel in out["relations"]:
            predicate = rel["predicate"]
            if predicate not in VALID_PREDICATES:
                raise ValueError(f"unknown predicate: {predicate!r}")
            src = alias_resolve_by_proposed(rel["src_canonical"], rel["src_label"])
            dst = alias_resolve_by_proposed(rel["dst_canonical"], rel["dst_label"])
            run_cypher(f"""
                MATCH (s) WHERE s.canonical = true AND s.name = $sname
                MATCH (d) WHERE d.canonical = true AND d.name = $dname
                MERGE (s)-[r:{predicate}]->(d)
                SET r.confidence = $conf, r.last_seen_ts = datetime()
            """, sname=src.name, dname=dst.name, conf=rel["confidence"])

        run_cypher("""
            MATCH (c:Chunk {id: $cid}) SET c.last_extracted_hash = $hash
        """, cid=ch["id"], hash=ch["hash"])
```

## 6. Garbage collection of edges

A relation that disappears from a page should drop in confidence, not vanish abruptly (the page might come back). Run a daily sweep:

Structural / system edges (`PART_OF`, `ALIAS_OF`, `MENTIONED_IN`, `DOCUMENTS`) carry no `confidence` and are managed by their own lifecycle (chunk refresh, alias resolution, etc.); they must be excluded from decay and hard-delete:

```cypher
// Decay edge confidence for edges not refreshed recently
MATCH ()-[r]->()
WHERE NOT type(r) IN ['PART_OF','ALIAS_OF','MENTIONED_IN','DOCUMENTS']
  AND r.last_seen_ts < datetime() - duration('P30D')
  AND r.confidence > 0.1
SET r.confidence = r.confidence * 0.9;

// Hard-delete edges that haven't been seen in 90 days OR confidence below 0.1
MATCH ()-[r]->()
WHERE NOT type(r) IN ['PART_OF','ALIAS_OF','MENTIONED_IN','DOCUMENTS']
  AND ((r.last_seen_ts < datetime() - duration('P90D'))
       OR r.confidence < 0.1)
DELETE r;
```

`:LINKS_TO` edges are exempt from decay — they're refreshed deterministically by §3 and either present or absent.

## 7. Orphan entity cleanup

After a long enough cleanup window, an entity may have zero `:MENTIONED_IN` and zero outbound/inbound typed edges. It's a candidate for removal — but **never delete automatically**. Flag and review:

```cypher
MATCH (n) WHERE n.canonical = true
OPTIONAL MATCH (n)-[m:MENTIONED_IN]->()
OPTIONAL MATCH (n)-[r]-() WHERE NOT type(r) IN ['MENTIONED_IN','ALIAS_OF']
WITH n, count(m) AS mentions, count(r) AS edges
WHERE mentions = 0 AND edges = 0
SET n.orphan_flagged_ts = datetime();
```

Operators query `MATCH (n) WHERE n.orphan_flagged_ts IS NOT NULL` to decide.

## 8. Cache invalidation

When an entity's mention count changes by > 20 % since the last cached community summary, invalidate caches that include it:

```cypher
MATCH (n) WHERE n.canonical = true AND n.name = $name
MATCH (q:Query)-[:CITED]->(n)
WHERE q.ts > datetime() - duration('P1D')
  AND q.pattern = 'GLOBAL_SUMMARY'
RETURN q.id;
// these query-result caches need invalidating
```

The community-summary cache (Pattern E) is keyed on `(seed_entity_set, date)` — invalidate when any seed entity has churned.

## 9. What does NOT need updating in place

- **Page-level embeddings** when only typo fixes change content — same hash → skipped.
- **Entity descriptions** when only one of many mentions changes — keep the canonical description until a meaningful update accumulates. Optional periodic re-summarisation pass that aggregates all mention texts and asks the LLM to update the canonical description.
- **`:Author`, `:Desk`, `:Regulation`** typically — these change rarely; admins update via a separate pipeline.
