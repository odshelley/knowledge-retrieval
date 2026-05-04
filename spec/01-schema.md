# 01 — Neo4j Schema

Domain-typed nodes (do **not** collapse to generic `:Entity`). Per Han et al. 2024 §2.2, a unified one-size-fits-all GraphRAG schema is "nearly impossible" because relations are domain-specific — the bank's quant domain has a natural type system, use it.

## 1. Source nodes (the corpus)

```cypher
(:Page {
  id: "wiki:fx-vol-surface",          // unique stable id from source system
  source: "wiki",                      // {wiki, confluence, sharepoint, code, ...}
  title: "FX Volatility Surface",
  url: "https://wiki.bank.local/fx-vol-surface",
  text: "...",                         // full page markdown/text
  embedding: [0.12, ...],              // 768-d (or whatever model emits)
  last_edited_ts: datetime(),
  content_hash: "sha256:abc...",       // change detection
  ingested_ts: datetime()
})

(:ModelDoc {
  id: "modeldoc:sabr-lmm-v3.2",
  source: "sharepoint",
  title: "SABR-LMM Model Documentation",
  url: "...",
  version: "3.2",
  effective_from: date("2026-01-15"),
  effective_to: null,                  // null = current
  text: "...",                         // full doc text (or chunked separately)
  embedding: [...],
  ingested_ts: datetime()
})

(:Chunk {
  id: "wiki:fx-vol-surface#3",         // page_id#chunk_idx
  parent_id: "wiki:fx-vol-surface",    // back-pointer
  parent_kind: "Page",                 // {Page, ModelDoc}
  text: "...",                         // ~600 tokens
  embedding: [...],
  position: 3
})

(:Chunk)-[:PART_OF]->(:Page)
(:Chunk)-[:PART_OF]->(:ModelDoc)
```

Chunks are kept separate from `Page` / `ModelDoc` because retrieval needs chunk granularity but the graph reasons over whole-document entities.

## 2. Native source links — free edges

Wiki hyperlinks are authored by quants and are higher-precision than anything an LLM will extract. Per Han et al. survey §2.2 ("explicit construction" beats "implicit construction"), prefer them where available:

```cypher
(:Page)-[:LINKS_TO {anchor_text: "SVI calibration", source: "wiki"}]->(:Page)
```

For `:ModelDoc` ingest, capture any explicit references in the doc (e.g., bibliography pointing to wiki pages) the same way.

## 3. Domain-typed entities

```cypher
(:Model {name, full_name, type, description, embedding, canonical:true})
(:Methodology {name, description, embedding, canonical:true})
(:Product {name, asset_class, description, embedding, canonical:true})
(:Instrument {name, asset_class, description, embedding, canonical:true})
(:RiskFactor {name, asset_class, description, embedding, canonical:true})
(:Calibration {name, target_model, description, embedding, canonical:true})
(:Regulation {name, jurisdiction, description, embedding, canonical:true})
(:Desk {name, asset_class, description, canonical:true})
(:Author {name, employee_id, canonical:true})
(:Concept {name, description, embedding, canonical:true})  // catch-all for things that don't fit above
```

`canonical:true` marks a resolved canonical node. Aliases live as `:Alias` nodes pointing at the canonical (see [04-alias-resolution.md](04-alias-resolution.md)).

## 4. Typed relations

Domain-meaningful predicates. **Never use a generic `:RELATES_TO`** — the type carries the semantic load.

```cypher
// Functional relations
(:Model)-[:USES {confidence}]->(:Methodology)
(:Model)-[:CALIBRATES_WITH {confidence}]->(:Calibration)
(:Model)-[:DEPENDS_ON {confidence}]->(:Model)
(:Methodology)-[:APPLIES_TO]->(:Product)
(:Methodology)-[:APPLIES_TO]->(:Instrument)
(:Product)-[:HEDGES_WITH]->(:Instrument)
(:Product)-[:HAS_RISK_FACTOR]->(:RiskFactor)

// Lifecycle / governance
(:Model)-[:SUPERSEDES]->(:Model)
(:Model)-[:APPROVED_BY]->(:Author)
(:Model)-[:OWNED_BY]->(:Desk)
(:Model)-[:SUBJECT_TO]->(:Regulation)

// Provenance
(:Entity)-[:MENTIONED_IN {span_start, span_end, confidence, extracted_by}]->(:Chunk)
(:ModelDoc)-[:DOCUMENTS {confidence, version_match:true}]->(:Model)
```

Every LLM-extracted relation carries a `confidence` field (`0.0`–`1.0`). The native-link edges (`:LINKS_TO`) implicitly have confidence `1.0`. Filtering at retrieval time uses these values.

## 5. Indexes and constraints

```cypher
// Uniqueness
CREATE CONSTRAINT page_id        IF NOT EXISTS FOR (p:Page)        REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT modeldoc_id    IF NOT EXISTS FOR (m:ModelDoc)    REQUIRE m.id IS UNIQUE;
CREATE CONSTRAINT chunk_id       IF NOT EXISTS FOR (c:Chunk)       REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT model_name     IF NOT EXISTS FOR (m:Model)       REQUIRE m.name IS UNIQUE;
CREATE CONSTRAINT method_name    IF NOT EXISTS FOR (m:Methodology) REQUIRE m.name IS UNIQUE;
// ...one per domain-typed label

// Vector indexes (Neo4j 5.x)
CREATE VECTOR INDEX page_emb     IF NOT EXISTS FOR (p:Page)     ON p.embedding OPTIONS {indexConfig:{`vector.dimensions`:1536, `vector.similarity_function`:'cosine'}};
CREATE VECTOR INDEX modeldoc_emb IF NOT EXISTS FOR (m:ModelDoc) ON m.embedding OPTIONS {indexConfig:{`vector.dimensions`:1536, `vector.similarity_function`:'cosine'}};
CREATE VECTOR INDEX chunk_emb    IF NOT EXISTS FOR (c:Chunk)    ON c.embedding OPTIONS {indexConfig:{`vector.dimensions`:1536, `vector.similarity_function`:'cosine'}};
CREATE VECTOR INDEX model_emb    IF NOT EXISTS FOR (m:Model)    ON m.embedding OPTIONS {indexConfig:{`vector.dimensions`:1536, `vector.similarity_function`:'cosine'}};
CREATE VECTOR INDEX method_emb   IF NOT EXISTS FOR (m:Methodology) ON m.embedding OPTIONS {indexConfig:{`vector.dimensions`:1536, `vector.similarity_function`:'cosine'}};
// ...one per domain entity label

// Helpful btree indexes
CREATE INDEX page_source         IF NOT EXISTS FOR (p:Page)        ON (p.source);
CREATE INDEX page_edited         IF NOT EXISTS FOR (p:Page)        ON (p.last_edited_ts);
CREATE INDEX modeldoc_effective  IF NOT EXISTS FOR (m:ModelDoc)    ON (m.effective_from, m.effective_to);
```

## 6. Access-control overlay (sketch)

Out of scope to specify in detail, but the schema accommodates it without redesign:

```cypher
(:Group {name})
(:Page)-[:VISIBLE_TO]->(:Group)
(:ModelDoc)-[:VISIBLE_TO]->(:Group)
```

Every retrieval query then carries the user's group set as a parameter and adds `WHERE EXISTS { (n)-[:VISIBLE_TO]->(:Group) WHERE g.name IN $user_groups }`. Do **not** filter post-hoc — must be in the same Cypher query as retrieval to avoid leaking node existence via timing.

## 7. Versioning sketch (model docs)

`:ModelDoc.version`, `:ModelDoc.effective_from`, `:ModelDoc.effective_to`. Multiple versions of the same model coexist; queries default to `effective_to IS NULL` (current). Historical queries pass an `as_of` parameter. The `(:ModelDoc)-[:DOCUMENTS]->(:Model)` edge is per-version; the `:Model` node itself is canonical and version-independent.
