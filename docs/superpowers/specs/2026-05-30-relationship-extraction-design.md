# Relationship Extraction — Design

**Date:** 2026-05-30
**Status:** Approved (brainstorming) → ready for implementation plan
**Scope:** Add inter-entity relationships to the bespoke document-graph builder so the
Neo4j graph actually carries the connective tissue its schema declares.

---

## 1. Problem

The pipeline extracts typed entities per paper — `Concept`, `Definition`, `Result` — and
writes them to Neo4j. But today those entities are **islands**: `Definition` and `Result`
nodes hang off their `Paper` via `STATES` and nothing else; `Concept` nodes are shared
across papers (via the resolver) but carry no relationships *among themselves* or *to* the
definitions/results that use them.

`pipeline/schema.py` already *declares* the missing edges in `PATTERNS`
(`Definition–DEFINES→Concept`, `Result–USES→Concept`, `Result–DEPENDS_ON→Result`,
`Concept–RELATED_TO→Concept`), and the docstring claims extraction validates against them —
but no asset writes them and the extraction step never extracts the relationships in the
first place. The raw material (inter-entity links) is not being produced.

Consequence: multi-hop GraphRAG queries the graph is *for* — "which theorems use concept X",
"what does Theorem 3.1 depend on", "which concepts relate to BSDE" — cannot be answered.

### Why bespoke (context)

This pipeline previously used neo4j-graphrag's `SimpleKGPipeline` and moved to a bespoke
builder for better entity-identity discipline (content-hash ids, conservative cross-paper
concept resolution, idempotent MERGEs, citation backfill). That trade inadvertently dropped
`SimpleKGPipeline`'s relationship-extraction capability. This work restores it, on top of the
stronger entity foundation.

---

## 2. Locked decisions

- **Target schema = academic / alethograph** (`pipeline/schema.py`): `Paper`, `Author`,
  `Concept`, `Definition`, `Result`, `Summary`, `Chunk`, `Document` (+ deferred `Book`,
  `Topic`, `Researcher`, `Idea`). The `spec/01-schema.md` / `spec/03-extraction-prompts.md`
  bank-quant-domain schema (`Model`, `Methodology`, `Product`, …) is an **earlier, abandoned
  design** and is NOT the target. (It remains a useful reference: its relation-extraction
  prompt already prototyped the hybrid predicate+description+confidence pattern adopted here.)
- **Topic linking and Idea construction are downstream**, out of this pipeline. The pipeline's
  job is to leave a graph rich enough for those user-driven skills: canonical `Concept` nodes,
  reachable concept↔concept structure, and accessible embeddings (Neo4j chunk vector index +
  pgvector concept embeddings). Topic nodes are never written during ingestion.
- **Relationship model = hybrid**: a small typed predicate vocabulary + a free-text
  `description` of the specific sense + a `confidence`. Validated by prior art (see §6).
- **Purpose = multi-hop GraphRAG retrieval** (and feeding the downstream Idea skill).

---

## 3. Scope & phasing

Two phases. **Both are committed**; Phase 2 is deferred for risk-sequencing, not optional.

### Phase 1 (build now) — within-paper semantic edges

| Edge | Endpoints | Risk |
|---|---|---|
| `Definition –DEFINES→ Concept` | paper-local Definition → canonical Concept | low |
| `Result –USES→ Concept` | paper-local Result → canonical Concept | low |
| `Result –DEPENDS_ON→ Result` | both paper-local Results | low |

Lower risk because each edge has at least one paper-local endpoint and needs **no cross-paper
edge merge**. Operates over entities the pipeline already extracts.

### Phase 2 (committed, next) — concept ↔ concept hybrid edges

Typed predicate (`SPECIALIZES`, `RELATED_TO`, `COMPARES_WITH`, `OTHER`) + `description` +
`confidence` + provenance (`support_count`, `source_paper_ids`). Higher risk: **both**
endpoints are canonical concepts, so the same edge recurs across papers and needs idempotent
cross-paper merge and keep-both-on-conflict handling. Full sketch in §5.

The extraction-schema change is designed once to cover both phases.

---

## 4. Phase 1 design

### 4.1 Core idea

The LLM sees only text, never our content-hash ids. So extraction emits links **by name**
(the names it just produced); `graph_write` — the single authority on identity — translates
those names into canonical concepts / result-ids and writes the edges. Unresolvable
references are dropped, never invented (faithfulness gate).

```
extracted_graph (LLM)            resolved_entities             graph_write
  definitions[i].defines  ──┐     surface→canonical map   ──┐    build maps:
  results[j].uses           │  →  (concept names)           │ →   surface→canonical
  results[j].depends_on  ───┘                               │     result-name→result_id
                                                             │    3 new MERGE passes
                                                             │    unmatched name → skip+count
```

### 4.2 Extraction schema + prompt (`pipeline/extraction.py`, mirrored in `extraction_anthropic.py`)

Add link fields to the entities already extracted:

- `definitions[].defines : list[str]` — concept names this definition defines (usually one).
- `results[].uses : list[str]` — concept names this result invokes.
- `results[].depends_on : list[str]` — result names this result depends on (e.g. `["Lemma 2.4"]`).

`EXTRACTION_SCHEMA` gains these as `array` of `string` on the respective items. `SYSTEM_PROMPT`
gains: *"For each definition, list which of the concepts you extracted it defines. For each
result, list which extracted concepts it uses and which other results (by name) it depends on.
Reference ONLY entities you already listed; if unsure, omit."* The final clause is the
**faithfulness gate** (suppresses LLM over-prediction; see §6).

### 4.3 Dataclasses + dedup (`pipeline/extraction.py`)

- `Definition` gains `defines: list[str] = field(default_factory=list)`.
- `Result` gains `uses: list[str]` and `depends_on: list[str]` (same default).
- `parse_extraction` reads the new fields (defaulting to `[]` when absent).
- `merge_results` **unions** the link lists when deduping an entity seen in overlapping chunks
  (so a result extracted from two chunks keeps the union of its `uses`/`depends_on`). Dedup
  keys are unchanged (concept name; normalized statement for def/result).

### 4.4 `resolved_entities` change (`pipeline/assets/resolved_entities.py`)

Today each resolved concept overwrites `name` with the canonical and drops the original
surface name. Add the surface name so `graph_write` can map references:

```python
resolved.append({
    "surface": c["name"],      # NEW: original extracted name, for link resolution
    "name": canonical,
    "kind": c["kind"],
    "action": action.value,
    "embedding": v,
})
```

### 4.5 `graph_write` — maps + three new MERGE passes (`pipeline/assets/graph_write.py`)

Build two lookup maps from the resolved/result rows:

```python
surface_to_canon = {c["surface"]: c["name"] for c in concepts}        # concept resolution
name_to_result_id = {r["name"]: r["id"] for r in rrows if r["name"]}  # result name → content-hash id
```

After the existing node-creating passes, in the same Neo4j session:

```cypher
-- DEFINES: paper-local Definition → canonical Concept
MATCH (d:Definition {id:$def_id}), (c:Concept {name:$canon}) MERGE (d)-[:DEFINES]->(c)
-- USES: paper-local Result → canonical Concept
MATCH (r:Result {id:$res_id}), (c:Concept {name:$canon}) MERGE (r)-[:USES]->(c)
-- DEPENDS_ON: Result → Result (both paper-local)
MATCH (r1:Result {id:$res_id}), (r2:Result {id:$dep_id}) MERGE (r1)-[:DEPENDS_ON]->(r2)
```

Reference resolution rules:
- `defines`/`uses` concept names → `surface_to_canon[name]`; if the name is not a key
  (LLM referenced a concept it didn't list), **skip and count** under a `skipped_refs` metric.
- `depends_on` result names → `name_to_result_id[name]`; skip+count if missing. Results with an
  empty `name` cannot be referenced (acceptable; recorded).

### 4.6 Idempotency

Falls out for free: all endpoints are identified by stable ids (content-hash for
Definition/Result, canonical name for Concept) and every edge is a `MERGE`. Re-running a paper
converges. Because every Phase 1 endpoint is paper-local or already-canonical, **no cross-paper
edge merge is required** in Phase 1.

### 4.7 Schema changes

**None.** `DEFINES`, `USES`, `DEPENDS_ON` are already in `RELATIONSHIP_TYPES`, and all three
patterns are already in `PATTERNS`. No new uniqueness constraints (edges have no ids). Phase 1
simply populates edges the schema already promised.

### 4.8 Tests

- **Unit**: `parse_extraction` reads the new fields and defaults them; `merge_results` unions
  link lists across overlapping chunks; `graph_write` map-building + unmatched-reference
  skip/count; correct edge-row construction.
- **Integration** (`tests/integration/test_end_to_end.py`): extend `test_one_paper_end_to_end`
  to assert `DEFINES`/`USES`/`DEPENDS_ON` edges exist with expected endpoints; extend the
  idempotency test to assert edge counts are unchanged on re-run.

---

## 5. Phase 2 design sketch (committed roadmap)

> **This phase is planned and will be built.** Recorded here so picking it up is "execute the
> plan", not "redesign". Risk-sequenced after Phase 1.

### 5.1 Extraction

Add a `concept_relations` array (EDC-style: extract freely, then canonicalize to the vocabulary):

```python
# concept_relations[]: {src: str, dst: str, predicate: str, description: str, confidence: float}
#   predicate ∈ {SPECIALIZES, RELATED_TO, COMPARES_WITH, OTHER}
```

- `src`/`dst` are concept names (resolved to canonical via the existing surface→canonical map).
- `predicate`: closed set; `OTHER` retains the raw free-text predicate in a property.
- `USES` is deliberately **excluded** from the concept-concept set to avoid overloading the
  `Result→Concept` `USES` edge.
- Faithfulness gate as in Phase 1; keep all extractions above a low confidence floor (do not
  hard-drop by default — confidence is a ranking signal, per §6).

### 5.2 Schema additions (Phase 2 only)

- `RELATIONSHIP_TYPES`: add `SPECIALIZES`, `COMPARES_WITH` (`RELATED_TO`, `USES` already present).
- `PATTERNS`: add `(Concept, SPECIALIZES, Concept)`, `(Concept, COMPARES_WITH, Concept)`
  (`Concept RELATED_TO Concept` already present).
- No new uniqueness constraints.

### 5.3 Edge model

```cypher
(c1:Concept)-[:SPECIALIZES {
    description: "...",          // the specific sense
    confidence: 0.9,            // true per-extraction confidence — NOT frequency
    support_count: 3,           // number of papers that asserted this edge
    source_paper_ids: [...]     // provenance, dedup-appended
}]->(c2:Concept)
```

### 5.4 Cross-paper merge mechanics (from prior art, §6)

- **Edge identity** = `(src_canonical, predicate, dst_canonical)`. Volatile fields set in
  `ON CREATE` / `ON MATCH`, **never** in the `MERGE` pattern → idempotent under Dagster retries.
- Both endpoints resolve to **canonical** concepts, so a Phase 2 edge can connect concepts from
  different papers; `MERGE` matches a canonical concept created by an earlier paper.
- **Keep-both on conflict**: a different predicate between the same pair is a distinct edge;
  do not overwrite. Attribute via `source_paper_ids`.
- **Symmetric `RELATED_TO`**: sort the endpoint pair before MERGE so `(A,B)` and `(B,A)`
  collapse to one edge; query as undirected.
- Aggregation on `ON MATCH`: append-and-dedupe `source_paper_ids`, increment `support_count`
  only for a new paper, keep `confidence` as max (or support-weighted mean) — distinct from
  `support_count`.
- **Provenance**: arrays initially; reify as `:Evidence` / `MENTIONED_IN` nodes only if arrays
  grow large (explicitly deferred).

Idempotent MERGE sketch:

```cypher
MATCH (a:Concept {name:$src}), (b:Concept {name:$dst})
MERGE (a)-[r:RELATED_TO]->(b)            // typed predicate as rel type; symmetric → endpoints pre-sorted
ON CREATE SET r.support_count=1, r.source_paper_ids=[$paper], r.descriptions=[$desc], r.confidence=$conf
ON MATCH SET
  r.support_count = r.support_count + (CASE WHEN $paper IN r.source_paper_ids THEN 0 ELSE 1 END),
  r.source_paper_ids = CASE WHEN $paper IN r.source_paper_ids THEN r.source_paper_ids ELSE r.source_paper_ids + $paper END,
  r.confidence = CASE WHEN $conf > r.confidence THEN $conf ELSE r.confidence END
```

---

## 6. Research grounding

Four parallel research agents surveyed prior art (2026-05-30). The hybrid model is the
established best practice, not a novel invention:

- **EDC — Extract, Define, Canonicalize** (Zhang et al., EMNLP 2024): the canonical hybrid —
  extract relations freely, snap to a typed predicate when one fits, retain free-text otherwise.
  Directly endorses our predicate + description model. https://arxiv.org/abs/2404.03868
- **SciERC** (Luan et al., 2018): proven small scientific relation vocabulary — `USED-FOR`,
  `HYPONYM-OF` (≈ our `SPECIALIZES`), `PART-OF`, `COMPARE` (≈ `COMPARES_WITH`), `FEATURE-OF`,
  `EVALUATE-FOR`, `CONJUNCTION`. Hierarchy (`HYPONYM-OF`) is kept distinct from association.
  https://aclanthology.org/D18-1360/
- **Neo4j `SimpleKGPipeline`**: typed predicates via `patterns`/`potential_schema` with
  **relationship properties** (description, confidence) and `additional_*` flags to hard-constrain;
  built-in resolvers + resolve-then-MERGE-on-canonical-id ordering keep edge endpoints canonical.
  https://neo4j.com/docs/neo4j-graphrag-python/current/user_guide_kg_builder.html
- **MS GraphRAG / LightRAG**: merge mechanics — key edges on endpoints(+predicate), keep volatile
  fields out of the MERGE pattern, sum/accumulate provenance, summarize descriptions when they pile
  up; symmetric edges via sorted endpoint pairs.
  https://microsoft.github.io/graphrag/index/default_dataflow/ ·
  https://neo4j.com/blog/developer/under-the-covers-with-lightrag-extraction/

Refinements adopted over a naive port of `spec/03`:
1. **Keep `confidence` separate from frequency** — GraphRAG/LightRAG *sum* weight (really a
   count); we store `support_count` separately from a true per-extraction `confidence`.
2. **Keep-both-with-attribution** on cross-paper conflicts rather than overwriting.
3. **Faithfulness gate**: require referencing only listed entities / allow abstention to suppress
   LLM over-prediction (Hallucination-Resistant RE, 2025: https://arxiv.org/pdf/2508.14391).

---

## 7. Optional follow-ups (not in Phase 1 scope)

- **Enforce `PATTERNS` at write time**: `graph_write` currently hard-codes edges and never
  consults `PATTERNS`; the declared whitelist is decorative. A one-line guard asserting each
  written edge is in `PATTERNS` would make the contract real. Recommended, but out of Phase 1
  scope to avoid widening it.
- **Tighten `schema.py` docstring** to state that `NODE_TYPES`/`RELATIONSHIP_TYPES`/`PATTERNS`
  are the declared vocabulary consumed by tests + bootstrap, and (until the guard above exists)
  not a runtime-enforced extraction gate.

---

## 8. Files touched (Phase 1)

| File | Change |
|---|---|
| `pipeline/extraction.py` | `EXTRACTION_SCHEMA` + `SYSTEM_PROMPT` link fields; `Definition`/`Result` dataclasses; `parse_extraction`; `merge_results` union of link lists |
| `pipeline/extraction_anthropic.py` | mirror schema/prompt (shares both already; verify link fields flow through) |
| `pipeline/assets/resolved_entities.py` | emit `surface` alongside canonical `name` |
| `pipeline/assets/graph_write.py` | build `surface_to_canon` + `name_to_result_id`; 3 new MERGE passes; skipped-refs metric |
| `tests/test_extraction.py` | new-field parse + merge-union unit tests |
| `tests/test_graph_write.py` | map-building + unmatched-ref skip + edge-row tests |
| `tests/integration/test_end_to_end.py` | assert new edges + idempotency |

`pipeline/schema.py` is unchanged in Phase 1 (edges already declared).
