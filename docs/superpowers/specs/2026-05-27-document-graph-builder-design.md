# Document → Knowledge-Graph Builder — Design Spec

**Date:** 2026-05-27
**Status:** Draft for review
**Repo:** `knowledge-retrieval`
**Author:** Osian (with Claude)

---

## 1. Context & motivation

The current `knowledge-retrieval` pipeline is a graph **enricher**: it assumes documents
are already curated (classified as paper/book, already present as nodes in the legacy
`portmanteau` graph) and layers chunks/embeddings/summaries on top of a mirrored copy of
that curated structure. Its discovery step reads the *list* of documents from the legacy
Neo4j DB, and `legacy_graph_mirror` / `structural_overlay` copy the pre-existing backbone.

We want the opposite: a **builder** that starts from raw documents the graph has never
seen, and constructs the graph from scratch. This spec describes a new, standalone pipeline
that runs **in parallel** to (and ultimately supersedes) the enrichment path, producing a
graph that mimics the *shape* of the alethograph graph but is **born entirely from the
documents themselves** — no dependency on, and no enrichment of, the existing curated DBs.

### Relationship to the existing setup

| Instance | Console name | Role in this design |
|---|---|---|
| `bd4528e1` | **portmanteau** | Legacy curated source-of-truth. **Untouched** by this pipeline. |
| `6b371650` | **alethograph** | Active graph. **Wiped and rebuilt** from scratch by this pipeline. |

The new builder **owns** `6b371650`. The old enrichment machinery that wrote to it is
retired (see §11).

---

## 2. Goals & non-goals

### Goals (v1)
- Point the pipeline at a **configurable source location** (a local folder for v1; designed so a cloud/online source can be added later).
- Ingest **papers only** on a **daily schedule**, automatically picking up new documents.
- **Parse** each document with a math-aware, self-hosted parser (Docling), preserving equations as LaTeX, with OCR for scanned pages.
- **Chunk** (equation-aware) and **embed** chunks.
- **Extract** entities and relationships against a **predefined schema** (the alethograph schema, extended — §6).
- Produce a **rich structured analysis** per paper (alethograph-skill quality) *and* promote mathematical content (definitions, theorems) to **first-class graph nodes** (Option "C").
- **Resolve entities** so the same concept doesn't become duplicate nodes, with every decision recorded.
- Write everything to `6b371650`.

### Non-goals (v1) — deferred extensions
- **Books.** Different structure (chapters), much longer, heavier OCR, different analysis template — and a known failure mode in the current pipeline (books time out on extraction). Added later as a clean extension.
- **Cloud/online sources** (S3, GDrive). Config is designed to allow it; not implemented in v1.
- **Human-in-the-loop entity-resolution review UI.** The decision-recording backbone is built now; the review loop is a later increment (§7).
- **Website integration.** The alethograph-explorer site will consume the analyses, but the web adapter is an explicit, agreed **downstream task** (§9), not part of v1.
- **Local/self-hosted LLMs.** Extraction + embeddings stay on hosted APIs (decision in §8).
- **First-class equation/notation querying beyond definitions & results** (e.g. every inline formula as a node). Out of scope.

---

## 3. Settled decisions (the forks we resolved)

1. **Builder, not enricher** — graph is built from raw documents; no read from or mirror of the legacy DB.
2. **Standalone & parallel** — supersedes the enrichment path; new asset graph.
3. **Papers only for v1**; books deferred.
4. **Reuse the alethograph schema, extended** with `Definition` and `Result` node types (§6).
5. **Parser: Docling (Granite-Docling VLM), self-hosted, two modes** — fast text path for clean digital PDFs, OCR/VLM path for scanned/image PDFs. **No Mathpix** in v1 (kept as a documented back-pocket fallback for equation-dense scans Docling mangles).
6. **Drop `SimpleKGPipeline`** (experimental orchestration); build chunk→embed→extract→resolve→write ourselves. Keep stable primitives: Docling, Neo4j driver + native vector index, the embedding/LLM SDKs, Dagster, MinIO, Postgres.
7. **Extraction + embeddings on hosted APIs** (OpenAI / Anthropic). Evaluate a model stronger than `gpt-5-nano` for bespoke extraction (quality knob, not a commitment).
8. **Entity resolution: conservative embedding-threshold**, split-when-unsure, every decision logged to Postgres; defer the human-review loop.
9. **Analysis = Option C**: rich narrative analysis **and** first-class `Definition`/`Result` nodes.
10. **Analysis storage**: canonical structured JSON (math as LaTeX) is the source of truth; HTML (KaTeX) is a render target for the website.
11. **Target DB: `6b371650` (alethograph), wiped and rebuilt.** Snapshot first; re-assert constraints/indexes. `bd4528e1` (portmanteau) untouched.

---

## 4. Architecture — asset DAG

```
[daily Dagster schedule] ── scans configured source folder ──▶ DynamicPartitions (1 per document, key = content hash)

one-time / admin job:
  reset_graph:  Aura snapshot ─▶ batched DETACH DELETE ─▶ re-assert constraints + vector indexes (init_neo4j)

per-document partition:

  raw_blob ──▶ parsed_document ──▶ triage_metadata ──▶ chunks ──▶ chunk_embeddings ─┐
  (register    (Docling: route      (confirm it's a    (equation-  (text-embedding-  │
   PDF in       digital→text vs       paper; extract     aware       3-small)         │
   MinIO,       scanned→OCR/VLM;      title/authors/     splitter,                    │
   hash = id)   md + LaTeX + JSON     year/arXiv|DOI;    keeps LaTeX                   │
               → MinIO)               reject junk/dupes) blocks intact)                │
                                                                                       ▼
                                              extracted_graph ──▶ resolved_entities ──▶ graph_write
                                              (bespoke LLM         (embed entity, NN-    (Cypher MERGE into
                                               extraction vs        match same-label      6b371650: Document,
                                               extended schema:     nodes; auto-merge      Paper, Author, Concept,
                                               concepts, rels,      high-conf, else        Topic, citations,
                                               definitions,         create-new; log        Chunks+embeddings,
                                               results)             decisions → Postgres)  Definitions, Results)
                                                                                       │
                                              paper_analysis ◀─────────────────────────┘
                                              (Claude structured analysis → Summary node
                                               + canonical JSON/LaTeX artifact in MinIO;
                                               HTML render deferred to website task)
```

Two structural shifts from the current pipeline:
- **Dynamic, folder-driven partitions on a schedule** replace static git-committed partitions discovered from the legacy DB.
- **A fully bespoke chain** — parse, chunk, extract, and resolve are each assets *we* control, replacing one opaque `SimpleKGPipeline` call.

---

## 5. Components

Each asset is keyed by **content hash** (so re-runs are idempotent) and has a single responsibility.

### 5.1 `source_discovery` (schedule)
- **Does:** scans the configured source folder; for each new/changed file, registers a dynamic partition keyed by SHA-256 of file bytes.
- **Config:** `SOURCE_DIR` env var (v1: local path). Interface designed so a future cloud source implements the same "list files → (key, bytes)" contract.
- **Schedule:** daily (Dagster `ScheduleDefinition`, cron). Manual trigger also available.

### 5.2 `raw_blob`
- **In:** file bytes. **Out:** object in MinIO `raw/` bucket keyed by hash.
- Establishes the immutable source artifact; hash is the document identity throughout.

### 5.3 `parsed_document` (Docling)
- **In:** `raw_blob`. **Out:** markdown + LaTeX + Docling structured JSON in MinIO `parsed/`.
- **Mode routing:** detect whether the PDF has an extractable text layer → **text mode**; else **OCR/VLM mode** (Granite-Docling). Equations emitted as LaTeX; tables as structured output.
- **Failure:** if parse yields empty/degenerate output (e.g. image PDF that still failed), **quarantine** the partition with a surfaced error — do **not** silently skip (fixes the current "image PDF → 0 chunks → silent skip" bug).

### 5.4 `triage_metadata`
- **In:** `parsed_document`. **Out:** `{is_paper, title, authors[], year, arxiv_id?, doi?}`.
- Confirms the document is a paper; extracts bibliographic metadata (drives `Paper` + `Author` nodes and dedupe by arXiv/DOI/hash). Rejects non-papers and exact-duplicate hashes.
- v1 has no paper-vs-book branch (papers only).

### 5.5 `chunks`
- **In:** `parsed_document` markdown. **Out:** ordered chunks as a Dagster asset materialization (IO manager); their final home is Neo4j `Chunk` nodes (§5.6). Each chunk has a stable `Chunk.id` = `{paper_id}:{position}`.
- **Equation-aware splitter:** never split inside a LaTeX block (`$$…$$`, `\begin{equation}…`). Target size with overlap, but boundaries snap to paragraph/equation edges rather than a hard character count.

### 5.6 `chunk_embeddings`
- **In:** `chunks`. **Out:** 1536-dim vectors (OpenAI `text-embedding-3-small`), attached to `Chunk` nodes; indexed by the existing `chunk_embedding` vector index.

### 5.7 `extracted_graph`
- **In:** `chunks` (+ metadata). **Out:** candidate entities + relationships constrained to the extended schema (§6).
- **Bespoke extraction:** our own prompts and post-validation that drop any (start,rel,end) triple not in `PATTERNS`. Hosted LLM; model chosen by the quality evaluation (§8).
- Produces: `Concept`s, `Paper CITES Paper`, `Paper DISCUSSES/STUDIES …`, plus `Definition` and `Result` candidates (§6).

### 5.8 `resolved_entities`
- **In:** `extracted_graph` candidates. **Out:** each candidate mapped to either an existing canonical node id or "create new" — with a logged decision.
- Mechanism in §7.

### 5.9 `graph_write`
- **In:** `resolved_entities`, `chunk_embeddings`. **Out:** Cypher `MERGE` into `6b371650`.
- Idempotent: keyed by `Paper.id`, `Concept.name`, `Chunk.id`, etc. Re-running a partition converges, never duplicates.

### 5.10 `paper_analysis`
- **In:** `parsed_document` (+ `extracted_graph`). **Out:** structured analysis (Claude), written as (a) a `Summary` node referencing (b) a canonical JSON artifact in MinIO with math as LaTeX.
- **Fields (papers):** motivation, contributions, method, key_results, limitations, related_work, **definitions[]**, **results[]** (theorem/lemma/proposition/corollary statements, each with LaTeX).
- The `definitions[]`/`results[]` here are the same items promoted to `Definition`/`Result` nodes — extracted once, surfaced both as queryable nodes and as analysis content.

---

## 6. Schema — reuse + extension

Reuse the existing alethograph schema verbatim (`pipeline/schema.py`: 7 node types, 19 relationship types, 28 patterns — note these include `Book` patterns which stay defined but are unused in v1).

**New node types (Option C):**
- `Definition` — properties: `id`, `statement` (markdown+LaTeX), `term`.
- `Result` — properties: `id`, `kind` ∈ {`theorem`,`lemma`,`proposition`,`corollary`}, `statement` (markdown+LaTeX), `name?` (e.g. "Theorem 3.2").

A single `Result` node with a `kind` property is used instead of four near-identical node types (they differ in label, not structure). `Definition` is separate because it *introduces* a Concept rather than asserting a relationship.

**New relationship types & patterns (subject-first, matching existing convention):**

| Start | Rel | End | Meaning |
|---|---|---|---|
| `Paper` | `STATES` | `Definition` | paper contains this definition |
| `Paper` | `STATES` | `Result` | paper states this theorem/lemma/… |
| `Definition` | `DEFINES` | `Concept` | the definition introduces a concept |
| `Result` | `USES` | `Concept` | the result depends on a concept |
| `Result` | `DEPENDS_ON` | `Result` | one result builds on another |

Also add a `Summary` node + `Paper HAS_SUMMARY Summary` (the current pipeline created `HAS_SUMMARY` ad hoc; make it explicit in `schema.py`).

**Constraints/indexes to add to `INIT_CYPHER`:** uniqueness on `Definition.id`, `Result.id`, `Summary.id`. (Chunk/Document constraints already present.)

---

## 7. Entity resolution / dedup

**v1 = conservative auto-resolution with a recorded decision trail; human review deferred.**

- For each extracted entity (primarily `Concept`, also `Definition`/`Result` terms): compute an embedding of `name + short context`.
- Nearest-neighbour search against existing same-label entities' embeddings.
  - **similarity ≥ HIGH** → MERGE into the existing canonical node.
  - **similarity < LOW** → create a new node.
  - **LOW ≤ similarity < HIGH (ambiguous band)** → **create new (split), not merge.** Duplicates are reversible by a later merge; wrong merges corrupt the graph and are hard to unwind. The pair is **flagged** for future review.
- **Every decision is recorded** in Postgres: `(candidate, matched_to, label, score, action, run_id, ts)`. An **alias map** table (`alias → canonical`) is consulted first on every resolution and is the seam future human decisions write back to.

**Storage:** Postgres is already running (Dagster metadata store). Add a schema/DB for entity embeddings + decisions + alias map, using **pgvector** for the NN search. (Chunk-level vectors stay in Neo4j's native vector index; *entity-resolution* vectors live in pgvector alongside the decision trail so resolution is self-contained.)

**Deferred (phase 2):** a CLI/UI to adjudicate the flagged band; decisions populate the alias map and future runs honour them automatically. No rework needed — the table and alias seam exist from v1.

---

## 8. Models

- **Embeddings:** OpenAI `text-embedding-3-small` (1536-dim) for both chunks and entity-resolution. Hosted.
- **Extraction:** hosted LLM. **Pre-build evaluation** picks the model — `gpt-5-nano` was chosen for SimpleKGPipeline's high call volume; for bespoke extraction where correctness matters, evaluate a stronger GPT or Claude on a handful of papers and let quality decide.
- **Analysis:** Anthropic Claude (current pipeline uses `claude-sonnet-4-6`); keep unless evaluation suggests otherwise.
- **Parsing:** Docling / Granite-Docling-258M (self-hosted, Apache-2.0). No hosted parser dependency.

---

## 9. Analysis output & website (downstream — agreed)

- **Canonical form:** structured **JSON**, every math-bearing field carrying inline **LaTeX**. Source of truth, stored in MinIO and referenced by the `Summary` node.
- **Render:** **HTML + KaTeX/MathJax** generated from the JSON for the alethograph-explorer site. Because canonical storage is structured, re-rendering to other targets (PDF, new theme) needs no re-extraction.
- **Website integration is an explicit downstream task** (not v1): a thin adapter drops rendered analyses where the explorer (`~/alethograph-explorer/`, indexes `content_index.json`) can pick them up, preserving the site's existing content contract so it "remains similar downstream." Designing the web layer is its own small piece, gated on the graph build working first. **We know we're going to do it.**

---

## 10. Error handling & idempotency

- **Content-hash identity** end-to-end → every asset re-runs safely; `graph_write` uses `MERGE` so re-processing converges, never duplicates.
- **No silent skips.** Parse/extract degenerate output **quarantines** the document with a surfaced, queryable error state — the current pipeline's habit of producing 0 chunks and skipping is treated as a bug, not a default.
- **Quarantine bucket / state** for documents that fail parse, fail triage (not a paper), or fail extraction, so failures are visible and re-drivable.

---

## 11. Keep / delete / build

- **Keep & reuse:** Dagster scaffolding, `resources.py` (Neo4j/MinIO/LLM connections), MinIO, Postgres, `schema.py` (extended per §6).
- **Delete:** `legacy_graph_mirror`, `structural_overlay`, the legacy-DB read in `discover_partitions.py`, static `partitions.json`, the MinIO PDF sensor — all enrichment machinery. Retire the old writers to `6b371650` so two pipelines never clobber the graph.
- **Build fresh:** every asset in §5, the extended schema, the equation-aware splitter, the bespoke extractor + prompts, the resolver + pgvector store, and `reset_graph`.

---

## 12. Pre-build gates (do before implementing)

1. **Docling LaTeX-fidelity spot test** — run Docling on ~5 of the gnarliest equation-heavy XVA/stochastics pages and eyeball the LaTeX. "Outputs LaTeX" ≠ "correct LaTeX for dense notation." This 10-minute test decides whether Docling alone is viable or whether Mathpix needs to come off the bench earlier than planned.
2. **Extraction-model evaluation** — compare candidate extraction models on a few papers (§8).
3. **Confirm `6b371650` is empty** (`MATCH (n) RETURN count(n)` → 0) and **a snapshot exists** before the first real rebuild run.

---

## 13. Testing strategy

- **Unit tests per asset** with small fixture PDFs (one clean digital, one scanned/image, one equation-dense).
- **Equation-aware splitter tests:** assert no LaTeX block is ever split.
- **Schema-validation tests:** extractor output containing an illegal (start,rel,end) triple is dropped.
- **Resolver tests:** known-duplicate names merge above threshold; ambiguous pairs create-new + log; alias map is honoured.
- **Idempotency test:** running a partition twice yields identical graph state (no duplicate nodes/edges).
- **Integration test:** one paper end-to-end against a disposable local Neo4j.

---

## 14. Open questions / risks

- **Docling math fidelity** (gate §12.1) — biggest unknown; mitigated by spot test + Mathpix back-pocket.
- **Extraction quality without curated wikilinks** — the curated vault previously guaranteed clean entity names; from raw text, the resolver (§7) carries more weight. Conservative splitting + the decision trail are the safety net.
- **Resolution thresholds** (HIGH/LOW) need tuning on real data; start conservative.
- **Definition/Result extraction precision** — promoting math objects to nodes is new; acceptable if precision is high even at modest recall for v1.
