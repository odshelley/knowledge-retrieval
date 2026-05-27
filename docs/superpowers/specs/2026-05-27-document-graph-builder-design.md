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
| `6b371650` | **alethograph** | Active graph. **Already wiped** (May 2026; Aura snapshot retained). **Rebuilt from scratch** by this pipeline. |

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
- **Enrich bibliography via Semantic Scholar** (reusing `research_tools.py`): abstract, TLDR, citation counts, author IDs, and `CITES` edges from the paper's references — mirroring the research skill, which sources metadata *and citations* from S2, not the PDF (§15).
- Produce a **rich structured analysis** per paper matching the **research skill's note template** *and* promote mathematical content (definitions, theorems) to **first-class graph nodes** (Option "C") (§15).
- **Resolve entities** so the same concept doesn't become duplicate nodes, with every decision recorded.
- Write everything to `6b371650`.

### Non-goals (v1) — deferred extensions
- **Books.** Different structure (chapters), much longer, heavier OCR, different analysis template — and a known failure mode in the current pipeline (books time out on extraction). Added later as a clean extension.
- **Cloud/online sources** (S3, GDrive). Config is designed to allow it; not implemented in v1.
- **Human-in-the-loop entity-resolution review UI.** The decision-recording backbone is built now; the review loop is a later increment (§7).
- **Website integration.** The alethograph-explorer site will consume the analyses, but the web adapter is an explicit, agreed **downstream task** (§9), not part of v1.
- **Local/self-hosted LLMs.** Extraction + embeddings stay on hosted APIs (decision in §8).
- **First-class equation/notation querying beyond definitions & results** (e.g. every inline formula as a node). Out of scope.
- **Interactive/curational graph layers.** Topic-DAG inference (`BROADER_THAN`/`RELATED_TO` placement), researcher auto-linking, and idea-seed proposals are *interactive* steps in the research skill that don't fit unattended batch — deferred (§15). Consistent with the earlier note that curated layers don't emerge from raw documents.
- **Per-paper learning goals.** The research skill shapes each summary around a learning goal it asks the user for. Batch has no per-paper human; it uses a fixed **standing analysis brief** instead (§15).

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
11. **Target DB: `6b371650` (alethograph) — already wiped** (May 2026; Aura snapshot retained). Rebuilt from scratch; re-assert constraints/indexes before the first build run. `bd4528e1` (portmanteau) untouched.
12. **Leverage the research skill — by vendoring, not depending.** Port its per-paper note template, its `#concept`/`#method` typing, and its top-3-references citation strategy, and **vendor a copy** of `research_tools.py`'s Semantic Scholar + graph-write logic into `pipeline/research_port.py` (§15). `research_tools.py` lives in a *different repo* (`~/Projects/alethograph`) and is **not a runtime dependency**. The skill's *agentic orchestration* is replaced by automated assets; its *proven logic and templates* are vendored.

---

## 4. Architecture — asset DAG

```
[daily Dagster schedule] ── scans configured source folder ──▶ DynamicPartitions (1 per document, key = content hash)

one-time / admin job:
  reset_graph:  (snapshot already taken) ─▶ batched DETACH DELETE ─▶ re-assert constraints + vector indexes

per-document partition  (writes serialized: max_concurrent_runs = 1):

  raw_blob              PDF → MinIO, keyed by file SHA-256  (= Document identity)
    └▶ parsed_document     Docling (text vs OCR/VLM) → markdown+LaTeX in MinIO; quarantine if empty
         ├▶ triage_metadata    establish Paper identity (DOI > arXiv-no-version > title); write Paper+Author;
         │                     S2 enrich (abstract/TLDR/counts/authors); stash top references
         ├▶ chunks             equation-aware split → chunk artifact  (no Neo4j write)
         │     └▶ chunk_embeddings   OpenAI vectors → artifact  (no Neo4j write)
         ├▶ extracted_graph    bespoke LLM (reads chunk artifact) → typed concepts, definitions, results
         │     └▶ resolved_entities   embed candidate concept, NN-match in pgvector, DECIDE only,
         │                            log decision → Postgres  (no Neo4j / no embedding write)
         └▶ paper_analysis     Claude → Summary node + canonical JSON/LaTeX  (parallel; HTML = website task)

  graph_write   SOLE Neo4j writer for document-derived content.
                Inputs: chunk_embeddings, resolved_entities, extracted_graph, triage_metadata.
                Writes (all MERGE, idempotent):
                  • Chunk nodes (+embedding) ─[:BELONGS_TO]→ Document
                  • Concept nodes  + pgvector entity-embedding upsert for new canonicals (one unit)
                  • Definition / Result nodes  (paper-local content-hash ids; never cross-merged)
                  • CITES — forward (referenced paper already present) + backward (pending_citations backfill)
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
- Establishes the immutable source artifact. **Two-level identity (see §5.4):**
  - **Document identity = file-byte SHA-256.** This is the partition key, the `raw/` blob key, and the `Document` node id. A re-download of the same bytes is the same Document.
  - **Paper identity** is a *separate* id derived in `triage_metadata` (DOI > arXiv-no-version > normalized title). Distinct files can map to the same Paper (e.g. arXiv v1 vs v2); the dedup rule in §5.4 handles that.

### 5.3 `parsed_document` (Docling)
- **In:** `raw_blob`. **Out:** markdown + LaTeX + Docling structured JSON in MinIO `parsed/`.
- **Mode routing:** detect whether the PDF has an extractable text layer → **text mode**; else **OCR/VLM mode** (Granite-Docling). Equations emitted as LaTeX; tables as structured output.
- **Failure:** if parse yields empty/degenerate output (e.g. image PDF that still failed), **quarantine** the partition with a surfaced error — do **not** silently skip (fixes the current "image PDF → 0 chunks → silent skip" bug).

### 5.4 `triage_metadata` + Semantic Scholar enrichment
- **In:** `parsed_document`. **Out:** `{is_paper, paper_id, title, authors[], year, arxiv_id?, doi?, s2_id?, abstract, tldr, citation_count, influential_citation_count, references[]}`.
- Confirms the document is a paper and extracts bibliographic metadata from the parsed front-matter (title/authors/year/arXiv/DOI).
- **Paper identity:** compute `paper_id` = **normalized DOI** if present, else **arXiv id with version suffix stripped** (`2401.12345v2` → `2401.12345`), else **normalized title**. `graph_write` MERGEs `Paper` on this id (distinct from the Document SHA-256).
- **Dedup rule (v1):** after computing `paper_id`, if a `Paper` with that id already exists *and already has an attached `Document`*, **quarantine** the new file as `duplicate-paper-different-bytes` (surfaced + re-drivable per §10) rather than creating a second Document/chunk set. True version-supersession (re-point chunks to the newest file) is deferred. Exact-duplicate *bytes* are already collapsed by the Document hash upstream.
- **Then enriches via Semantic Scholar** (vendored S2 logic in `pipeline/research_port.py`): resolve the paper on S2 by arXiv-id/DOI/title; pull abstract, TLDR, citation counts, and S2 author ids. Mirrors the research skill — metadata and citations come from **S2, not the PDF**. Writes `Paper` + `Author` nodes here (deterministic ids; not embedded/resolved, so no drift risk).
- **References → backfill:** the top references (top-3 by influential-citation count) are **stashed**, not written as edges here. `CITES` creation/backfill is owned by `graph_write` via the `pending_citations` table (§5.9). Note: the full citation graph only converges after all papers are ingested + backfilled.
- v1 has no paper-vs-book branch (papers only).
- **Vendoring note:** the S2 calls (`cmd_search`/`cmd_paper`/`cmd_references`) are **ported into `pipeline/research_port.py`** — CLI/argparse stripped, the `~/.claude/research-neo4j.json` default connection stripped, Neo4j taken from the pipeline's `Neo4jResource` (→ `6b371650`). A one-line provenance comment cites the source (`~/Projects/alethograph/skills/research/scripts/research_tools.py` @ `0f22fa6`) so future drift is traceable. `research_tools.py` is **not** imported at runtime.

### 5.5 `chunks`
- **In:** `parsed_document` markdown. **Out:** ordered chunk **artifact** (text + position) persisted to MinIO/Dagster IO — **no Neo4j write here** (`graph_write` is the sole `Chunk` writer, §5.9). Each chunk has a stable id `{document_id}:{position}` (`document_id` = file SHA-256 = partition key).
- **Equation-aware splitter:** never split inside a LaTeX block (`$$…$$`, `\begin{…}…\end{…}`). Target size with overlap, but boundaries snap to paragraph/equation edges rather than a hard character count.

### 5.6 `chunk_embeddings`
- **In:** `chunks` artifact. **Out:** 1536-dim vectors (OpenAI `text-embedding-3-small`) as an **artifact** — **no Neo4j write here**. `graph_write` (§5.9) creates the `Chunk` nodes carrying these embeddings; the existing `chunk_embedding` vector index covers them.

### 5.7 `extracted_graph`
- **In:** `chunks` (+ metadata). **Out:** candidate entities + relationships constrained to the extended schema (§6).
- **Bespoke extraction:** our own prompts and post-validation that drop any (start,rel,end) triple not in `PATTERNS`. Hosted LLM; model chosen by the quality evaluation (§8).
- **Ported from the research skill** (§15): target **3–7 concepts per paper**, each **typed `#concept` vs `#method`** (theoretical idea/object/framework vs implementable algorithm/technique) carried on `Concept.tags`; concepts are "self-contained" (make sense without the source paper). Create `Concept DERIVED_FROM Paper` so derivation count is trackable (single-source concepts are flagged thin, per the skill's health checks).
- Prompt design: start from `spec/03-extraction-prompts.md`'s JSON-extraction scaffold (system/user/gleaning prompts, confidence thresholds), but **swap in alethograph's label vocabulary and ~5 alethograph few-shot exemplars** (spec/03's exemplars are quant-wiki, not papers).
- Produces: typed `Concept`s, `Paper DISCUSSES Concept` / `Paper STUDIES Topic`, plus `Definition` and `Result` candidates (§6). (`CITES` comes from S2 in §5.4, not from chunk extraction.)

### 5.8 `resolved_entities`
- **In:** `extracted_graph` candidates (Concepts only — see §7). **Out:** for each candidate Concept, a mapping to either an existing canonical name or "create new", written as a **decision row in Postgres** and emitted as an artifact for `graph_write`.
- **Decides only.** It embeds the candidate to run the pgvector NN query and records the decision; it does **not** write Neo4j nodes and does **not** upsert the entity embedding. Those happen exactly once, in `graph_write` (§7, single-writer rule). Mechanism in §7.

### 5.9 `graph_write`
- **In:** `resolved_entities`, `chunk_embeddings`, `extracted_graph`, `triage_metadata`. **Out:** Cypher `MERGE` into `6b371650`. **Sole writer of the *derived* graph** — Chunks, Concepts (+ the pgvector entity-embedding table), Definitions, Results, and CITES. (The `Paper`/`Author` *identity* nodes are written once upstream in `triage_metadata` — deterministic ids, not embedded/resolved, so no drift; everything else is written here.)
- Writes, all idempotent `MERGE`:
  - **Chunk** nodes (+embedding) `─[:BELONGS_TO]→ Document`.
  - **Concept** nodes — and, for each *newly created* canonical, upserts its pgvector entity embedding **in the same logical step** so Neo4j and pgvector cannot drift; a crash mid-write is repaired by re-running the partition.
  - **Definition** / **Result** nodes (paper-local content-hash ids, §6).
  - **CITES** via `pending_citations` (forward + backward, §3 item 3 below).
- **CITES backfill (no separate orchestration):** a Postgres `pending_citations` table `(citing_paper_id, ref_doi?, ref_arxiv_id?, ref_title_norm, ref_s2_id?, influential_count, created_ts, resolved bool)`. After MERGEing this Paper, run two passes:
  - **Forward:** for each stashed top reference, if the target `Paper` already exists → `MERGE (citing)-[:CITES]->(target)`; else insert a `pending_citations` row.
  - **Backward:** `pending_citations WHERE NOT resolved AND (ref_doi | ref_arxiv_id | ref_s2_id | ref_title_norm) matches THIS paper's identifiers` → create the `CITES` edges and set `resolved = true`.
  - Both passes are `MERGE`, hence idempotent on re-run.
- **Idempotency keys:** `Document.id`, `Paper.id`, `Concept.name`, `Chunk.id`, **`Definition.id`**, **`Result.id`**. Re-running a partition converges, never duplicates.

### 5.10 `paper_analysis`
- **In:** `parsed_document`, S2 metadata (§5.4), `extracted_graph`. **Out:** structured analysis (Claude), written as (a) a `Summary` node referencing (b) a canonical JSON artifact in MinIO with math as LaTeX.
- **Fields = the research skill's note template** (§15), so the output is interchangeable with what the skill produces (and the website can render the same shape): frontmatter (aliases/citeKey, year, topics, authors, venue, url, `semantic_scholar_id`, citation counts, `tldr`), **Abstract**, **Summary**, **Key Contributions**, **Methodology**, **Key Findings**, **Important References** (top-3 cited), **Atomic Notes** (links to the typed concept/method nodes) — **plus** `definitions[]` and `results[]` (the Option-C extension), each with LaTeX.
- The `definitions[]`/`results[]` here are the same items promoted to `Definition`/`Result` nodes — extracted once, surfaced both as queryable nodes and as analysis content.
- **No per-paper learning goal** (batch): the Summary is written against a fixed **standing analysis brief** instead of the skill's interactive Step-C prompt (§15).

---

## 6. Schema — reuse + extension

Reuse the existing alethograph schema verbatim (`pipeline/schema.py`: 7 node types, 19 relationship types, 28 patterns — note these include `Book` patterns which stay defined but are unused in v1).

**New node types (Option C):**
- `Definition` — properties: `id`, `statement` (markdown+LaTeX), `term`.
- `Result` — properties: `id`, `kind` ∈ {`theorem`,`lemma`,`proposition`,`corollary`}, `statement` (markdown+LaTeX), `name?` (e.g. "Theorem 3.2").

**Identity — paper-local, deterministic, never cross-merged (v1):** Definitions and Results belong to the paper that states them and are *not* deduplicated across papers. Ids are content-derived so re-extraction is idempotent:
- `Definition.id = f"{paper_id}:def:{sha1(normalized_statement)[:12]}"`
- `Result.id     = f"{paper_id}:{kind}:{sha1(normalized_statement)[:12]}"`
- `normalized_statement` = whitespace-collapsed, lowercased, LaTeX-normalized text, so trivial re-extraction differences don't mint new ids.

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

**Scope — `Concept` only.** Definitions and Results are **not** resolution targets; they are paper-local and never cross-merged (§6). Only the `Concept` that a `Definition DEFINES` or a `Result USES` flows through resolution, exactly like any other extracted Concept.

- For each candidate `Concept`: compute an embedding of `name + short context`.
- Nearest-neighbour search (pgvector) against existing `Concept` embeddings.
  - **similarity ≥ HIGH** → resolve to the existing canonical name.
  - **similarity < LOW** → create new.
  - **LOW ≤ similarity < HIGH (ambiguous band)** → **create new (split), not merge.** Duplicates are reversible by a later merge; wrong merges corrupt the graph and are hard to unwind. The pair is **flagged** for future review.
- **Every decision is recorded** in Postgres: `(candidate, matched_to, label, score, action, run_id, ts)`. An **alias map** table (`alias → canonical`) is consulted first on every resolution and is the seam future human decisions write back to.

**Single-writer consistency (two stores must not drift).** `resolved_entities` *decides only* — it queries pgvector and writes the decision row; it touches neither Neo4j nor the pgvector entity-embedding table. **`graph_write` is the only writer of both** the Neo4j `Concept` node and its pgvector embedding, written together and keyed by canonical id (idempotent upsert). A crash mid-write is repaired by simply re-running the partition.

**Concurrency — serialized writes are an invariant.** The resolve→write path assumes single-threaded writes. `max_concurrent_runs = 1` is already set (`docker/dagster.yaml:23`); this spec makes it a **documented invariant**, not an accident. If concurrency > 1 is ever restored, guard the resolve→write critical section with a **Postgres advisory lock per `label`** so two partitions can't mint the same Concept simultaneously.

**Cold-start expectation.** Early papers legitimately find no NN match and create new `Concept` nodes — that is correct behaviour, not a bug. Resolution quality improves as the embedding store fills.

**Storage:** Postgres is already running (Dagster metadata store). Add a schema/DB for entity embeddings + decisions + alias map + `pending_citations` (§5.9), using **pgvector** for the NN search. (Chunk-level vectors stay in Neo4j's native vector index; *entity-resolution* vectors live in pgvector alongside the decision trail so resolution is self-contained.)

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
- **Quarantine bucket / state** for documents that fail parse, fail triage (not a paper), are a **duplicate paper with different bytes** (§5.4), or fail extraction — so failures are visible and re-drivable.

---

## 11. Keep / delete / build

- **Keep & reuse:** Dagster scaffolding, `resources.py` (Neo4j/MinIO/LLM connections), MinIO, Postgres, `schema.py` (extended per §6).
- **Delete:** `legacy_graph_mirror`, `structural_overlay`, the legacy-DB read in `discover_partitions.py`, static `partitions.json`, the MinIO PDF sensor — all enrichment machinery. Retire the old writers to `6b371650` so two pipelines never clobber the graph.
- **Build fresh:** every asset in §5; the extended schema; the equation-aware splitter; the bespoke extractor + prompts; the resolver + pgvector store; the `pending_citations` table (§5.9); `reset_graph`; and **`pipeline/research_port.py`** — a *vendored copy* of `research_tools.py`'s S2 + graph-write logic (§5.4/§15). `research_tools.py` itself is **not a runtime dependency** (lives in `~/Projects/alethograph`).

---

## 12. Pre-build gates (do before implementing)

1. **Docling LaTeX-fidelity spot test** — run Docling on ~5 of the gnarliest equation-heavy XVA/stochastics pages and eyeball the LaTeX. "Outputs LaTeX" ≠ "correct LaTeX for dense notation." This 10-minute test decides whether Docling alone is viable or whether Mathpix needs to come off the bench earlier than planned.
2. **Extraction-model evaluation** — compare candidate extraction models on a few papers (§8).
3. **Verify `6b371650` is empty post-wipe** (`MATCH (n) RETURN count(n)` → 0) and that the **Aura snapshot is retained**. (The wipe is already done — this is a check, not the wipe.)

---

## 13. Testing strategy

- **Unit tests per asset** with small fixture PDFs (one clean digital, one scanned/image, one equation-dense).
- **Equation-aware splitter tests:** assert no LaTeX block is ever split.
- **Schema-validation tests:** extractor output containing an illegal (start,rel,end) triple is dropped.
- **Resolver tests:** known-duplicate names merge above threshold; ambiguous pairs create-new + log; alias map is honoured.
- **Idempotency test:** running a partition twice yields identical graph state (no duplicate nodes/edges).
- **Definition/Result idempotency:** re-running a partition produces no duplicate `Definition`/`Result` nodes (content-hash ids hold).
- **Citation backfill test:** ingesting B-then-A (where A cites B) yields the `CITES` edge via the backward `pending_citations` pass.
- **Integration test:** one paper end-to-end against a disposable local Neo4j.

---

## 14. Open questions / risks

- **Docling math fidelity** (gate §12.1) — biggest unknown; mitigated by spot test + Mathpix back-pocket.
- **Extraction quality without curated wikilinks** — the curated vault previously guaranteed clean entity names; from raw text, the resolver (§7) carries more weight. Conservative splitting + the decision trail are the safety net.
- **Resolution thresholds** (HIGH/LOW) need tuning on real data; start conservative.
- **Definition/Result extraction precision** — promoting math objects to nodes is new; acceptable if precision is high even at modest recall for v1.
- **Parity with the research skill** — see §15. The pipeline is unattended batch, the skill is interactive-agentic; some curational outputs are deferred by design, not by oversight.

---

## 15. Parity with the research skill

The alethograph `research` skill (source repo: `~/Projects/alethograph/skills/research/` @ `0f22fa6`)
worked well and is the quality bar. It is **agentic and interactive**: Claude reads the PDF
(20-page chunks) shaped by a per-paper *learning goal*, synthesises the note, hand-picks 3–7
concepts, and proposes topic-DAG placements / idea seeds for user review. This pipeline is
**unattended batch**. The strategy is therefore: **reuse the skill's proven tools and
templates; replace only its agentic orchestration.**

### Vendor a copy into `pipeline/research_port.py` (not a runtime dependency)
`research_tools.py` lives in a *separate repo* (`~/Projects/alethograph`). We **copy the needed logic** into `pipeline/research_port.py`, stripping the CLI/argparse wrapper and the `~/.claude/research-neo4j.json` default connection, and taking the Neo4j driver from the pipeline's `Neo4jResource` (→ `6b371650`). A one-line provenance comment cites the source file + commit (`@ 0f22fa6`) so future drift is traceable.
- **Per-paper note template** → `paper_analysis` output fields (§5.10): frontmatter, Abstract, Summary, Key Contributions, Methodology, Key Findings, Important References (top-3), Atomic Notes. Output is interchangeable with the skill's notes, so the website stays compatible.
- **Concept typing** → `#concept` vs `#method` on `Concept.tags`, 3–7/paper, self-contained (§5.7).
- **Semantic Scholar enrichment + citation strategy** → vendor `cmd_search`/`cmd_paper`/`cmd_references`; abstract, TLDR, citation counts, author ids, and `CITES` from top-3 references (§5.4). *Metadata and citations come from S2, not the PDF — this was the biggest gap in the first draft.*
- **Graph-write logic** → vendor the Cypher from `cmd_db_add_paper`/`cmd_db_add_concept`/`cmd_db_cite_paper`/`cmd_db_backfill_citations`, run through the pipeline's Neo4j resource. `CITES` backfill is wired into `graph_write` via `pending_citations` (§5.9).
- **`DERIVED_FROM` derivation tracking** and the skill's health-check notions (thin/single-source concepts) inform the resolver and later review.

### Replace with an automated equivalent
- **Per-paper learning goal** → a fixed **standing analysis brief** (a project-level prompt) shapes every Summary instead of an interactive Step-C question.
- **Interactive concept dedup/enrichment** → the conservative embedding resolver + Postgres decision trail (§7).

### Deferred (interactive/curational — don't fit unattended batch yet)
- **Topic-DAG inference** (`BROADER_THAN`/`RELATED_TO` placement with confidence + provenance).
- **Researcher auto-linking** (topic-match) and **Idea-seed proposals** (speculative, scored).
- These are exactly the "curated layers" flagged earlier as not emerging from raw documents. They remain the province of the alethograph *plugin* until a later phase adds an automated or review-gated version.

### Net answer to "will it extract the same stuff?"
- **Document-derived backbone — yes:** same note template, typed concepts, citations, S2 metadata, `Paper`/`Author`/`Concept`/`Topic` + `AUTHORED`/`CITES`/`HAS_TOPIC`/`DISCUSSES`/`DERIVED_FROM`, plus the new `Definition`/`Result` nodes.
- **Curational layers — no (by design, deferred):** topic-DAG, researcher links, idea seeds, and learning-goal-personalised summaries are not reproduced unattended in v1.
