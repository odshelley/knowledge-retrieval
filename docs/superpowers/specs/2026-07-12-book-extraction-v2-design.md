# Book/Paper Extraction v2 — Design

**Date:** 2026-07-12
**Status:** Approved (Osian, 2026-07-12)
**Scope decision:** Shared upgrade — schema and prompt changes apply to both the paper and book pipelines; all new fields are additive so existing payloads and graph data remain valid.

## Motivation

The first real book ingestion (Williams, *Probability with Martingales*, 2026-07-12) surfaced five gaps:

1. **No notation capture.** The prompt forbids bare symbols as concepts, so "Let $W_t$ denote…" and the entire "A Guide to Notation" chapter produce nothing useful. Worse, the notation guide's glossary lines were mis-typed as `Definition` nodes (`a.e.:`, `CF:characteristic function`).
2. **Truncated statements.** Some Result statements are just the heading echoed ("Composition Lemma.") because the statement body fell across a chunk boundary and the model had nothing else to emit.
3. **No proofs.** `Result.statement` explicitly excludes proofs; proof text survives only inside embedded chunks with no structured link.
4. **Sparse dependency edges.** `depends_on` may only reference labels seen in the same chunk → 7 `DEPENDS_ON` edges for a book with ~220 numbered results.
5. **Front/back-matter junk.** Title/Copyright/Contents/Index chapters each became 1-section "chapters", got extracted (wasted Opus calls), and emitted junk Definitions from section headers.

## Design

### 1. Schema changes (`pipeline/extraction/extraction.py`) — shared, additive

- **New `Notation` model:** `{symbol_latex: str, meaning: str, concept: str = ""}`. Emitted whenever a chunk *introduces* a symbol or abbreviation. `concept` optionally names a concept from the same response (e.g. `$W_t$` → "Brownian motion"). New `notations: list[Notation]` field on `ExtractionResult`.
- **Proof capture on `Result` (link + sketch strategy):** optional `proof: {sketch: str, technique: str} | None`, filled only when proof text (or its beginning) is visible in the chunk — 2–4 sentences covering technique and key steps, never a transcription. Boolean `proof_present: bool = False` on the Result marks that proof text was visible in the chunk being extracted; since extraction is per-chunk, the chapter asset records `(result label, chunk id/position)` pairs in the chapter payload wherever `proof_present` is true, and the linking pass turns those pairs into `PROVED_IN` edges.
- **Unrestricted references:** `Result.depends_on` becomes free-text labels referencing any result anywhere in the source, forward references included. New `Definition.uses: list[str]` analogous.
- **Statement integrity:** prompt rule — never echo the heading as the statement; if the body is cut off at the chunk boundary, extract the visible part and set `statement_complete: bool = False`. `merge_results` change: on duplicate label within a section, keep the variant with `statement_complete=True`, tie-break on longer statement (chunk overlap of 600 chars means the successor chunk usually sees the full body).
- All new fields are optional with defaults → old payloads still validate; paper pipeline unaffected until its prompt/schema pick the fields up (which they do automatically, being shared).

### 2. System prompt — one shared prompt, book-aware, with exemplars

- Reframe from "STEM research papers" to "STEM research papers and mathematical books".
- Delete the "bare notation is never a concept" rule; replace with routing: symbols/abbreviations go in `notations`, named ideas go in `concepts`.
- Add the statement-body rule (§1) and a rule to emit nothing from tables of contents, indexes, or copyright text (defense in depth behind chapter classification).
- Add 2–3 few-shot exemplars (one theorem-with-proof chunk, one notation-introduction chunk). Side effect: pushes the system prompt past the 4,096-token Opus cache minimum, making the existing `cache_control` marker effective (~90% saving on prompt prefix after the first chunk).
- Net cost impact ≈ +20–25% output tokens per chunk (sketches); prompt growth is absorbed by caching.

### 3. Chapter classification (structure step, `pipeline/books/`)

- Structure step tags each chapter `role ∈ {content, notation_guide, exercises, front_matter, back_matter}`.
- Heuristics first: title regex (contents/index/copyright/preface/references/notation/exercises), page count, position in book. One cheap batched LLM tie-break call per book for ambiguous chapters only.
- `book_chapters_sensor` registers partitions only for `content | notation_guide | exercises`.
- `role` stored as a property on the `Chapter` node.

### 4. Graph schema (`pipeline/graph/schema.py`, `pipeline/books/write.py`)

New node/edge types:

- `(:Notation {id, symbol_latex, meaning})-[:DENOTES]->(:Concept)` (edge only when `concept` resolved)
- `(:Notation)-[:INTRODUCED_IN]->(:Section)`
- `(:Result)-[:DEPENDS_ON]->(:Result)` — existing type, now populated cross-chapter
- `(:Result)-[:PROVED_IN]->(:Chunk)`
- `(:Result)-[:HAS_PROOF]->(:Proof {sketch, technique})`

Notation uniqueness is **per-document**: `Notation.id = <doc_sha>:<normalized_symbol>` so `$\mu$` in Williams never collides with `$\mu$` in a finance paper. Add `notation_id` uniqueness constraint to `schema.py`; re-run `scripts/init_neo4j.py` (idempotent).

### 5. Linking pass — `book_link_resolution` asset (pass 2)

Per-book partition; a sensor triggers it when all the book's chapter partitions have materialized `book_chapter_graph_write`.

1. **Label index:** one Cypher query for `(result_id, name, kind, chapter_number)` of every Result in the book.
2. **Deterministic resolution:** normalize both sides (lowercase, strip punctuation, extract numeric tag: "Lemma 9.6" → `(lemma, 9.6)`; node "9.6. Lemma." → `(lemma, 9.6)`); match on number with kind tie-break; named-theorem phrase matching for prose references.
3. **Edge writes:** `MERGE` for `DEPENDS_ON`, `PROVED_IN` (from `proof_present` chunk positions), `DENOTES` (notation→concept). All idempotent.
4. **Fuzzy residue:** one batched cheap-model call per book (unresolved refs + full label list → match or "unmatchable"). Unmatched refs are logged and dropped — never guessed.

Properties: no new storage (Neo4j is the pending table), handles forward references, re-runnable independently of extraction. Cost: seconds of wall time, ≪ $0.01 per book.

Rejected alternatives: Postgres pending-edges table + retry sensor (same result, more infra, only benefit is edges appearing mid-ingestion); running label index in prompt (breaks forward refs, bloats prompts, order-dependent).

### 6. Testing

All offline (no API):

- Pydantic schema validation round-trips, including old-payload compatibility.
- `merge_results` prefers complete/longer statement on duplicate label.
- Label normalizer against the gnarly real cases: "5.3. MON", "Lemma 9.6", "Theorem 3.12. Skorokod representation…", named-theorem prose refs.
- Chapter-role heuristics on the Williams outline + synthetic outlines.
- One recorded-fixture test per prompt exemplar pinning extraction output shape.

### 7. Migration & re-run (approved: full Williams wipe + re-ingest, ~$3–4)

Per Osian (2026-07-12): wipe ALL current Williams data from the graph before the v2 run — not just the extraction layer. Since chapter classification (§3) changes the structure step's output anyway, the clean path is a full re-ingest.

1. Wipe script (runs immediately before the v2 re-run): delete the Williams Book node and its entire subtree (Chapters, Sections, Chunks, Document, plus Concepts/Definitions/Results left orphaned by the deletion). Also delete the smoke-test fixture book ("Stochastic Processes: A Tiny Book", `isbn:9783161484100`) and its subtree.
2. Clear all Williams Dagster partitions (book partition + 14 chapter partitions) and their materializations so the sensors treat the PDF as new.
3. Re-run `scripts/init_neo4j.py` for the new constraint.
4. `books_sensor` re-ingests: structure build with roles → chapter extraction under v2 (content/notation_guide/exercises only) → linker. Parsing + embeddings rebuild costs are negligible (OpenAI embeddings ~cents).
5. Verify against success criteria below.

## Success criteria (Williams re-run)

- Notation nodes exist, including the notation guide's entries (a.e., CF, DF…) properly typed — zero glossary lines as Definitions.
- \>50 cross-chapter `DEPENDS_ON` edges (was 7).
- `PROVED_IN` coverage on the majority of theorems; proof sketches present where proofs are visible.
- Zero heading-echo statements (statement == name).
- No Definitions/Concepts sourced from Contents/Index/Copyright; those chapters not extracted at all.
- Paper pipeline still green (`uv run pytest` + one paper extraction smoke run).
