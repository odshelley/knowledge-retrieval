# Book Ingestion Pipeline — Design

**Date:** 2026-07-05
**Status:** Approved (pending final spec review)
**Depends on:** existing paper pipeline (post-restructure c636533, HAS_DOCUMENT fix 432b426)

## Goal

Extend the knowledge-retrieval Dagster project to ingest books into the **same Neo4j graph** as papers, with grounding good enough for a mathematician: query answers should cite *"Theorem 3.1, Cont & Tankov, §3.2, p. 71"*, and a concept mentioned in a book must resolve to the **same global `Concept` node** as the identical concept extracted from a paper.

## Decisions (from brainstorming)

| Question | Decision |
|---|---|
| Grounding granularity | **Theorem/definition level** — numbered results and definitions as first-class nodes with book/chapter/section/page location |
| Extraction coverage | **Everything, chapter-partitioned** — the whole book is eventually extracted, one Dagster partition per chapter |
| Corpus | Born-digital publisher PDFs (clean text layer, usually embedded TOC bookmarks). OCR out of scope |
| Routing | **Separate `BOOKS_SOURCE_DIR`** watched by its own sensor; no paper-vs-book auto-triage |
| Architecture | **Approach A: parallel asset lineage, shared internals** — a second asset chain in the same code location, reusing existing packages as libraries |

### Alternatives rejected

- **Branch inside existing assets** (one partition set, `doc_type` discriminator): couples both pipelines, keeps the book-timeout problem (no chapter-level restartability), regression risk on the working paper path.
- **Separate Dagster code location/repo sharing only Neo4j**: forces packaging or duplicating the resolution stack; duplicated entity resolution is how two drifting Concept spaces happen — the exact failure this project exists to avoid.

## Graph schema

New node labels: **`Chapter`**, **`Section`**. Reused: `Book` (declared in `schema.py`, currently unpopulated), `Document`, `Chunk`, `Concept`, `Definition`, `Result`, `Author`.

```
(:Book    {id, title, authors, year, edition, isbn, publisher})
(:Chapter {id, number, title, page_start, page_end})   // id = {book_id}:ch03
(:Section {id, number, title, page_start, page_end})   // id = {book_id}:ch03:s02, number = "3.2"
```

Edges:

```
(Book)-[:HAS_DOCUMENT]->(Document)          // same anchor pattern as Paper-HAS_DOCUMENT (432b426)
(Author)-[:AUTHORED]->(Book)                // already declared
(Book)-[:HAS_CHAPTER {order}]->(Chapter)
(Chapter)-[:HAS_SECTION {order}]->(Section)
(Chunk)-[:BELONGS_TO]->(Document)           // unchanged — existing chunk RAG works as-is
(Chunk)-[:PART_OF]->(Section)               // new — locates every chunk in the hierarchy
(Section)-[:STATES]->(Definition|Result)    // papers: Paper-STATES; books: Section-STATES
(Definition)-[:DEFINES]->(Concept)          // identical to papers
(Result)-[:USES]->(Concept)                 // identical to papers
(Result)-[:DEPENDS_ON]->(Result)            // identical; may cross chapters within a book
(Book)-[:COVERS]->(Concept)                 // already declared — aggregate, finally populated
```

Key properties:

- `Definition`/`Result` gain **`label`** (the book's own numbering: `"Theorem 3.1"`, `"Definition 2.14"`; null for unnumbered) and **`page`**. Ids stay content-local — `{book_id}:ch03:res:{hash12}` — mirroring the paper-local scheme so statements never falsely merge across sources.
- **Concepts are the bridge between papers and books.** `Concept.name` is globally unique; resolution is the shared stack (below). Example grounded query:

```cypher
MATCH (c:Concept {name: "Lévy-Khintchine representation"})
      <-[:DEFINES|USES]-(s)<-[:STATES]-(loc)
RETURN s.label, s.statement, loc   // loc is a mix of Paper and Section nodes
```

Schema housekeeping folded into this work:

- Add `Chapter`, `Section`, `Document` to `NODE_TYPES`; add `HAS_DOCUMENT`, `HAS_CHAPTER`, `HAS_SECTION`, `PART_OF` to `RELATIONSHIP_TYPES` (the paper pipeline's `HAS_DOCUMENT` is currently missing from the list).
- Unique constraints on `Chapter.id`, `Section.id`; extend `PATTERNS` with the new triples.
- Fix stale `pipeline.schema` / `pipeline.cypher` imports in `scripts/init_neo4j.py` and `scripts/reset_graph.py` (broken since the c636533 restructure).

## Identity

`book_id = isbn:{isbn13}` when frontmatter extraction finds one, else `title:{normalized}` — same precedence idiom as `compute_paper_id` (doi > arxiv > title). No Semantic Scholar, no external lookup in v1. Chapter/section ids derive from `book_id` plus position.

## Asset chain and partitioning

Two dynamic partition sets, two jobs, one new sensor. New assets in `pipeline/assets/`; logic in a new `pipeline/books/` package plus reused existing packages.

### Book-level partitions (`books`, key = SHA-256 of the PDF)

A sensor watches `BOOKS_SOURCE_DIR` (new env var) and registers partitions.

```
book_raw_blob → book_parsed → book_metadata
                     ↓             ↓
               book_structure → book_chunks → book_structure_write
```

- **`book_raw_blob`** — copy PDF to MinIO (books bucket).
- **`book_parsed`** — pypdfium2 as today, but keeps **per-page text** (papers flatten to one blob; books need page provenance) and reads the **PDF outline/bookmarks** for the chapter/section tree. Fallback: heading-pattern detection. Neither → quarantine.
- **`book_metadata`** — LLM frontmatter over the front-matter pages → title, authors, ISBN, edition, year, publisher → `book_id`. Replaces paper triage; nothing asks "is this a paper".
- **`book_structure`** — builds the Chapter/Section tree with page ranges; **registers one chapter partition per chapter** (`{book_sha}:ch03`) in the second partition set.
- **`book_chunks`** — reuses `split_markdown` (equation-atomic chunking is exactly right for math books), run **per section**, so every chunk is born with `section_id` + page range. Embeds via the shared `embed_texts` (`text-embedding-3-small`, 1536-dim, same `chunk_embedding` vector index).
- **`book_structure_write`** — writes Book/Author/Document/Chapter/Section/Chunk plus hierarchy edges and chunk embeddings. **After this single run, vector RAG works over the entire book**, before any extraction.

### Chapter-level partitions (`book_chapters`, key = `{book_sha}:ch{nn}`)

```
book_chapter_extraction → book_chapter_resolved → book_chapter_graph_write
```

- **`book_chapter_extraction`** — reuses the existing Pydantic extraction models and provider plumbing (OpenAI/Anthropic per `EXTRACTION_PROVIDER`), prompt extended to capture the book's own numbering (`label`) and given chapter/section context. A chapter is ~20–40 chunks: minutes per run, and a failure re-runs one chapter, not the book. Per-chunk progress logging as in 6ba754a.
- **`book_chapter_resolved`** — calls the **same `resolve_concepts()`**: same `canonical_key`, same pgvector thresholds (0.90 / 0.60), same LLM adjudicator, same `resolution_decisions` audit table. The "a Lévy process is a Lévy process" guarantee is enforced by code reuse, not convention. Decide-only, like `resolved_entities`.
- **`book_chapter_graph_write`** — reuses the existing Definition/Result/Concept edge builders with `Section` as the STATES anchor; adds `Book-COVERS->Concept` aggregates; performs the same `alias_map` / `entity_embeddings` upserts.

### Orchestration

- After `book_structure_write` succeeds, **all chapter partitions are auto-enqueued** ("everything, eventually"). `max_concurrent_runs=1` (existing `docker/dagster.yaml`) serializes them, preserving the single-writer invariant that resolution depends on.
- Chapters run in ascending order. Cross-chapter `DEPENDS_ON` back-references resolve at write time by `(book_id, label)` lookup; a **forward** reference to a not-yet-extracted chapter is logged and skipped in v1 (no pending-edges table).
- The paper chain is untouched. Shared-file edits are limited to `schema.py` additions and small refactors that *expose* existing functions (e.g., extraction accepting an extra context string) with no paper-path behavior change.

## Error handling

- **Scanned / no text layer** → `QuarantineError("needs-ocr")` via the existing `needs_ocr()` check.
- **No outline and heading detection fails** → `QuarantineError("no-structure")` with a log of what was tried. Corpus is born-digital, so rare; the v2 fallback would be synthetic fixed-size sections, not a redesign.
- **Chapter extraction/write failure** → that partition fails visibly in the Dagster UI and re-runs alone. All graph writes are `MERGE`-based on content-hash ids → idempotent re-runs.
- **Forward `DEPENDS_ON`** → logged and skipped.
- **A half-extracted book is a valid state**, not an error: structure + RAG for everything, theorem graph for extracted chapters. The Dagster partition view is the progress bar.

## Testing

- **Unit:** outline→Chapter/Section tree builder (fixture outlines incl. messy nesting); section-aware chunking with correct page attribution; `label` parsing ("Theorem 3.1", "Definition 2.14", unnumbered); `book_id` precedence (isbn > title).
- **Integration fixture:** a small LaTeX-built book PDF (~3 chapters, a few numbered theorems/definitions) checked into the repo; run the full chain against dev Neo4j; assert hierarchy edges, `Section-STATES`, labels, and pages with Cypher.
- **The Lévy-process test, literally:** seed a Concept via the paper path, extract a fixture book chapter mentioning the same concept, assert both sources point at the *same* Concept node — pins the shared-resolution guarantee as a regression test.
- **Paper-path regression:** existing tests untouched and passing.

## Out of scope (v1)

- OCR / scanned books
- The retrieval/query layer (still spec-only for papers too; this schema is designed to serve both when built — `spec/05-query-router.md`)
- Cross-source statement linking (book theorem ↔ paper theorem)
- External metadata enrichment (Open Library, etc.)
- A book analogue of `paper_analysis` summaries
- Figures and exercises
