# knowledge-retrieval

Dagster + MinIO + Neo4j + Postgres/pgvector pipeline that ingests **raw paper PDFs from a folder** on a daily schedule and **builds a knowledge graph from scratch** — parsing, chunking, embedding, extracting typed entities, resolving duplicates, and writing a research-grade analysis per paper.

> **Builder, not enricher.** This pipeline does **not** read an Obsidian vault and does **not** mirror a pre-existing curated graph. It starts from documents the graph has never seen and constructs the graph itself. The resulting graph mimics the *shape* of the alethograph graph, but every node is born from the documents. (The earlier vault-enrichment pipeline — legacy mirror, `SimpleKGPipeline`, static partitions — has been removed.)

## What it does

Point `SOURCE_DIR` at a folder of paper PDFs. A daily Dagster schedule discovers new files, and for each one:

1. **Stores** the PDF in MinIO, keyed by its SHA-256 (the document's identity).
2. **Parses** it with [Docling](https://github.com/docling-project/docling) (self-hosted) to markdown + LaTeX, routing scanned/image PDFs through the Granite-Docling VLM path; degenerate parses are **quarantined**, not silently skipped.
3. **Triages & enriches** — establishes paper identity (DOI > arXiv > normalized title), then pulls abstract, TLDR, citation counts, author IDs, and references from **Semantic Scholar**, writing `Paper` + `Author` nodes. Non-papers and duplicate-paper-different-bytes are quarantined.
4. **Chunks** equation-aware (never splitting a LaTeX block) and **embeds** chunks (OpenAI `text-embedding-3-small`).
5. **Extracts** typed `Concept`s (`#concept`/`#method`), `Definition`s, and `Result`s (theorem/lemma/…) against the alethograph schema.
6. **Resolves** each concept against existing canonicals via pgvector NN search (conservative, split-when-unsure), recording every decision to Postgres for a future human-review loop.
7. **Writes** the derived graph (`graph_write` is the sole writer): `Chunk`/`Concept`/`Definition`/`Result` nodes, `Paper→Concept` `DISCUSSES`/`DERIVED_FROM`, and `CITES` edges (forward + backward backfill via a `pending_citations` table).
8. **Analyses** the paper with Claude into a structured, research-skill-shaped JSON (`Summary` node + canonical LaTeX-bearing artifact).

Everything is keyed by content hash, so re-running a document converges (idempotent `MERGE`s), never duplicates.

## Architecture

| Component | Role | Where it runs |
|---|---|---|
| **Dagster webserver + daemon** | Orchestration, dynamic partitions, daily schedule, run history | Docker (`localhost:3000`) |
| **Postgres** | Dagster metadata **+** pgvector entity-resolution store (`entity_embeddings`, `resolution_decisions`, `alias_map`, `pending_citations`) | Docker (`127.0.0.1:5432`) |
| **MinIO** | S3-compatible blob storage: `raw` PDFs, `parsed` markdown, and `chunks`/`extracted`/`analysis`/`triage` artifacts | Docker (`127.0.0.1:9000/9001`) |
| **Neo4j (Aura)** | The knowledge graph — Papers, Authors, Concepts, Topics, Definitions, Results, Summaries, Chunks (+ native vector index) | Aura cloud |
| **Docling** | Self-hosted math-aware PDF parser (text + Granite-Docling VLM paths) | in-process |
| **OpenAI** | Entity extraction + embeddings (`text-embedding-3-small`) | API |
| **Anthropic** | Per-paper structured analysis (`claude-sonnet-4-6`) | API |
| **Semantic Scholar** | Bibliographic metadata + citation graph | API |

## Pipeline (Dagster asset DAG)

Per-document partition, keyed by file SHA-256. Writes are serialized (`max_concurrent_runs = 1`).

```
raw_blob ─▶ parsed_document ─┬─▶ triage_metadata (+Semantic Scholar) ─┐
 PDF→MinIO   Docling          │   Paper/Author identity, refs stash    │
 by hash     text/VLM         │                                        │
                              ├─▶ chunks ─────────────────────────────┤
                              │   equation-aware split + embed         │
                              │   (artifact only)                      ├─▶ graph_write
                              ├─▶ extracted_graph ─▶ resolved_entities ┘   SOLE writer:
                              │   typed concepts/    pgvector decide-only   Chunk/Concept(+pgvector)/
                              │   definitions/results (decision trail)      Definition/Result/CITES
                              └─▶ paper_analysis  (Claude → Summary node + canonical JSON; parallel)
```

A daily schedule (`daily_ingest_schedule`, 06:00 Europe/London) scans `SOURCE_DIR`, registers a dynamic partition per new PDF, and requests the `ingest_document` job. Manual materialization works too.

## Schema

Reuses the alethograph node/relationship schema (`Paper`, `Book`, `Author`, `Concept`, `Topic`, `Researcher`, `Idea`) and extends it with **`Definition`**, **`Result`** (a single node with a `kind` ∈ {theorem, lemma, proposition, corollary}), and **`Summary`** nodes, plus `STATES` / `DEFINES` / `USES` / `DEPENDS_ON` / `HAS_SUMMARY` relationships. Definitions and Results are paper-local with content-hash ids (never cross-merged). See [`pipeline/schema.py`](pipeline/schema.py).

## Quickstart

```bash
# 1. config
cp .env.example .env          # fill in NEO4J_NEW_*, OPENAI_API_KEY, ANTHROPIC_API_KEY,
                              # SOURCE_DIR (folder of PDFs), RESOLVER_POSTGRES_DSN
uv sync                       # install python deps (uv)

# 2. infra
docker compose up -d          # postgres + minio (+ buckets) + dagster web/daemon

# 3. one-time bootstrap of the graph + resolver store
#    (take an Aura snapshot of the target DB first — reset_graph wipes it)
uv run python scripts/reset_graph.py --yes   # batched wipe + re-assert schema/constraints/indexes
uv run python scripts/init_postgres.py       # pgvector extension + resolver/pending_citations tables

# 4. ingest
open http://localhost:3000    # let the daily schedule run, or materialize `ingest_document`
```

`scripts/init_neo4j.py` applies the schema constraints/indexes without wiping (use it instead of `reset_graph.py` on a graph you don't want to clear). Full runbook: [docs/operations.md](docs/operations.md).

> **Pre-build gates (run once, by hand):** before the first production build — (a) spot-check Docling's LaTeX fidelity on a few equation-dense pages incl. a scanned/VLM one; (b) evaluate the extraction model on a handful of papers; (c) confirm the target Aura DB is empty post-wipe with a snapshot retained. See [docs/operations.md](docs/operations.md).

## Querying the graph

The resulting graph supports vector search (Neo4j native chunk index), Cypher graph traversal, and graph-aware RAG. [notebooks/smoke_test.ipynb](notebooks/smoke_test.ipynb) demonstrates retrieval patterns from pure vector search up to multi-hop subgraph traversal with hybrid graph×vector ranking. *(The notebook predates the builder rewrite; the retrieval patterns still apply, but some queries may reference the old structural overlay and need light updating for the from-scratch schema.)*

## Repository layout

```
pipeline/                Dagster code location
  assets/
    raw_blob.py          PDF → MinIO, keyed by content hash (= Document identity)
    parsed_document.py   Docling parse (text/VLM) → markdown+LaTeX; quarantine on empty
    triage_metadata.py   Paper identity + Semantic Scholar enrich; write Paper/Author; stash refs
    chunks.py            equation-aware split + embed → MinIO artifact (no graph write)
    extracted_graph.py   LLM extraction → typed concepts/definitions/results artifact
    resolved_entities.py pgvector NN resolution; decide-only, decision trail in Postgres
    graph_write.py       SOLE writer of the derived graph + CITES backfill
    paper_analysis.py    Claude structured analysis → Summary node + canonical JSON
  parsing.py             Docling wrapper + text/VLM mode routing
  chunking.py            equation-aware markdown chunker
  embedding.py           OpenAI embedding helper
  extraction.py          extraction prompts, parsing, dedup
  resolver.py            decide()/pgvector NN/decision + alias-map + embedding-dim guard
  research_port.py       vendored Semantic Scholar + Cypher logic (NOT a runtime dep on alethograph)
  analysis.py            standing analysis brief + analysis JSON contract
  text_norm.py           shared statement normalization (ids/dedup keys)
  schema.py              node/relationship schema + INIT_CYPHER
  resources.py           Neo4j / MinIO / OpenAI / Anthropic / Postgres resources
  partitions.py          dynamic content-hash partitions
  source.py              folder discovery (SOURCE_DIR); cloud-source-ready contract
  schedules.py           daily_ingest_schedule
  jobs.py / definitions.py / storage.py / cypher.py

scripts/
  reset_graph.py         snapshot-aware batched wipe + schema re-init
  init_neo4j.py          apply schema constraints + vector index (no wipe)
  init_postgres.py       pgvector extension + resolver/pending_citations tables

docker/                  Dagster Dockerfile + workspace.yaml + dagster.yaml
docs/                    operations.md + design specs + implementation plans
notebooks/smoke_test.ipynb
tests/                   pytest suite (unit; integration gated behind --run-integration)
```

## Relationship to alethograph & the research skill

The graph's shape and the per-paper analysis template mirror the [alethograph](https://github.com/odshelley/alethograph) `research` skill (the quality bar), so analyses stay interchangeable with what the skill produces and the explorer site can render them. The skill's proven Semantic Scholar + graph-write logic is **vendored** into `pipeline/research_port.py` (CLI/connection stripped); `research_tools.py` is *not* imported at runtime. The skill's *interactive/agentic* steps (per-paper learning goals, topic-DAG inference, researcher links, idea seeds, human-review dedup) are intentionally **deferred** — this pipeline is unattended batch.

## Design grounding

Retrieval and graph-construction patterns are informed by:

- **Edge et al. 2024** — *From Local to Global: A Graph RAG Approach to Query-Focused Summarization* (Microsoft GraphRAG)
- **Han et al. 2024** — *Retrieval-Augmented Generation with Graphs* (survey)
- **Guo et al. 2024** — *LightRAG: Simple and Fast Retrieval-Augmented Generation*
- **Gutiérrez et al. 2024** — *HippoRAG*
- **Xiang et al. 2025** — *When to use Graphs in RAG / GraphRAG-Bench*
- **Zeng et al. 2025** — *Empowering GraphRAG with Knowledge Filtering and Integration*

Design spec + implementation plan: [docs/superpowers/specs/2026-05-27-document-graph-builder-design.md](docs/superpowers/specs/2026-05-27-document-graph-builder-design.md) and [docs/superpowers/plans/2026-05-27-document-graph-builder.md](docs/superpowers/plans/2026-05-27-document-graph-builder.md). Earlier per-aspect notes are in [spec/](spec/).

## Tests

```bash
uv run --extra dev pytest                 # unit suite (fast, no live services)
uv run --extra dev pytest --run-integration  # end-to-end; needs live Aura/MinIO/OpenAI/Anthropic/Postgres + fixture PDFs
```

## Status

v1 builder: papers only, ingested from a local folder. **Deferred by design:** books, cloud/online sources, topic-DAG inference, researcher auto-linking, idea seeds, the human-review UI for flagged concept merges, and the alethograph-explorer web adapter (the `alias_map` and decision-trail seams exist for the review loop).

## Configuration

`.env` (not committed) carries `SOURCE_DIR`, the `NEO4J_NEW_*` Aura creds, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `MINIO_*`, `POSTGRES_*`, `RESOLVER_POSTGRES_DSN`, and `DAGSTER_HOME`. The legacy/portmanteau Aura DB is **not used** by this pipeline. See [docs/operations.md](docs/operations.md) for the full list and the dev-only credential note.
