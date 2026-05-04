# knowledge-retrieval

Dagster + MinIO + Neo4j pipeline that turns a personal Obsidian academic-research vault into a queryable knowledge graph with vector retrieval, structural traversal, and graph-aware RAG over the resulting graph.

## What it does

Given a curated Obsidian vault of papers, books, concepts, topics, ideas, and researchers (from the [alethograph](https://github.com/odshelley/alethograph) plugin), this repo builds a fresh Neo4j graph that combines:

1. **Curated structure mirrored from a legacy Neo4j (Aura) DB** — Papers, Books, Authors, Concepts, Topics, Researchers, Ideas, and all 27 relationship patterns between them, copied 1:1 (verbatim labels and directions).
2. **LLM-extracted chunks + entities** — for each paper/book PDF, [`SimpleKGPipeline`](https://neo4j.com/docs/neo4j-graphrag-python/) (via `neo4j-graphrag-python`) extracts text chunks, computes embeddings, and pulls entities/relationships out of the chunk content.
3. **Per-paper Claude summaries** — six-field structured summary per paper (motivation / contributions / method / key results / limitations / related work), generated with `claude-sonnet-4-6`.

The result: a single Neo4j database that supports vector search, multi-hop graph traversal, and combined "GraphRAG" retrieval against a personal academic library.

## Architecture

| Component | Role | Where it runs |
|---|---|---|
| **Dagster webserver + daemon** | Pipeline orchestration, asset graph, run history | Docker (`localhost:3000`) |
| **Postgres** | Dagster metadata store | Docker |
| **MinIO** | S3-compatible blob storage for PDFs and v1 markdown summaries | Docker (`localhost:9001`) |
| **Neo4j (new)** | Active knowledge graph — chunks, embeddings, entities, summaries | Aura cloud |
| **Neo4j (legacy)** | Source-of-truth curated graph; mirrored once at index time | Aura cloud |
| **OpenAI** | Entity extraction (`gpt-5-nano`) + embeddings (`text-embedding-3-small`) | API |
| **Anthropic** | Per-paper structured summaries (`claude-sonnet-4-6`) | API |

Architecture diagram: [docs/architecture.png](docs/architecture.png) (source: [docs/architecture.dot](docs/architecture.dot)).

## Pipeline (Dagster assets)

```
                ┌───────────────────┐
                │ legacy_graph_mirror│   non-partitioned, runs once per re-sync
                │  (mirrors curated │
                │   structural KG)  │
                └────────┬──────────┘
                         │
   per partition (113 papers + 7 books):
                         ▼
   ┌──────────┐   ┌────────────┐   ┌──────────────┐   ┌───────────────────┐   ┌────────────────┐
   │ pdf_blob │ + │ v1_md_blob │ → │ kg_extracted │ → │ structural_overlay│ → │ paper_summary  │
   └──────────┘   └────────────┘   └──────────────┘   └───────────────────┘   └────────────────┘
   MinIO HEAD     MinIO HEAD       SimpleKGPipeline:    legacy connections      Claude:
   on PDF         on v1 .md        chunks, embeddings,  for this partition      structured 6-field
                                   entity extraction    (AUTHORED, CITES,       summary
                                                         HAS_TOPIC)
```

Two jobs are exposed:

- **`bulk_reingest`** — runs everything across all partitions. The mirror seeds the structural backbone once; per-partition assets layer chunks/embeddings/summaries on top.
- **`legacy_mirror_job`** — just the mirror, idempotent. Re-run any time the legacy Aura DB changes.

## Quickstart

```bash
# 1. one-time setup
cp .env.example .env                           # fill in Aura creds + OpenAI/Anthropic keys
docker compose up -d                           # postgres + minio + dagster
uv sync                                         # install python deps
uv run python scripts/init_neo4j.py            # apply schema constraints to new Aura DB

# 2. discover partitions from legacy DB + Obsidian vault
uv run python scripts/discover_partitions.py   # writes data/partitions.json
uv run python scripts/snapshot_vault.py        # uploads PDFs + v1 mds to MinIO
docker compose restart dagster-webserver dagster-daemon  # pick up new partitions

# 3. open Dagster UI and materialize bulk_reingest
open http://localhost:3000
```

Day-to-day operations: see [docs/operations.md](docs/operations.md).

## Querying the graph

[notebooks/smoke_test.ipynb](notebooks/smoke_test.ipynb) walks through six retrieval patterns of increasing sophistication:

| Pattern | What it does | LLM? | Graph traversal? |
|---|---|---|---|
| 1 | Pure vector retrieval (top-K chunks by cosine sim) | no | no |
| 2 | Vector + Cypher topic filter | no | 1 hop |
| 3 | Pure Cypher graph traversal (e.g. "concepts derived from N papers") | no | yes |
| 4 | Plain GraphRAG (vector retriever + LLM synthesis) | yes | no |
| 5 | Graph-aware RAG — Han et al. 2024 / LightRAG-style: vector seed + Cypher 1-hop expansion → structural metadata in prompt | yes | 1 hop |
| 6 | **Real subgraph traversal** — entity extraction → seed locating → 3-hop walk through the curated graph → hybrid graph × vector chunk ranking → LLM synthesis with traversal evidence | yes | up to 3 hops |

Pattern 6 is the most useful for cross-topic / multi-hop questions ("what concepts connect topic X with topic Y?") that vector RAG can't answer at all.

## Repository layout

```
pipeline/                Dagster code location (assets, resources, jobs, schema, sensors)
  assets/
    legacy_mirror.py     non-partitioned mirror of the curated graph
    pdf_blob.py          per-paper PDF presence in MinIO
    v1_md_blob.py        per-paper v1 summary presence in MinIO
    kg_extracted.py      SimpleKGPipeline runs over PDF + v1 md
    structural_overlay.py per-partition copy of legacy connections (Author/Topic/Cites)
    paper_summary.py     Claude-generated structured summary
  schema.py              Node/relationship schema (1:1 with legacy DB)
  resources.py           Neo4j / MinIO / OpenAI / Anthropic resources
  jobs.py                bulk_reingest, legacy_mirror_job
  partitions.py          static partition def from data/partitions.json
  sensors.py             MinIO key-arrival sensor

scripts/
  init_neo4j.py          apply schema constraints + vector index
  discover_partitions.py legacy DB → vault PDF resolver → data/partitions.json
  snapshot_vault.py      upload PDFs + v1 mds + tarball to MinIO

docker/                  Dagster Dockerfile + workspace.yaml + dagster.yaml
docs/                    Architecture + operations + design specs + plans
spec/                    Earlier (per-aspect) design specs — schema, ingestion,
                         extraction prompts, alias resolution, query router, etc.
notebooks/smoke_test.ipynb
data/partitions.json     Source of truth for Dagster partitions (committed)
tests/                   pytest suite (unit + integration)
```

## Design grounding

The retrieval and graph-construction patterns are informed by the following papers:

- **Edge et al. 2024** — *From Local to Global: A Graph RAG Approach to Query-Focused Summarization* (Microsoft GraphRAG)
- **Han et al. 2024** — *Retrieval-Augmented Generation with Graphs* (the survey)
- **Guo et al. 2024** — *LightRAG: Simple and Fast Retrieval-Augmented Generation*
- **Gutiérrez et al. 2024** — *HippoRAG: Neurobiologically Inspired Long-Term Memory for LLMs*
- **Xiang et al. 2025** — *When to use Graphs in RAG / GraphRAG-Bench*
- **Zeng et al. 2025** — *Empowering GraphRAG with Knowledge Filtering and Integration*

Per-aspect design notes — schema, extraction prompts, alias resolution, query router, filtering/fallback, update semantics, MVP plan — are in [spec/](spec/). The current implementation is the substrate described in [docs/specs/2026-05-03-knowledge-pipeline-design.md](docs/specs/2026-05-03-knowledge-pipeline-design.md).

## Status

- **120 partitions** loaded (113 papers + 7 books). 1 book has a missing PDF in the vault and is parked as unresolved.
- **Legacy mirror**: 937 nodes + 1,503 relationships copied from legacy Aura → new Aura, idempotent.
- **`bulk_reingest`**: ~95% of partitions materialise cleanly end-to-end. Failures are mostly oversized books timing out on `kg_extracted`.

## Configuration

`.env` (not committed) carries credentials for both Aura DBs, OpenAI, Anthropic, MinIO, and the path to your Obsidian vault. See [docs/operations.md](docs/operations.md#configuration) for the full variable list.
