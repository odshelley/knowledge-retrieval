# Knowledge Pipeline — Design

**Date:** 2026-05-03
**Status:** Approved (brainstorm), not yet planned

## 1. Goal

Build a local prototype of a knowledge-retrieval pipeline that mirrors the architecture I expect to defend at work for an enterprise quant wiki. The exercise is about the **orchestration and substrate**, not retrieval research. The retrieval layer starts boring (vector + graph traversal via `neo4j_graphrag`) and stays boring until something specific demands more.

The corpus is my existing research material: 141 academic PDFs and 1080 LLM-generated markdown summaries currently in an iCloud Obsidian vault.

## 2. Non-goals

- Migrating off the iCloud vault. The vault is read-only input.
- Web UI / explorer. Query from notebooks via `neo4j_graphrag` retrievers.
- Microsoft GraphRAG community summaries, HippoRAG PPR, LightRAG dual-level retrieval. All deferred. The substrate must come first.
- Authoring surface. No CLI / API for editing notes — markdown summaries become a *generated* artifact, not a hand-edited one.
- **Porting the alethograph skills (`/research`, `/researcher-*`, `/idea`, `/topic-expert`, `/study`, `/review`) to use the new substrate.** This is a follow-up workstream — see §11. The substrate is built so that port is feasible later; this spec stops at the substrate boundary.

## 3. Source-of-truth model

| Tier | Content | Store | Authority |
|---|---|---|---|
| Canonical | PDFs (papers + textbook PDFs) | MinIO, content-hashed | Source of truth |
| T1 derived | Chunks, embeddings, structural extraction (sections, refs), entity graph | Neo4j (new database) | Deterministic; cheap to recompute |
| T2 derived | LLM-generated paper summaries (the v2 replacement for v1 markdown) | Neo4j (`Summary` nodes attached to `Paper`) | Expensive; lineage tracked; selectively re-materialized |

The existing 1080 v1 markdown summaries are ingested as supplementary `Document` nodes tagged `source: legacy_summary` so the LLM extraction can pull additional entities and the wikilink structure becomes free graph signal — but they are not authoritative.

## 4. Stack

Four services. The Docker Compose set is intentionally chosen to mirror the enterprise pattern with no service that exists "just for the prototype":

| Service | Role | Enterprise analogue |
|---|---|---|
| **MinIO** (Docker) | Canonical PDF blob store, content-hashed; vault snapshots | S3 |
| **Postgres** (Docker) | Dagster metadata (run history, asset materializations, sensors) | Postgres / RDS |
| **Dagster** (Docker) | Pipeline orchestration | Dagster / Airflow |
| **Neo4j Aura** (cloud) | Chunks + embeddings (native vector index) + entity graph + structural metadata. **A new, separate database** from the existing alethograph one. | Neo4j Enterprise |

No Qdrant, no Weaviate — Neo4j's native HNSW vector indexes handle dense retrieval, which is what `neo4j_graphrag` defaults to. Adding a separate vector store buys nothing for this corpus size.

## 5. Safety / non-destructive guarantees

The migration is non-destructive by construction. There is nothing to "roll back" because nothing existing is mutated:

1. **Existing Aura DB is untouched.** New pipeline writes to a new database; the alethograph skills keep running unchanged against the existing one.
2. **iCloud vault is read-only to the new pipeline.** PDFs and v1 markdown are *copied* into MinIO at ingest time; the vault filesystem is never written.
3. **Frozen vault snapshot.** First pipeline run tars the entire vault (1080 .md + 141 PDFs) to `vault-snapshots/2026-05-03-initial.tar.gz` in MinIO. MinIO bucket replicates to one external location (Backblaze B2 or iCloud — operator choice).
4. **Aura on-demand backup** triggered manually before first run.

If the new system goes wrong: delete the new Aura database, `docker compose down -v`, done.

## 6. Schema

`SimpleKGPipeline` writes `Document`, `Chunk`, and entity nodes/edges according to the schema below. Native vector index on `Chunk.embedding` for retrieval.

```python
NODE_TYPES = [
    "Paper",       # title, doi, arxiv_id, year, abstract
    "Author",
    "Concept",     # mathematical concept (e.g. "Lévy process", "BSDE")
    "Method",      # algorithmic / numerical (e.g. "Deep BSDE solver", "PNSGD")
    "Theorem",
    "Definition",
    "Topic",       # broad domain area (e.g. "XVA", "machine unlearning")
]

RELATIONSHIP_TYPES = [
    "AUTHORED_BY",   # Paper -> Author
    "CITES",         # Paper -> Paper
    "INTRODUCES",    # Paper -> Concept | Method | Theorem | Definition
    "USES",          # Paper -> Method | Concept
    "BUILDS_ON",     # Method -> Method, Concept -> Concept
    "IN_TOPIC",      # Paper -> Topic
]

PATTERNS = [
    ("Paper", "AUTHORED_BY", "Author"),
    ("Paper", "CITES", "Paper"),
    ("Paper", "INTRODUCES", "Concept"),
    ("Paper", "INTRODUCES", "Method"),
    ("Paper", "INTRODUCES", "Theorem"),
    ("Paper", "INTRODUCES", "Definition"),
    ("Paper", "USES", "Method"),
    ("Paper", "USES", "Concept"),
    ("Method", "BUILDS_ON", "Method"),
    ("Concept", "BUILDS_ON", "Concept"),
    ("Paper", "IN_TOPIC", "Topic"),
]
```

`SimpleKGPipeline` also creates the structural `Document` and `Chunk` nodes and `HAS_CHUNK` / `FROM_DOCUMENT` edges automatically.

`Topic` nodes are **not** created by LLM extraction. They are seeded by `structural_overlay` from the canonical topic DAG in the existing alethograph database, and `IN_TOPIC` edges are added by the same overlay step. The LLM extraction's job is everything else (Concept, Method, Theorem, Definition, plus the relations between them).

The schema is intentionally narrow. New types are added only when a concrete retrieval question is being blocked.

## 7. Pipeline (Dagster assets)

```
pdf_blob ──┐
           ├─► kg_extracted ──► structural_overlay ──► paper_summary
v1_md_blob ┘
```

| Asset | Computation | Inputs | Output |
|---|---|---|---|
| `pdf_blob` | Sensor on MinIO `pdfs/` bucket; content-hashed; one partition per PDF | MinIO event | Materialization metadata (hash, size) |
| `v1_md_blob` | Same shape, on `legacy-summaries/` bucket; one partition per .md | MinIO event | Same |
| `kg_extracted` | `SimpleKGPipeline.run_async(file_path)` per PDF + corresponding v1 .md if present, using the schema in §6 | `pdf_blob`, `v1_md_blob` | Neo4j `Document`, `Chunk`, entities, relations |
| `structural_overlay` | Cypher to add `Paper {doi, arxiv_id, year}`, `IN_TOPIC` edges from existing alethograph topic DAG, `AUTHORED_BY` edges | `kg_extracted` + topic-DAG export from existing Aura DB | Updated `Paper`/`Author`/`Topic` nodes |
| `paper_summary` | Prompt LLM with chunks → write `Summary` node, attach `(Paper)-[:HAS_SUMMARY]->(Summary)` | `kg_extracted`, `structural_overlay` | `Summary` nodes (the v2 replacement for the v1 markdown) |

Re-materialization rules:
- `pdf_blob` re-materializes when content hash changes (i.e. never, unless replaced).
- `kg_extracted` re-materializes when `pdf_blob` changes *or* schema version bumps.
- `paper_summary` re-materializes when its upstream changes *or* on manual trigger (e.g. "regenerate summaries for all XVA-topic papers with the new prompt").

This gives selective regeneration without manual bookkeeping — Dagster's lineage handles it.

## 8. Initial bulk ingest

One-time job:
1. Manual Aura backup of existing DB.
2. `docker compose up`. Postgres, MinIO, Dagster start clean.
3. `scripts/snapshot_vault.py` — tars iCloud vault to `vault-snapshots/2026-05-03-initial.tar.gz` in MinIO; uploads each PDF to `pdfs/` and each .md to `legacy-summaries/`.
4. Trigger Dagster job `bulk_reingest`. All 141 PDFs + matching .md flow through the pipeline.
5. Smoke-test retrieval against the new DB from a notebook using `neo4j_graphrag.VectorRetriever` and `VectorCypherRetriever`.

Estimated extraction cost: $50–200 depending on LLM choice. `gpt-5-nano` is cheapest; `claude-haiku-4-5` is comparable. Choose at implementation time based on extraction quality on a 5-paper sample.

## 9. Model choices (defaults; pluggable)

| Component | Default | Rationale |
|---|---|---|
| Extraction LLM | `gpt-5-nano` (matches the `neo4j_graphrag` tutorial) — re-evaluate against `claude-haiku-4-5` on a 5-paper sample | Cheap; schema does the heavy lifting |
| Embeddings | OpenAI `text-embedding-3-small` (1536 dims) | `neo4j_graphrag` default; cheap; one-line swap to a local model later if work demands data-residency |
| Chunking | `FixedSizeSplitter(chunk_size=500, chunk_overlap=100)` from `neo4j_graphrag` | Tutorial default. Revisit if retrieval quality is poor on equation-heavy text. |

All three are intended to be replaced when work imposes constraints (private inference, on-prem embeddings, smarter chunking for academic PDFs). The pipeline isolates each behind a single config knob.

## 10. Open questions (defer to plan)

- Exact embedding model and chunk size — pin after a sample run.
- How `structural_overlay` fetches DOI/arxiv-ID metadata: from the existing alethograph DB? from a Crossref/arxiv API enrichment step? Both?
- Which LLM gets the `paper_summary` job — the same one as extraction, or a stronger model (Claude Sonnet 4.6 / Opus 4.7) for higher-quality summaries?
- Whether `paper_summary` outputs structured sections (motivation, method, results, limitations) or free-form prose. Suggest structured.
- **PDF ↔ v1 .md mapping rule.** The vault has 141 PDFs but 1080 .md files (concept notes, person notes, daily notes, idea notes, paper notes). Only paper notes correspond to PDFs. The mapping has to come from somewhere — frontmatter on the .md (`paper: <pdf-filename>`)? The existing alethograph `Paper.note_path` field in the current Aura DB? Filename heuristic? Pin during planning.
- **Order of post-substrate work.** Read-side skill port first (high value, low complexity once substrate exists), then write-side skill port. Each likely deserves its own spec. Sequencing depends on how much daily friction the unported skills cause once the substrate is up.

## 11. Constraints from the anticipated skill port

The skills will be ported to use this substrate as a follow-up workstream (likely two specs: a read-side port and a write-side port). The substrate must therefore be built such that port is feasible. Concrete constraints on this spec:

- **Stable write-trigger interface.** The pipeline exposes a single canonical way to submit new content: drop the PDF in MinIO `pdfs/` (and optionally a companion .md in `legacy-summaries/`); a Dagster sensor picks it up. Write-side skills (`/research`, `/study`) eventually invoke this path instead of writing directly to a database. No alternative ingest paths.
- **Schema chosen for retrieval.** The typed entities in §6 (`Concept`, `Method`, `Theorem`, `Definition`) and the `INTRODUCES` / `USES` / `BUILDS_ON` relations exist *because* the read-side port will want to filter on them and traverse them. Schema decisions made for storage convenience would be wrong; schema decisions are made for the queries we expect skills to issue.
- **`Summary` nodes are drop-in replacements for v1 markdown.** Read-side skills today `Read` a `.md` file at `Paper.note_path`. Post-port they `MATCH (p:Paper)-[:HAS_SUMMARY]->(s:Summary)` and read `s.text`. The data shape mirrors the file shape so port is mechanical.

### Read-side port (sketch — not part of this spec)

| Skill | After port |
|---|---|
| `/researcher-*` | `VectorCypherRetriever` scoped by topic/paper, returning chunks with citations |
| `/topic-expert` | Topic DAG walk → chunk retrieval scoped to subtree → evidence-grounded answer |
| `/idea` | Graph traversal over the new DB's typed entity graph; every connection has chunk-level evidence |

### Write-side port (sketch — not part of this spec, harder)

| Skill | After port |
|---|---|
| `/research` | Download PDF → upload to MinIO → Dagster sensor → pipeline does extraction |
| `/study` | Upload textbook → trigger pipeline |

### What this spec still does *not* commit to

- Any retrieval pattern beyond what `neo4j_graphrag` ships out of the box (`VectorRetriever`, `VectorCypherRetriever`, `HybridRetriever`, `Text2CypherRetriever`).
- Any specific Dagster deployment topology (local dev only; production deployment is a separate spec).
- The schedule or scope of the read-side and write-side port specs (see §10).
- Long-term retirement of the existing alethograph DB. It continues to feed `structural_overlay` (Topic DAG, DOI/arxiv metadata) for the foreseeable future. Folding it into the new DB and decommissioning is a separate decision after both ports complete.
