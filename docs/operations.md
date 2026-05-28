# Operations runbook

This is the day-to-day reference for running the substrate. The architecture is in [docs/specs/2026-05-03-knowledge-pipeline-design.md](specs/2026-05-03-knowledge-pipeline-design.md); the implementation plan is in [docs/superpowers/plans/2026-05-03-knowledge-pipeline-substrate.md](superpowers/plans/2026-05-03-knowledge-pipeline-substrate.md).

## Daily

| | |
|---|---|
| Start the stack | `docker compose up -d` |
| Stop the stack | `docker compose down` |
| Stop and wipe state (Postgres + MinIO) | `docker compose down -v` (does **not** touch Aura) |
| Dagster UI | `http://localhost:3000` |
| MinIO console | `http://localhost:9001` (creds in `.env`) |
| Tail Dagster logs | `docker compose logs -f dagster-webserver dagster-daemon` |

## Adding a new paper

Drop the PDF into the `SOURCE_DIR` folder. The `daily_ingest_schedule` (06:00 Europe/London) will detect it automatically by SHA-256 hash, register a new dynamic partition, and run the full pipeline. To ingest immediately without waiting for the schedule, trigger a run manually from the Dagster UI (Assets → `raw_blob` → select the partition → "Materialize selected").

## Re-extracting a paper after a schema change

1. Bump the schema in `pipeline/schema.py`.
2. Apply: `uv run python scripts/init_neo4j.py`.
3. In the UI: select the affected asset (e.g. `extracted_graph`, `paper_analysis`) for the relevant partition(s) → "Materialize selected." Downstream assets will materialize automatically.

## Regenerating analysis with a new prompt

1. Edit `SYSTEM_PROMPT` in `pipeline/analysis.py`.
2. Restart Dagster.
3. UI → `paper_analysis` → select all partitions of interest → "Materialize selected."

## Backups

- Aura: console → "Backup" → "Create snapshot" before any schema change.
- MinIO: the host's MinIO data volume is at `<docker_root>/volumes/knowledge-retrieval_minio_data`. Back up with `tar` or sync to Backblaze B2 / iCloud.
- Vault snapshots are already in MinIO `vault-snapshots/`.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Dagster UI: "Failed to load workspace" | Import error in `pipeline.definitions` | `docker compose logs dagster-webserver` and fix the traceback |
| Asset run: `OpenAI rate limit` | Too many concurrent runs | Lower `max_concurrent_runs` in `docker/dagster.yaml` |
| Asset run: `Neo4j ServiceUnavailable` | Aura paused (free tier) | Resume in Aura console |
| `extracted_graph`: empty Concept set on every paper | Tutorial-style schema (too generic) — tune `NODE_TYPES` with descriptions | See spec §6 |
| Schedule fires but no new runs | No new PDFs found in `SOURCE_DIR`, or all hashes already registered | Check `SOURCE_DIR`; confirm PDFs are present |

## Out-of-scope reminders

These are explicit non-goals (spec §2 / §11):
- Skill ports (`/research`, `/researcher-*`, `/idea`, `/topic-expert`) — separate workstream.
- Production Dagster topology (k8s, Helm, separate user-code gRPC) — separate spec.
- Microsoft GraphRAG / HippoRAG / LightRAG retrieval patterns — separate spec(s).
- Decommissioning the legacy alethograph DB — only after both skill ports complete.

---

## Environment variables

All services read configuration exclusively from environment variables (set in `.env` or the shell):

| Variable | Purpose |
|---|---|
| `SOURCE_DIR` | Absolute path to the directory that the daily schedule scans for new PDFs |
| `RESOLVER_POSTGRES_DSN` | PostgreSQL DSN for the entity-resolver / pending-citations tables (e.g. `postgresql://user:pass@localhost:5432/knowledge`) |
| `NEO4J_NEW_URI` | Bolt/Neo4j URI for the active Aura instance (e.g. `neo4j+s://xxxxxxxx.databases.neo4j.io`) |
| `NEO4J_NEW_USERNAME` | Aura username (typically `neo4j`) |
| `NEO4J_NEW_PASSWORD` | Aura password |
| `NEO4J_NEW_DATABASE` | Aura database name (typically `neo4j`) |
| `MINIO_ENDPOINT` | MinIO S3-compatible endpoint (e.g. `http://localhost:9000`) |
| `MINIO_ACCESS_KEY` | MinIO access key |
| `MINIO_SECRET_KEY` | MinIO secret key |
| `OPENAI_API_KEY` | OpenAI API key (used for embeddings and extraction) |
| `ANTHROPIC_API_KEY` | Anthropic API key (used for `paper_analysis` asset) |

## One-time bootstrap

Run these steps once before the first production build (or after a full graph wipe):

1. **Aura snapshot** — in the Neo4j Aura console, select the `6b371650` instance → "Backup" → "Create snapshot". Keep this snapshot until the first successful production build is verified.

2. **Wipe and re-initialise the graph**:
   ```bash
   uv run python scripts/reset_graph.py --yes   # batched DETACH DELETE + schema constraint re-init
   ```
   After the wipe, confirm the DB is empty (zero nodes, zero relationships) before proceeding.

3. **Schema init** (if the script exists):
   ```bash
   uv run python scripts/init_neo4j.py          # idempotent — safe to run even if schema already exists
   ```

4. **Postgres init** (pgvector extension + resolver / pending-citations tables):
   ```bash
   uv run python scripts/init_postgres.py
   ```

5. **Start the local stack** (MinIO + Postgres):
   ```bash
   docker compose up -d
   ```
   The `minio-init` service runs once and creates all required buckets: `raw`, `parsed`, `chunks`, `triage`, `extracted`, `analysis`, `pdfs`, `legacy-summaries`, `vault-snapshots`.

## Daily schedule — `daily_ingest_schedule`

- **Cron**: `0 6 * * *` (06:00 Europe/London)
- **Job**: `ingest_document` — the full 8-asset pipeline (raw → parse → triage → chunk → extract → resolve → write → analyse)
- **Behaviour**: on each tick the schedule scans `SOURCE_DIR` for PDF files, computes the SHA-256 of each file, and registers a new Dagster dynamic partition (keyed by the SHA-256 hash) for any PDF not previously seen. A `RunRequest` is emitted per new partition so Dagster materialises the full pipeline for that document.

The schedule is registered in `pipeline/definitions.py` and is enabled by default when the Dagster daemon is running.

## Tests

**Unit suite** (no live services required):
```bash
uv run --extra dev pytest -q
```

**Integration suite** (requires live Aura, MinIO, OpenAI, Anthropic, and Postgres; fixture PDFs must be available under `SOURCE_DIR`):

1. Set the environment variables `INTEGRATION_FIXTURE_HASH`, `INTEGRATION_FIXTURE_B_HASH`, and `INTEGRATION_FIXTURE_A_HASH` to the SHA-256 hashes of your fixture PDFs.
2. Ensure all environment variables above are set and all services are reachable.
3. Run:
   ```bash
   uv run --extra dev pytest --run-integration
   ```

## Phase-0 pre-build gates (human-run)

These checks must be completed manually before the first production build is triggered:

- **Docling LaTeX-fidelity spot test**: run Docling on a representative scanned PDF (exercising the VLM/OCR path) and a LaTeX-heavy PDF; verify the output preserves equations and delimiters correctly.
- **Extraction-model evaluation**: sample 5–10 representative abstracts and confirm the extraction prompt produces well-formed `ExtractedGraph` JSON with no hallucinated node types.
- **Post-wipe confirmation**: after running `reset_graph.py --yes`, query Aura directly (`MATCH (n) RETURN count(n)`) and confirm zero nodes before starting any pipeline runs.
