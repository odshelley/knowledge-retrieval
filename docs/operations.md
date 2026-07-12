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

## Security note — default credentials and network binding

The default MinIO credentials (`minioadmin`/`minioadmin`) and Postgres credentials (`dagster`/`dagster`) are for local development only. **Change them before any network-accessible deployment** by setting `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` in your `.env` file.

By default the `docker-compose.yml` binds the MinIO (ports 9000, 9001) and Postgres (port 5432) services to `127.0.0.1` only, so they are not reachable from outside the host. Do not change this to `0.0.0.0` without first rotating the credentials and applying appropriate firewall rules.

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

## kg MCP server

A stateless, read-only MCP query server over the knowledge-retrieval graph, in
`server/`. Deployed separately from the Dagster substrate above — it does not
share a container image and does not install `dagster`/`docling`.

### Environment variables

Configured entirely via env vars — see the `# kg MCP server (server/) —
read-only graph access` block in `.env.example` for the full set with
descriptions:

| Variable | Purpose |
|---|---|
| `KG_NEO4J_URI` | Bolt URI for the graph the server reads from |
| `KG_NEO4J_USER` | Neo4j user — should be a dedicated read-only user (see note below) |
| `KG_NEO4J_PASSWORD` | Password for that user |
| `KG_NEO4J_DATABASE` | Database name (typically `neo4j`) |
| `OPENAI_API_KEY` | Used server-side to embed incoming queries for `search_chunks`/`search_papers` |
| `KG_TOKENS` | `name:salt:sha256hex,...` — bearer-token allowlist, built by `scripts/issue_token.py` |
| `KG_EMBED_MODEL` | Query-embedding model (default `text-embedding-3-small`) |
| `KG_RATE_LIMIT` | Requests/minute per token (default `60`) |

### Token issuance

Mint a token for a new colleague/consumer:

```bash
uv run python scripts/issue_token.py <name>   # name: lowercase alphanumeric/underscore
```

This prints two lines:
- the **token** — give it to the colleague once, over a private channel (Slack DM,
  Signal, etc.), never in a shared channel or a commit. It is not stored anywhere
  server-side; if lost, the colleague needs a new token minted under a new name.
- the **`KG_TOKENS` entry** (`name:salt:hash`) — append it (comma-separated) to the
  server's `KG_TOKENS` secret:
  ```bash
  fly secrets set -a kg-graph KG_TOKENS="<existing-entries>,<new-entry>"
  ```
  This restarts the machine to pick up the new token; existing tokens keep working.

### Deploy

One-time setup (see [Fly CLI setup](#fly-cli-setup) below), then:

```bash
fly deploy -c docker/fly.toml
```

Deploys `docker/Dockerfile.server` per `docker/fly.toml`. The Fly health check
(`GET /healthz` every 30s) gates the rollout — a deploy that never reports healthy
rolls back automatically.

### Smoke test

After every deploy, confirm both connectivity and every tool:

```bash
uv run --extra server python scripts/smoke_server.py https://kg-graph.fly.dev <your-token>
```

Exits 0 iff `/healthz` returns 200 and all 8 tools (`get_corpus_overview`,
`search_chunks`, `search_papers`, `get_paper`, `get_concept`, `get_results`,
`get_citations`, `get_dependency_chain`) return without error. Any non-zero exit
or `ERROR` line means investigate before telling anyone the server is live.

### Health

- `GET /healthz` is unauthenticated and returns `{"server": true, "graph": <bool>}` —
  `graph: false` (HTTP 503) means the app is up but Aura is unreachable (e.g. paused
  free-tier instance — resume in the Aura console, same as the pipeline's Aura).
- Fly's own `[[http_service.checks]]` in `docker/fly.toml` polls the same endpoint
  and will mark the machine unhealthy / stop routing traffic to it if `graph` stays
  false or the endpoint stops responding.
- All `/v1/*` traffic (the MCP endpoint itself, `/v1/mcp`) requires a valid bearer
  token; `/healthz` deliberately does not, so uptime monitors don't need a token.

### Fly CLI setup

The Fly CLI is not part of the base toolchain — install and authenticate once per
machine before running any `fly` command above:

```bash
brew install flyctl
fly auth login
```

First-time app creation (creates the Fly app from `docker/fly.toml` without
deploying):

```bash
fly launch --no-deploy --copy-config -c docker/fly.toml   # accept the app name
```

Set secrets (Neo4j creds, OpenAI key, and the `KG_TOKENS` allowlist). Mint a
token first, then paste its entry into `fly secrets set`:

```bash
uv run python scripts/issue_token.py osian
# copy BOTH printed lines: the token goes to the colleague/yourself (once),
# the entry ("name:salt:hash") goes into KG_TOKENS below

fly secrets set -a kg-graph \
  KG_NEO4J_URI=... KG_NEO4J_USER=... KG_NEO4J_PASSWORD=... \
  OPENAI_API_KEY=... KG_TOKENS="<paste the name:salt:hash entry here>"
```

Comma-append additional `name:salt:hash` entries to `KG_TOKENS` for more users.

Then `fly deploy -c docker/fly.toml` and the smoke test above. Record the deployed
URL (`https://<app>.fly.dev`) — the `kg` plugin's `.mcp.json` needs it.

### Read-only Neo4j user — outstanding follow-up

`KG_NEO4J_USER` should be a dedicated Neo4j user with the `reader` role, never the
admin account — `server/graph.py` already opens every session with
`default_access_mode=READ_ACCESS`, but a genuinely read-only DB user is defense in
depth. As of Task 6, Aura's free tier didn't support creating additional users, so
`.env`/`fly secrets` currently point `KG_NEO4J_USER` at the same admin credentials
the pipeline uses. If/when the Aura tier is upgraded (or a `kg_reader` user becomes
available another way), create it with the `reader` role, then update
`KG_NEO4J_USER`/`KG_NEO4J_PASSWORD` locally and via `fly secrets set` and redeploy.
