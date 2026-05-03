# Morning handoff — 2026-05-04

Overnight work on the knowledge-pipeline substrate. Read top-to-bottom.

## TL;DR

- **17 commits** on `main`. All 17 plan tasks have their **code** done.
- **31 unit tests pass**, 1 integration test correctly auto-skipped.
- **Docker is not installed on this machine** — that's the first thing to fix this morning.
- **Aura DB not provisioned, secrets not in `.env`** — these need your hands.
- Nothing has been done that touches your iCloud vault, your existing alethograph Aura DB, or any external service. The code is all written and tested locally; nothing has *run* against real infrastructure yet.

## What's in the repo now

```
knowledge-retrieval/
├── docker-compose.yml                    # postgres + minio + 2 dagster services
├── docker/
│   ├── dagster.Dockerfile
│   ├── dagster.yaml                      # postgres-backed instance config
│   └── workspace.yaml
├── pyproject.toml + uv.lock              # 166 deps locked
├── .env.example                          # template
├── .env                                  # local copy with placeholder values (gitignored)
├── pipeline/
│   ├── definitions.py                    # 5 assets + sensor + bulk_reingest job
│   ├── partitions.py                     # loader for data/partitions.json
│   ├── resources.py                      # Neo4j / MinIO / OpenAI / Anthropic
│   ├── schema.py                         # NODE_TYPES, RELATIONSHIP_TYPES, PATTERNS, INIT_CYPHER
│   ├── sensors.py                        # minio_pdf_sensor (30s polling)
│   ├── jobs.py                           # bulk_reingest job
│   └── assets/
│       ├── pdf_blob.py                   # MinIO pointer + sha256
│       ├── v1_md_blob.py                 # MinIO pointer (present/absent aware)
│       ├── kg_extracted.py               # SimpleKGPipeline runner
│       ├── structural_overlay.py         # legacy-DB → new-DB overlay
│       └── paper_summary.py              # Claude Sonnet structured summary
├── scripts/
│   ├── discover_partitions.py            # legacy-DB → data/partitions.json
│   ├── snapshot_vault.py                 # vault → MinIO tarball + per-paper uploads
│   └── init_neo4j.py                     # apply schema constraints + vector index
├── notebooks/
│   └── smoke_test.ipynb                  # 4 retrieval patterns (vector / vector+cypher / pure-cypher / GraphRAG)
├── tests/
│   ├── test_resources.py                 # 5 tests
│   ├── test_partitions.py                # 5 tests
│   ├── test_schema.py                    # 5 tests
│   ├── test_snapshot.py                  # 3 tests
│   ├── test_definitions.py               # 1 test
│   ├── test_pdf_blob.py                  # 1 test
│   ├── test_v1_md_blob.py                # 2 tests
│   ├── test_structural_overlay.py        # 2 tests
│   ├── test_paper_summary.py             # 3 tests
│   ├── test_sensors.py                   # 4 tests
│   └── integration/test_single_paper.py  # 1 integration (skipped without --run-integration)
└── docs/
    ├── specs/2026-05-03-knowledge-pipeline-design.md
    ├── superpowers/plans/2026-05-03-knowledge-pipeline-substrate.md
    ├── operations.md                     # ops runbook
    └── MORNING-HANDOFF.md                # ← you are here
```

## What's NOT done — your morning checklist

These are the steps the substrate needs to actually run. None of them are code work; they're operator steps that need your credentials, your machine, or your judgment.

### 1. Install Docker Desktop (one-time)

```bash
brew install --cask docker
open -a Docker
```

Wait until the Docker icon in the menu bar shows "Docker Desktop is running." Then:

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
docker compose config > /dev/null && echo "compose valid"
```

Should print `compose valid`.

### 2. Provision a new Neo4j Aura database (Plan Task 4)

In the Aura console:

1. **Trigger an on-demand backup of the existing alethograph DB first.** Defensive — the new pipeline won't touch it, but a snapshot costs nothing.
2. Create a new database. Either a new database within the existing instance (if your tier supports it) or a new instance. Capture: URI, username, password, database name.

### 3. Populate `.env`

Edit `/Users/osianshelley/Projects/knowledge-retrieval/.env` and fill in:

```
NEO4J_NEW_URI=neo4j+s://...
NEO4J_NEW_USERNAME=neo4j
NEO4J_NEW_PASSWORD=...
NEO4J_NEW_DATABASE=neo4j

NEO4J_LEGACY_URI=...      # from ~/.claude/research-neo4j.json
NEO4J_LEGACY_USERNAME=neo4j
NEO4J_LEGACY_PASSWORD=...

OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
```

The MinIO and Postgres values can stay at their defaults (`minioadmin` / `dagster`).

Verify:
```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
uv run python -c "
import os
from dotenv import load_dotenv; load_dotenv()
from neo4j import GraphDatabase
for prefix in ('NEW', 'LEGACY'):
    drv = GraphDatabase.driver(os.environ[f'NEO4J_{prefix}_URI'], auth=(os.environ[f'NEO4J_{prefix}_USERNAME'], os.environ[f'NEO4J_{prefix}_PASSWORD']))
    drv.verify_connectivity(); print(prefix, 'ok')
"
```

Should print `NEW ok` and `LEGACY ok`.

### 4. Bring up Postgres + MinIO

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
docker compose up -d postgres minio minio-init
docker compose logs minio-init | tail -5   # should show 3 buckets created
```

### 5. Apply schema to the new Aura DB

```bash
uv run python scripts/init_neo4j.py
```

Should print 8 statements + summary. No errors.

### 6. Build & start the Dagster services

```bash
docker compose up -d
docker compose ps         # should show 5 containers running/healthy
```

Open `http://localhost:3000`. The asset graph will be empty because partitions aren't discovered yet. That's expected.

### 7. Run partition discovery

```bash
uv run python scripts/discover_partitions.py
```

Expected output: `resolved: ~135-141 → data/partitions.json` plus a count of unresolved (the script writes those to `data/partitions_unresolved.json`).

**Triage step:** Open `data/partitions_unresolved.json` and decide which ones to manually fix. Most likely you'll want to add hand-written entries for textbooks (no arxiv_id, no DOI). Append to `data/partitions.json` directly. Then validate JSON:

```bash
uv run python -c "import json; json.loads(open('data/partitions.json').read()); print('json ok')"
```

Commit:

```bash
git add data/partitions.json
git commit -m "feat(data): partitions.json from discovery + manual triage"
```

Restart Dagster so the partitions reload:

```bash
docker compose restart dagster-webserver dagster-daemon
```

UI now shows ~140 partitions per asset.

### 8. Run vault snapshot

```bash
uv run python scripts/snapshot_vault.py
```

This tarballs the iCloud vault → MinIO `vault-snapshots/` and uploads every paper's PDF + matching v1 .md → `pdfs/` and `legacy-summaries/`. Takes a few minutes (vault has ~141 PDFs + 1080 .md files).

Verify in the MinIO console (`http://localhost:9001`):
- `pdfs/` should have ~140 objects
- `legacy-summaries/` should have ~140 objects
- `vault-snapshots/` should have one `.tar.gz`

### 9. 5-paper sample run

Before the full bulk ingest, sanity-check on 5 papers:

```bash
uv run pytest tests/integration/test_single_paper.py --run-integration -v -s
```

Expected: ~5–15 minutes wall time, ~$0.50–$2 in API costs. Test asserts `≥5 papers, >50 chunks, ≥1 concept` in the new DB after.

If extracted concepts look generic ("Process", "System") rather than domain-relevant ("BSDE", "Wasserstein distance"), the schema may need `description:` hints on each `NODE_TYPES` entry. See the legacy spec file 10 for examples — but only do this if extraction quality is bad. Don't tune preemptively.

### 10. Full bulk reingest

In Dagster UI: Jobs → `bulk_reingest` → "Launch Backfill" → select all partitions → "Submit."

Expected: 30–90 min wall time, $50–200 in API costs. Failures on individual partitions are isolated; click red cells to retry.

### 11. Smoke test

```bash
uv run jupyter lab notebooks/smoke_test.ipynb
```

Step through the four retrieval patterns. If Pattern 4 (full GraphRAG) returns a coherent paragraph, the substrate is working.

### 12. Commit the partitions.json + any tuning

If you tuned the schema during step 9, regenerate:
```bash
uv run python scripts/init_neo4j.py
git add pipeline/schema.py
git commit -m "tune(schema): add description hints for domain-relevant extraction"
```

## Concerns / things I noticed

- **`tests/test_definitions.py`** was added by the implementer in Task 9 but wasn't in the plan. It's a 1-test smoke check that imports `pipeline.definitions` and asserts the asset/resource counts. Useful, low cost. I let it stay.
- **`pyproject.toml`** had `testpaths` added at the end (post-Task-17) because pytest was discovering `neo4j_course/` tutorial tests and failing. That's commit `1a8351d`. Innocuous fix.
- **Tasks 4, 6 (real run), 7 (real run), 8 (real run), 9 (UI), 10–14 (UI), 15 (UI), 16 (UI + bulk run)** all have operator steps that were deferred. Steps 1–12 above cover them.
- **Spec deviations:** none. The plan was followed verbatim except for explicitly deferring operator-only steps.
- **Coverage:** every spec section has corresponding implementation. The plan's self-review section confirms no gaps.

## Quick sanity checks if anything seems off

```bash
# All unit tests still pass?
uv run pytest -v

# Dagster code location loads?
DAGSTER_HOME=$(pwd)/.dagster_home uv run dagster definitions list-locations -w docker/workspace.yaml

# Docker services healthy?
docker compose ps

# Aura DB has the schema?
uv run python -c "
from pipeline.resources import new_neo4j_from_env
new = new_neo4j_from_env()
with new.get_driver().session(database=new.database) as s:
    for r in s.run('SHOW INDEXES'): print(r['name'], r['type'])
"
```

## What comes next (after the substrate is up)

Per spec §11 and the plan:

1. **Read-side skill port** — own spec. Re-points `/researcher-*`, `/idea`, `/topic-expert` at the new DB via `neo4j_graphrag` retrievers.
2. **Write-side skill port** — own spec. Re-points `/research`, `/study` at MinIO upload + Dagster sensor.
3. **Optional retrieval upgrades** (community summaries / HippoRAG / LightRAG) — own spec(s). Defer until concrete retrieval pain demands them.

Don't start any of these until the substrate has actually run end-to-end and you've used it for real for a bit.
