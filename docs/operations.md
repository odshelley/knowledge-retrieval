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

1. Drop the PDF into MinIO `pdfs/` with key `<paper_id>.pdf`. (Optionally drop a hand-written summary into `legacy-summaries/<paper_id>.md`.)
2. Add an entry to `data/partitions.json` and commit.
3. Restart `dagster-webserver` to reload the partition list: `docker compose restart dagster-webserver dagster-daemon`.
4. The `minio_pdf_sensor` (if enabled) picks the new key up within 30s and runs the full pipeline for that partition. Or trigger manually from the UI.

## Re-extracting a paper after a schema change

1. Bump the schema in `pipeline/schema.py`.
2. Apply: `uv run python scripts/init_neo4j.py`.
3. In the UI: select `kg_extracted` for the affected partitions → "Materialize selected." Downstream assets (`structural_overlay`, `paper_summary`) will materialize automatically because their inputs changed.

## Regenerating summaries with a new prompt

1. Edit `PROMPT_TEMPLATE` in `pipeline/assets/paper_summary.py`.
2. Restart Dagster.
3. UI → `paper_summary` → select all partitions in your topic of interest → "Materialize selected."

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
| `kg_extracted`: empty Concept set on every paper | Tutorial-style schema (too generic) — tune `NODE_TYPES` with descriptions | See spec §6 |
| Sensor fires but no new runs | `minio_pdf_sensor` cursor too far ahead | UI → Sensors → "Reset cursor" |

## Out-of-scope reminders

These are explicit non-goals (spec §2 / §11):
- Skill ports (`/research`, `/researcher-*`, `/idea`, `/topic-expert`) — separate workstream.
- Production Dagster topology (k8s, Helm, separate user-code gRPC) — separate spec.
- Microsoft GraphRAG / HippoRAG / LightRAG retrieval patterns — separate spec(s).
- Decommissioning the legacy alethograph DB — only after both skill ports complete.
