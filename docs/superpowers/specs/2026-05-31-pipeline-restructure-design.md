# `pipeline/` restructure: stage-based packages

**Date:** 2026-05-31
**Status:** Approved (design)
**Scope:** Move-only refactor of `pipeline/` into stage-based subpackages + import rewrites. No behavior change.

## Problem

`pipeline/` is a flat dump of 19 modules (the `assets/` subpackage is already grouped). Responsibilities — Dagster wiring, ingest, extraction, resolution, graph write, analysis — are interleaved with no structure, so the package root is hard to navigate.

## Goals

- Group library modules by pipeline stage so each stage's code sits next to the asset that drives it, mirroring the asset DAG.
- Keep genuinely-shared modules out of any single stage package (no cross-stage coupling).
- Zero behavior change: a pure move + import rewrite, proven by the existing test suite and a Dagster code-location load check.

## Non-goals

- No test-folder restructure — `tests/` stays flat; only its import lines change.
- No re-export shims / `__init__` magic — empty `__init__.py` per package (matching the existing `assets/` convention), full module paths everywhere.
- No code changes inside any moved module beyond its own import lines.
- `tests/` parity, splitting large modules, or any logic change are explicitly out of scope.

## Target layout

```
pipeline/
  __init__.py             # unchanged (empty)
  definitions.py          # Dagster entry point — STAYS at root (workspace.yaml loads pipeline.definitions)
  text_norm.py            # shared util (extraction<->graph_write key contract) — STAYS at root
  embedding.py            # shared OpenAI embed helper (ingest chunks + resolution) — STAYS at root
  assets/                 # unchanged
  runtime/                # resources, partitions, jobs, schedules, storage
  ingest/                 # source, parsing, chunking
  extraction/             # extraction, extraction_anthropic
  resolution/             # resolver, canonicalize
  graph/                  # cypher, research_port, schema
  analysis/               # analysis
```

Root drops from 19 modules to 3 (entry point + 2 shared utils) plus 6 packages.

### Why these placements

- **`runtime/`** — Dagster wiring and infra config. `storage` (MinIO bucket names, imported by 8 assets) is infra config and belongs here, not at root.
- **Shared at root** — `text_norm` is imported by both `extraction` and `graph_write` (it exists to keep their dedup keys/ids aligned — a contract *between* stages, owned by neither). `embedding` is imported by both `chunks` (ingest) and `resolved_entities` (resolution). Placing either inside one stage would force the other stage to import across stage boundaries; keeping them at root avoids that.
- **`graph/`** — `cypher` (Cypher fragments), `research_port` (S2 enrichment + `WRITE_PAPER` Cypher), and `schema` (KG schema for Aura; currently imported only by its test) all describe the target graph.

### Exact module → destination

| Module | Destination |
|---|---|
| `resources.py`, `partitions.py`, `jobs.py`, `schedules.py`, `storage.py` | `runtime/` |
| `source.py`, `parsing.py`, `chunking.py` | `ingest/` |
| `extraction.py`, `extraction_anthropic.py` | `extraction/` |
| `resolver.py`, `canonicalize.py` | `resolution/` |
| `cypher.py`, `research_port.py`, `schema.py` | `graph/` |
| `analysis.py` | `analysis/` |
| `definitions.py`, `text_norm.py`, `embedding.py`, `__init__.py` | stay at root |

## Mechanics

1. **Create packages:** add `runtime/ ingest/ extraction/ resolution/ graph/ analysis/`, each with an empty `__init__.py` (matches `assets/__init__.py`).
2. **Move:** `git mv` each module to its destination (preserves blame/history).
3. **Rewrite imports** everywhere a moved module is referenced — `pipeline/assets/*`, `tests/*`, inter-module, and `definitions.py`. Cover every form: `from pipeline.<m> import …`, `import pipeline.<m>`, `import pipeline.<m> as …`, `from pipeline import <m>`. The rename map is the destination table above (e.g. `pipeline.resolver` → `pipeline.resolution.resolver`, `pipeline.storage` → `pipeline.runtime.storage`). Note `pipeline.extraction` becomes a *package*, so the module is `pipeline.extraction.extraction` (and `extraction_anthropic` imports become `from pipeline.extraction.extraction import …`).
4. **`workspace.yaml` unchanged** — `definitions.py` stays at root, so `module_name: pipeline.definitions` is still valid.

## Verification

- `uv run pytest -q` — full suite green (the existing tests exercise every moved module; broken imports fail loudly).
- `uv run python -c "import pipeline.definitions"` — proves the Dagster code location still imports cleanly (the real failure mode for a Dagster reorg).
- `uv run ruff check pipeline tests` — clean (catches unused/var import leftovers).

## Timing / safety

Execute the moves **only after the in-flight materialization completes.** `pipeline/` is bind-mounted read-only into the running Dagster containers; renaming modules mid-run can break the in-flight chain's imports. After the moves land and tests pass, restart `kr_dagster_webserver` + `kr_dagster_daemon` so the new module paths load.

## Acceptance criteria

- Layout matches the target above; root contains only `__init__.py`, `definitions.py`, `text_norm.py`, `embedding.py`, `assets/`, and the 6 stage/runtime packages.
- Full suite green, `import pipeline.definitions` succeeds, ruff clean.
- `git mv` used so history is preserved; no logic diffs inside moved modules (only import lines).
