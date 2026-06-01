# `pipeline/` Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the 19 flat modules in `pipeline/` into stage-based subpackages (`runtime/ ingest/ extraction/ resolution/ graph/ analysis/`), keeping shared utils + the Dagster entry point at root, with zero behaviour change.

**Architecture:** Pure move + import-rewrite refactor. Each task moves one destination group with `git mv` (preserving history) and rewrites every importer, ending with the full test suite green. There is no new behaviour, so the safety net is the existing suite plus an explicit `import pipeline.definitions` check (the real failure mode for a Dagster code-location reorg).

**Tech Stack:** Python 3.12, Dagster, pytest, `uv`, ruff. Dagster loads `pipeline.definitions` per `docker/workspace.yaml`.

**Reference spec:** `docs/superpowers/specs/2026-05-31-pipeline-restructure-design.md`.

**Rename map (every `pipeline.<old>` → `pipeline.<new>`):**

| old module path | new module path |
|---|---|
| `pipeline.resources` | `pipeline.runtime.resources` |
| `pipeline.partitions` | `pipeline.runtime.partitions` |
| `pipeline.jobs` | `pipeline.runtime.jobs` |
| `pipeline.schedules` | `pipeline.runtime.schedules` |
| `pipeline.storage` | `pipeline.runtime.storage` |
| `pipeline.source` | `pipeline.ingest.source` |
| `pipeline.parsing` | `pipeline.ingest.parsing` |
| `pipeline.chunking` | `pipeline.ingest.chunking` |
| `pipeline.extraction` | `pipeline.extraction.extraction` |
| `pipeline.extraction_anthropic` | `pipeline.extraction.extraction_anthropic` |
| `pipeline.resolver` | `pipeline.resolution.resolver` |
| `pipeline.canonicalize` | `pipeline.resolution.canonicalize` |
| `pipeline.cypher` | `pipeline.graph.cypher` |
| `pipeline.research_port` | `pipeline.graph.research_port` |
| `pipeline.schema` | `pipeline.graph.schema` |
| `pipeline.analysis` | `pipeline.analysis.analysis` |

**Two non-dotted import forms** (a `pipeline.<old>` substring search misses these — handle explicitly):
- `from pipeline import research_port as rp` → `from pipeline.graph import research_port as rp` (in `pipeline/assets/triage_metadata.py:9`)
- `from pipeline import schema` → `from pipeline.graph import schema` (in `tests/test_schema.py:1`)

**Collision caution:** when rewriting `pipeline.extraction`, do **not** touch `pipeline.extraction_anthropic`. Rewrite `pipeline.extraction_anthropic` first, then the bare `pipeline.extraction` references — and verify by eye that no path became `pipeline.extraction.extraction.extraction_anthropic`.

**⚠️ Timing:** Do not start until the in-flight Dagster materialization has finished. `pipeline/` is bind-mounted read-only into the running containers; renaming modules mid-run breaks the in-flight chain. After Task 8, restart `kr_dagster_webserver` + `kr_dagster_daemon` to reload.

---

## Task 1: Scaffold the empty packages

Create the six package directories with empty `__init__.py` (matching `pipeline/assets/__init__.py`). No modules move yet, so the suite stays green — this isolates the "create dirs" step from the moves.

**Files:**
- Create: `pipeline/runtime/__init__.py`, `pipeline/ingest/__init__.py`, `pipeline/extraction/__init__.py`, `pipeline/resolution/__init__.py`, `pipeline/graph/__init__.py`, `pipeline/analysis/__init__.py`

- [ ] **Step 1: Create the package dirs + empty inits**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
for p in runtime ingest extraction resolution graph analysis; do
  mkdir -p "pipeline/$p" && : > "pipeline/$p/__init__.py"
done
```

- [ ] **Step 2: Verify suite still green (nothing references the new empty packages yet)**

Run: `uv run pytest -q`
Expected: all pass (same count as before the refactor).

- [ ] **Step 3: Commit**

```bash
git add pipeline/runtime/__init__.py pipeline/ingest/__init__.py pipeline/extraction/__init__.py \
        pipeline/resolution/__init__.py pipeline/graph/__init__.py pipeline/analysis/__init__.py
git commit -m "refactor(pipeline): scaffold stage-based package dirs"
```

---

## Task 2: Move the `runtime/` group (resources, partitions, jobs, schedules, storage)

**Files:**
- Move: `resources.py partitions.py jobs.py schedules.py storage.py` → `pipeline/runtime/`
- Rewrite imports in: `pipeline/definitions.py`, `pipeline/runtime/schedules.py` (self), all 8 `pipeline/assets/*.py` that import `partitions`/`storage`, `tests/test_resources.py`, `tests/test_partitions.py`, `tests/test_resolver.py` (resources line only), `tests/integration/test_end_to_end.py`

- [ ] **Step 1: Move the modules**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
git mv pipeline/resources.py   pipeline/runtime/resources.py
git mv pipeline/partitions.py  pipeline/runtime/partitions.py
git mv pipeline/jobs.py        pipeline/runtime/jobs.py
git mv pipeline/schedules.py   pipeline/runtime/schedules.py
git mv pipeline/storage.py     pipeline/runtime/storage.py
```

- [ ] **Step 2: Rewrite every importer of these five modules**

Apply the rename map (`pipeline.resources`→`pipeline.runtime.resources`, `pipeline.partitions`→`pipeline.runtime.partitions`, `pipeline.jobs`→`pipeline.runtime.jobs`, `pipeline.schedules`→`pipeline.runtime.schedules`, `pipeline.storage`→`pipeline.runtime.storage`) on the import line(s) of each file below. Symbols imported do not change — only the module path.

- `pipeline/definitions.py`: `from pipeline.jobs import …` → `from pipeline.runtime.jobs import …`; `from pipeline.schedules import …` → `from pipeline.runtime.schedules import …`; `from pipeline.resources import …` → `from pipeline.runtime.resources import …`
- `pipeline/runtime/schedules.py`: `from pipeline.partitions import DOCUMENTS_PARTITION` → `from pipeline.runtime.partitions import DOCUMENTS_PARTITION`. (Leave its `from pipeline.source import …` line untouched — `source` moves in Task 3.)
- All 8 asset modules — `pipeline/assets/raw_blob.py`, `chunks.py`, `parsed_document.py`, `extracted_graph.py`, `resolved_entities.py`, `graph_write.py`, `triage_metadata.py`, `paper_analysis.py` — import both: rewrite `from pipeline.partitions import documents_partitions_def` → `from pipeline.runtime.partitions import documents_partitions_def`, and `from pipeline.storage import …` → `from pipeline.runtime.storage import …`.
- `tests/test_resources.py`: `from pipeline.resources import …` → `from pipeline.runtime.resources import …`
- `tests/test_partitions.py`: `from pipeline.partitions import …` → `from pipeline.runtime.partitions import …`
- `tests/test_resolver.py`: `from pipeline.resources import …` → `from pipeline.runtime.resources import …` (leave the `pipeline.resolver` line for Task 5).
- `tests/integration/test_end_to_end.py`: rewrite both `pipeline.resources` and `pipeline.partitions` import lines to the `pipeline.runtime.*` paths.

Find any stragglers: `grep -rnE "pipeline\.(resources|partitions|jobs|schedules|storage)\b" pipeline tests` — every hit must be a `pipeline.runtime.*` path after this step.

- [ ] **Step 3: Verify suite green + entry point imports**

Run: `uv run pytest -q && uv run python -c "import pipeline.definitions"`
Expected: all tests pass; the import prints nothing and exits 0.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(pipeline): move resources/partitions/jobs/schedules/storage to runtime/"
```

---

## Task 3: Move the `ingest/` group (source, parsing, chunking)

**Files:**
- Move: `source.py parsing.py chunking.py` → `pipeline/ingest/`
- Rewrite imports in: `pipeline/runtime/schedules.py` (source), `pipeline/assets/raw_blob.py` (source), `pipeline/assets/parsed_document.py` (parsing), `pipeline/assets/chunks.py` (chunking), `tests/test_source.py`, `tests/test_parsing.py`, `tests/test_parsed_document.py`, `tests/test_chunking.py`

- [ ] **Step 1: Move the modules**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
git mv pipeline/source.py   pipeline/ingest/source.py
git mv pipeline/parsing.py  pipeline/ingest/parsing.py
git mv pipeline/chunking.py pipeline/ingest/chunking.py
```

- [ ] **Step 2: Rewrite importers**

Apply `pipeline.source`→`pipeline.ingest.source`, `pipeline.parsing`→`pipeline.ingest.parsing`, `pipeline.chunking`→`pipeline.ingest.chunking` on the import line of each:
- `pipeline/runtime/schedules.py`: `from pipeline.source import file_partition_key, list_pdf_files, source_dir` → `from pipeline.ingest.source import file_partition_key, list_pdf_files, source_dir`
- `pipeline/assets/raw_blob.py`: the `pipeline.source` import → `pipeline.ingest.source`
- `pipeline/assets/parsed_document.py`: the `pipeline.parsing` import → `pipeline.ingest.parsing`
- `pipeline/assets/chunks.py`: the `pipeline.chunking` import → `pipeline.ingest.chunking`
- `tests/test_source.py` → `pipeline.ingest.source`; `tests/test_parsing.py` and `tests/test_parsed_document.py` → `pipeline.ingest.parsing`; `tests/test_chunking.py` → `pipeline.ingest.chunking`.

Stragglers: `grep -rnE "pipeline\.(source|parsing|chunking)\b" pipeline tests` — all must be `pipeline.ingest.*`.

- [ ] **Step 3: Verify**

Run: `uv run pytest -q && uv run python -c "import pipeline.definitions"`
Expected: all pass; import exits 0.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(pipeline): move source/parsing/chunking to ingest/"
```

---

## Task 4: Move the `extraction/` group (extraction, extraction_anthropic)

**Note the prefix collision:** rewrite `pipeline.extraction_anthropic` **before** the bare `pipeline.extraction`, and confirm no path became `pipeline.extraction.extraction.extraction_anthropic`.

**Files:**
- Move: `extraction.py extraction_anthropic.py` → `pipeline/extraction/`
- Rewrite imports in: `pipeline/extraction/extraction_anthropic.py` (self → extraction), `pipeline/assets/extracted_graph.py`, `tests/test_extraction.py`, `tests/test_extraction_anthropic.py`

- [ ] **Step 1: Move the modules**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
git mv pipeline/extraction_anthropic.py pipeline/extraction/extraction_anthropic.py
git mv pipeline/extraction.py           pipeline/extraction/extraction.py
```

- [ ] **Step 2: Rewrite importers (anthropic first)**

- `pipeline/extraction/extraction_anthropic.py:8`: `from pipeline.extraction import (` → `from pipeline.extraction.extraction import (`
- `pipeline/assets/extracted_graph.py:13`: `from pipeline.extraction_anthropic import extract_from_chunk_anthropic` → `from pipeline.extraction.extraction_anthropic import extract_from_chunk_anthropic`
- `pipeline/assets/extracted_graph.py:12`: `from pipeline.extraction import extract_from_chunk, merge_results` → `from pipeline.extraction.extraction import extract_from_chunk, merge_results`
- `tests/test_extraction_anthropic.py`: rewrite its `pipeline.extraction_anthropic` import → `pipeline.extraction.extraction_anthropic`, and any `pipeline.extraction` import → `pipeline.extraction.extraction`.
- `tests/test_extraction.py:1`: `from pipeline.extraction import (` → `from pipeline.extraction.extraction import (`

Stragglers: `grep -rn "pipeline\.extraction" pipeline tests` — confirm every line reads `pipeline.extraction.extraction` or `pipeline.extraction.extraction_anthropic`, with no triple `.extraction.extraction.extraction_anthropic`.

- [ ] **Step 3: Verify**

Run: `uv run pytest -q && uv run python -c "import pipeline.definitions"`
Expected: all pass; import exits 0.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(pipeline): move extraction + extraction_anthropic to extraction/"
```

---

## Task 5: Move the `resolution/` group (resolver, canonicalize)

**Files:**
- Move: `resolver.py canonicalize.py` → `pipeline/resolution/`
- Rewrite imports in: `pipeline/resolution/resolver.py` (self → canonicalize), `pipeline/assets/resolved_entities.py`, `pipeline/assets/graph_write.py`, `tests/test_resolver.py` (resolver line), `tests/test_resolve_concepts.py`, `tests/test_canonicalize.py`

- [ ] **Step 1: Move the modules**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
git mv pipeline/resolver.py     pipeline/resolution/resolver.py
git mv pipeline/canonicalize.py pipeline/resolution/canonicalize.py
```

- [ ] **Step 2: Rewrite importers**

Apply `pipeline.resolver`→`pipeline.resolution.resolver`, `pipeline.canonicalize`→`pipeline.resolution.canonicalize`:
- `pipeline/resolution/resolver.py`: `from pipeline.canonicalize import canonical_key` → `from pipeline.resolution.canonicalize import canonical_key`
- `pipeline/assets/resolved_entities.py`: the `pipeline.resolver` import → `pipeline.resolution.resolver`
- `pipeline/assets/graph_write.py`: the `pipeline.resolver` import → `pipeline.resolution.resolver`
- `tests/test_resolver.py`: the `from pipeline.resolver import …` line → `pipeline.resolution.resolver` (the resources line was already fixed in Task 2)
- `tests/test_resolve_concepts.py`: `from pipeline.resolver import resolve_concepts` → `from pipeline.resolution.resolver import resolve_concepts`
- `tests/test_canonicalize.py`: `from pipeline.canonicalize import canonical_key` → `from pipeline.resolution.canonicalize import canonical_key`

Stragglers: `grep -rnE "pipeline\.(resolver|canonicalize)\b" pipeline tests` — all must be `pipeline.resolution.*`.

- [ ] **Step 3: Verify**

Run: `uv run pytest -q && uv run python -c "import pipeline.definitions"`
Expected: all pass; import exits 0.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(pipeline): move resolver + canonicalize to resolution/"
```

---

## Task 6: Move the `graph/` group (cypher, research_port, schema)

**Files:**
- Move: `cypher.py research_port.py schema.py` → `pipeline/graph/`
- Rewrite imports in: `pipeline/assets/triage_metadata.py` (research_port — non-dotted form), `tests/test_cypher.py`, `tests/test_research_port.py` (import **and** the `@patch` string), `tests/test_schema.py` (non-dotted form)

- [ ] **Step 1: Move the modules**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
git mv pipeline/cypher.py        pipeline/graph/cypher.py
git mv pipeline/research_port.py pipeline/graph/research_port.py
git mv pipeline/schema.py        pipeline/graph/schema.py
```

- [ ] **Step 2: Rewrite importers (mind the non-dotted + @patch forms)**

- `pipeline/assets/triage_metadata.py:9`: `from pipeline import research_port as rp` → `from pipeline.graph import research_port as rp`
- `tests/test_research_port.py:2`: `from pipeline.research_port import (` → `from pipeline.graph.research_port import (`
- `tests/test_research_port.py:18`: `@patch("pipeline.research_port.requests.get")` → `@patch("pipeline.graph.research_port.requests.get")`
- `tests/test_cypher.py`: the `pipeline.cypher` import → `pipeline.graph.cypher`
- `tests/test_schema.py:1`: `from pipeline import schema` → `from pipeline.graph import schema`

Stragglers: `grep -rnE "pipeline\.(cypher|research_port|schema)\b|from pipeline import (research_port|schema)" pipeline tests` — all must point at `pipeline.graph.*`.

- [ ] **Step 3: Verify**

Run: `uv run pytest -q && uv run python -c "import pipeline.definitions"`
Expected: all pass; import exits 0.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(pipeline): move cypher/research_port/schema to graph/"
```

---

## Task 7: Move the `analysis/` group (analysis)

**Files:**
- Move: `analysis.py` → `pipeline/analysis/analysis.py`
- Rewrite imports in: `pipeline/assets/paper_analysis.py`, `tests/test_analysis.py`

- [ ] **Step 1: Move the module**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
git mv pipeline/analysis.py pipeline/analysis/analysis.py
```

- [ ] **Step 2: Rewrite importers**

Apply `pipeline.analysis`→`pipeline.analysis.analysis`:
- `pipeline/assets/paper_analysis.py`: the `pipeline.analysis` import → `pipeline.analysis.analysis`
- `tests/test_analysis.py:1`: `from pipeline.analysis import ANALYSIS_FIELDS, PaperAnalysis, validate_analysis` → `from pipeline.analysis.analysis import ANALYSIS_FIELDS, PaperAnalysis, validate_analysis`

Stragglers: `grep -rn "pipeline\.analysis\b" pipeline tests` — every hit must read `pipeline.analysis.analysis` (the package import `pipeline.analysis` alone should appear nowhere).

- [ ] **Step 3: Verify**

Run: `uv run pytest -q && uv run python -c "import pipeline.definitions"`
Expected: all pass; import exits 0.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(pipeline): move analysis to analysis/"
```

---

## Task 8: Final verification

**Files:** none (verification only).

- [ ] **Step 1: Assert the target layout**

```bash
cd /Users/osianshelley/Projects/knowledge-retrieval
echo "--- root .py (expect only __init__, definitions, text_norm, embedding) ---"
ls -1 pipeline/*.py | xargs -n1 basename
echo "--- packages ---"
ls -1d pipeline/*/
```
Expected root files: `__init__.py`, `definitions.py`, `embedding.py`, `text_norm.py`. Expected dirs: `analysis/ assets/ extraction/ graph/ ingest/ resolution/ runtime/`.

- [ ] **Step 2: Full suite + entry-point import + lint**

```bash
uv run pytest -q
uv run python -c "import pipeline.definitions; print('definitions OK')"
uv run ruff check pipeline tests
```
Expected: all tests pass (same count as pre-refactor); prints `definitions OK`; ruff `All checks passed!`.

- [ ] **Step 3: Confirm no stale `pipeline.<module>` paths remain**

```bash
grep -rnE "pipeline\.(resources|partitions|jobs|schedules|storage|source|parsing|chunking|resolver|canonicalize|cypher|research_port|schema)\b" pipeline tests
grep -rnE "from pipeline import (research_port|schema)\b" pipeline tests
```
Expected: **no output** from either (all rewritten). `pipeline.extraction` / `pipeline.analysis` are intentionally still present as the new package prefixes, so they are not searched here.

- [ ] **Step 4: (No commit needed — Tasks 2–7 already committed.)**

---

## Post-implementation (operational, not part of the commits)

After the branch is merged and you've pulled `main`, reload Dagster so the new module paths take effect:

```bash
docker compose restart kr_dagster_webserver kr_dagster_daemon   # or the UI Reload button
```

The `docker/workspace.yaml` entry (`module_name: pipeline.definitions`) is unchanged, so no compose edits are needed.
