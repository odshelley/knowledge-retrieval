# Document → Knowledge-Graph Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Dagster pipeline that ingests raw paper PDFs from a folder on a daily schedule and constructs the alethograph knowledge graph (in the wiped `6b371650` Aura DB) from scratch — parsing, chunking, embedding, extracting typed entities/definitions/results, resolving duplicates, and producing a research-skill-quality analysis.

**Architecture:** Per-document Dagster assets keyed by content hash: `raw_blob → parsed_document (Docling) → triage_metadata (+Semantic Scholar) → chunks (equation-aware) → chunk_embeddings → extracted_graph → resolved_entities → graph_write`, with `paper_analysis` branching off. Dynamic, folder-driven partitions replace the old static/legacy-DB discovery. The old enrichment assets (`legacy_graph_mirror`, `structural_overlay`) are deleted. Entity resolution uses pgvector in the existing Postgres; the decision trail is recorded for a future human-review loop.

**Tech Stack:** Python 3.12, `uv`, Dagster 1.9.5, Neo4j (Aura, `6b371650`), MinIO (S3), Postgres + pgvector, Docling/Granite-Docling, OpenAI (embeddings + extraction), Anthropic Claude (analysis), Semantic Scholar API. TDD with pytest (integration tests gated behind `--run-integration`).

**Spec:** `docs/superpowers/specs/2026-05-27-document-graph-builder-design.md` (read it first).

**Conventions (verified against the repo):**
- Assets: `@asset(partitions_def=..., deps=[...], required_resource_keys={...})`, `def asset(context)`, resources via `context.resources.<key>`, return `MaterializeResult(metadata={...})`, log via `context.log`.
- Resource keys: `neo4j_new`, `minio`, `openai`, `anthropic` (we add `postgres`). `neo4j_legacy` is removed.
- Neo4j: `with res.get_driver().session(database=res.database) as s: s.run(cypher, **params)`.
- MinIO: `context.resources.minio.get_client()` → boto3 S3 client.
- Add deps with `uv add <pkg>`; run things with `uv run …`; tests `uv run pytest …`.
- Integration tests: `@pytest.mark.integration`, skipped unless `uv run pytest --run-integration`.

---

## Phase 0 — Pre-flight gates (do before writing code)

These are the spec's §12 gates. They are spikes, not TDD tasks — but they block implementation.

### Gate A: Docling LaTeX-fidelity spot test

- [ ] **A1.** Pick ~5 of the gnarliest equation-heavy pages from the XVA/stochastics corpus (export single-page PDFs).
- [ ] **A2.** In a scratch venv, run Docling on each and inspect the emitted LaTeX:

```bash
uv run --with docling python - <<'PY'
from docling.document_converter import DocumentConverter
conv = DocumentConverter()
for p in ["p1.pdf","p2.pdf","p3.pdf","p4.pdf","p5.pdf"]:
    md = conv.convert(p).document.export_to_markdown()
    print("="*40, p); print(md)
PY
```

- [ ] **A3.** Eyeball: are displayed equations correct LaTeX? Decision: if ≥4/5 are faithful, proceed Docling-only. If not, escalate Mathpix off the bench (out of scope for this plan — record the finding and stop to re-plan parsing).

### Gate B: Extraction-model evaluation

- [ ] **B1.** Run the extraction prompt (defined in Task 12) on 3 already-understood papers with two candidate models (e.g. `gpt-5-nano` vs a stronger GPT/Claude). Compare concept precision/recall by hand.
- [ ] **B2.** Record the chosen `extraction_model` string; it becomes the default in `OpenAILLMResource` (Task 12).

### Gate C: Confirm target DB is empty + snapshot exists

- [ ] **C1.** Take an Aura snapshot of `6b371650` via the Neo4j Aura console.
- [ ] **C2.** Confirm empty:

```bash
uv run python - <<'PY'
import os
from neo4j import GraphDatabase
d = GraphDatabase.driver(os.environ["NEO4J_NEW_URI"], auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
with d.session(database=os.environ.get("NEO4J_NEW_DATABASE","neo4j")) as s:
    print("node count:", s.run("MATCH (n) RETURN count(n) AS n").single()["n"])
PY
```
Expected: `node count: 0`.

---

## Phase 1 — Foundation (schema, teardown, scheduling, blob)

**Phase exit state:** old enrichment machinery removed; extended schema live; a daily schedule discovers PDFs in a folder, registers dynamic partitions, and lands each PDF in MinIO. No graph build yet.

### Task 1: Add dependencies

**Files:**
- Modify: `pyproject.toml` (via `uv add`)

- [ ] **Step 1: Add runtime deps**

Run:
```bash
cd ~/Projects/knowledge-retrieval/.claude/worktrees/spec-document-graph-builder
uv add docling "psycopg[binary]>=3.2" pgvector requests
```
Expected: `pyproject.toml` `dependencies` gains `docling`, `psycopg[binary]`, `pgvector`, `requests`; `uv.lock` updates.

- [ ] **Step 2: Verify imports resolve**

Run:
```bash
uv run python -c "import docling, psycopg, pgvector, requests; print('ok')"
```
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "build: add docling, psycopg, pgvector, requests deps"
```

---

### Task 2: Extend the schema (Definition / Result / Summary)

**Files:**
- Modify: `pipeline/schema.py`
- Test: `tests/test_schema.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_schema.py`:
```python
from pipeline.schema import (
    NODE_TYPES, RELATIONSHIP_TYPES, PATTERNS, iter_init_statements,
)

def test_new_node_types_present():
    for label in ("Definition", "Result", "Summary"):
        assert label in NODE_TYPES

def test_new_relationship_types_present():
    for rel in ("STATES", "DEFINES", "USES", "DEPENDS_ON", "HAS_SUMMARY"):
        assert rel in RELATIONSHIP_TYPES

def test_new_patterns_present():
    expected = {
        ("Paper", "STATES", "Definition"),
        ("Paper", "STATES", "Result"),
        ("Definition", "DEFINES", "Concept"),
        ("Result", "USES", "Concept"),
        ("Result", "DEPENDS_ON", "Result"),
        ("Paper", "HAS_SUMMARY", "Summary"),
    }
    assert expected.issubset(set(PATTERNS))

def test_init_cypher_has_new_constraints():
    joined = " ".join(iter_init_statements())
    assert "definition_id" in joined
    assert "result_id" in joined
    assert "summary_id" in joined
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_schema.py -v`
Expected: FAIL (new labels/rels/patterns/constraints missing).

- [ ] **Step 3: Implement**

In `pipeline/schema.py`, append to `NODE_TYPES`: `"Definition"`, `"Result"`, `"Summary"`. Append to `RELATIONSHIP_TYPES`: `"STATES"`, `"DEFINES"`, `"USES"`, `"DEPENDS_ON"`, `"HAS_SUMMARY"`. Append to `PATTERNS`:
```python
    ("Paper",      "STATES",       "Definition"),
    ("Paper",      "STATES",       "Result"),
    ("Definition", "DEFINES",      "Concept"),
    ("Result",     "USES",         "Concept"),
    ("Result",     "DEPENDS_ON",   "Result"),
    ("Paper",      "HAS_SUMMARY",  "Summary"),
```
Insert into the `INIT_CYPHER` string (before the closing `"""`):
```cypher

CREATE CONSTRAINT definition_id IF NOT EXISTS
  FOR (d:Definition) REQUIRE d.id IS UNIQUE;

CREATE CONSTRAINT result_id IF NOT EXISTS
  FOR (r:Result) REQUIRE r.id IS UNIQUE;

CREATE CONSTRAINT summary_id IF NOT EXISTS
  FOR (s:Summary) REQUIRE s.id IS UNIQUE;
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_schema.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pipeline/schema.py tests/test_schema.py
git commit -m "feat(schema): add Definition/Result/Summary nodes, rels, constraints"
```

---

### Task 3: `reset_graph` — snapshot-aware wipe + re-init

**Files:**
- Create: `scripts/reset_graph.py`
- Create: `pipeline/cypher.py` (shared Cypher builders)
- Test: `tests/test_cypher.py`

- [ ] **Step 1: Write failing test for the batched-delete builder**

`tests/test_cypher.py`:
```python
from pipeline.cypher import batched_detach_delete

def test_batched_detach_delete_uses_call_in_transactions():
    q = batched_detach_delete(batch_size=5000)
    assert "MATCH (n)" in q
    assert "DETACH DELETE n" in q
    assert "IN TRANSACTIONS OF 5000 ROWS" in q
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_cypher.py -v`
Expected: FAIL (`pipeline.cypher` missing).

- [ ] **Step 3: Implement `pipeline/cypher.py`**

```python
"""Reusable Cypher fragments."""
from __future__ import annotations


def batched_detach_delete(batch_size: int = 10000) -> str:
    """Delete every node in transaction batches so large graphs don't OOM."""
    return (
        "MATCH (n) "
        f"CALL {{ WITH n DETACH DELETE n }} IN TRANSACTIONS OF {batch_size} ROWS"
    )
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_cypher.py -v`
Expected: PASS

- [ ] **Step 5: Implement `scripts/reset_graph.py`**

```python
"""Wipe the new Neo4j DB and re-assert schema. Requires a manual Aura snapshot first."""
from __future__ import annotations

import os
import sys

from neo4j import GraphDatabase

from pipeline.cypher import batched_detach_delete
from pipeline.schema import iter_init_statements


def main() -> None:
    if "--yes" not in sys.argv:
        print("Refusing to wipe without --yes. Take an Aura snapshot first.")
        sys.exit(1)
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]),
    )
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    with driver.session(database=db) as s:
        before = s.run("MATCH (n) RETURN count(n) AS n").single()["n"]
        print(f"deleting {before} nodes...")
        # IN TRANSACTIONS must be auto-committed: use a top-level run, not execute_write.
        s.run(batched_detach_delete())
        after = s.run("MATCH (n) RETURN count(n) AS n").single()["n"]
        print(f"node count now: {after}")
        for stmt in iter_init_statements():
            s.run(stmt)
        print(f"re-applied {len(iter_init_statements())} schema statements to {db}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Commit**

```bash
git add scripts/reset_graph.py pipeline/cypher.py tests/test_cypher.py
git commit -m "feat: reset_graph script (batched wipe + schema re-init)"
```

---

### Task 4: Dynamic, content-hash partitions

**Files:**
- Modify: `pipeline/partitions.py` (replace static definition)
- Test: `tests/test_partitions.py`

- [ ] **Step 1: Write failing tests**

`tests/test_partitions.py`:
```python
from pipeline.partitions import documents_partitions_def, hash_bytes

def test_hash_bytes_is_stable_sha256_hex():
    h = hash_bytes(b"hello")
    assert h == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

def test_documents_partitions_def_named():
    d = documents_partitions_def()
    assert d.name == "documents"
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_partitions.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement (replace the body of `pipeline/partitions.py`)**

```python
"""Dynamic, content-hash-keyed partitions — one per ingested document."""
from __future__ import annotations

import hashlib

from dagster import DynamicPartitionsDefinition

DOCUMENTS_PARTITION = "documents"


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def documents_partitions_def() -> DynamicPartitionsDefinition:
    return DynamicPartitionsDefinition(name=DOCUMENTS_PARTITION)
```

Note: this removes `data/partitions.json`, `load_partitions`, `paper_ids`, `get_partition`, `partitions_def`. Callers are updated in Tasks 6–7.

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_partitions.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pipeline/partitions.py tests/test_partitions.py
git commit -m "feat(partitions): dynamic content-hash partitions"
```

---

### Task 5: Source discovery + daily schedule

**Files:**
- Create: `pipeline/source.py` (folder scan helpers — pure, testable)
- Create: `pipeline/schedules.py` (daily schedule that registers partitions + requests runs)
- Test: `tests/test_source.py`

- [ ] **Step 1: Write failing tests for the scan helper**

`tests/test_source.py`:
```python
from pathlib import Path
from pipeline.source import list_pdf_files, file_partition_key

def test_list_pdf_files_returns_only_pdfs(tmp_path: Path):
    (tmp_path / "a.pdf").write_bytes(b"x")
    (tmp_path / "b.txt").write_text("y")
    (tmp_path / "c.PDF").write_bytes(b"z")
    found = sorted(p.name for p in list_pdf_files(tmp_path))
    assert found == ["a.pdf", "c.PDF"]

def test_file_partition_key_is_hash_of_contents(tmp_path: Path):
    f = tmp_path / "a.pdf"
    f.write_bytes(b"hello")
    assert file_partition_key(f) == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_source.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/source.py`**

```python
"""Discover source documents. v1: a local folder. Future: same contract for cloud."""
from __future__ import annotations

import os
from pathlib import Path

from pipeline.partitions import hash_bytes


def source_dir() -> Path:
    return Path(os.environ["SOURCE_DIR"]).expanduser()


def list_pdf_files(root: Path) -> list[Path]:
    return [p for p in root.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]


def file_partition_key(path: Path) -> str:
    return hash_bytes(path.read_bytes())
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_source.py -v`
Expected: PASS

- [ ] **Step 5: Implement `pipeline/schedules.py`**

```python
"""Daily schedule: scan SOURCE_DIR, register new dynamic partitions, request runs."""
from __future__ import annotations

from dagster import RunRequest, ScheduleEvaluationContext, schedule

from pipeline.partitions import DOCUMENTS_PARTITION
from pipeline.source import file_partition_key, list_pdf_files, source_dir


@schedule(cron_schedule="0 6 * * *", job_name="ingest_document", execution_timezone="Europe/London")
def daily_ingest_schedule(context: ScheduleEvaluationContext):
    existing = set(context.instance.get_dynamic_partitions(DOCUMENTS_PARTITION))
    requests = []
    new_keys = []
    for pdf in list_pdf_files(source_dir()):
        key = file_partition_key(pdf)
        if key in existing or key in new_keys:
            continue
        new_keys.append(key)
        requests.append(RunRequest(partition_key=key, run_key=key))
    if new_keys:
        context.instance.add_dynamic_partitions(DOCUMENTS_PARTITION, new_keys)
        context.log.info(f"registered {len(new_keys)} new document partitions")
    return requests
```

(`ingest_document` job is defined in Task 16; until then the schedule references a name that will exist by phase end.)

- [ ] **Step 6: Run, verify the helper tests still pass**

Run: `uv run pytest tests/test_source.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add pipeline/source.py pipeline/schedules.py tests/test_source.py
git commit -m "feat: source discovery + daily ingest schedule"
```

---

### Task 6: `raw_blob` asset (PDF → MinIO)

**Files:**
- Create: `pipeline/assets/raw_blob.py`
- Modify: `pipeline/storage.py` (add `RAW_BUCKET`)
- Test: `tests/test_raw_blob.py`

- [ ] **Step 1: Add bucket constant + write failing test**

In `pipeline/storage.py` add: `RAW_BUCKET = "raw"`.

`tests/test_raw_blob.py`:
```python
from unittest.mock import MagicMock
from pipeline.assets.raw_blob import _upload_if_absent

def test_upload_if_absent_skips_when_present():
    s3 = MagicMock()
    s3.head_object.return_value = {}  # exists
    uploaded = _upload_if_absent(s3, "raw", "k.pdf", b"data")
    assert uploaded is False
    s3.put_object.assert_not_called()

def test_upload_if_absent_uploads_when_missing():
    import botocore.exceptions
    s3 = MagicMock()
    s3.head_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "404"}}, "HeadObject")
    uploaded = _upload_if_absent(s3, "raw", "k.pdf", b"data")
    assert uploaded is True
    s3.put_object.assert_called_once()
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_raw_blob.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/assets/raw_blob.py`**

```python
"""raw_blob: ensure this document's PDF is in MinIO, keyed by content hash."""
from __future__ import annotations

import botocore.exceptions
from dagster import MaterializeResult, MetadataValue, asset

from pipeline.partitions import documents_partitions_def
from pipeline.source import file_partition_key, list_pdf_files, source_dir
from pipeline.storage import RAW_BUCKET


def _upload_if_absent(s3, bucket: str, key: str, data: bytes) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return False
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    return True


@asset(partitions_def=documents_partitions_def(), required_resource_keys={"minio"})
def raw_blob(context) -> MaterializeResult:
    key = context.partition_key  # = content hash
    # Find the source file whose hash matches this partition.
    match = next((p for p in list_pdf_files(source_dir()) if file_partition_key(p) == key), None)
    if match is None:
        raise ValueError(f"no source PDF matches partition {key}")
    data = match.read_bytes()
    s3 = context.resources.minio.get_client()
    uploaded = _upload_if_absent(s3, RAW_BUCKET, f"{key}.pdf", data)
    return MaterializeResult(metadata={
        "key": f"{RAW_BUCKET}/{key}.pdf",
        "source_filename": match.name,
        "size_bytes": MetadataValue.int(len(data)),
        "uploaded": uploaded,
    })
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_raw_blob.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pipeline/assets/raw_blob.py pipeline/storage.py tests/test_raw_blob.py
git commit -m "feat(asset): raw_blob uploads source PDF to MinIO by content hash"
```

---

### Task 7: Remove enrichment machinery; rewire `Definitions`

**Files:**
- Delete: `pipeline/assets/legacy_mirror.py`, `pipeline/assets/structural_overlay.py`, `pipeline/assets/pdf_blob.py`, `pipeline/assets/v1_md_blob.py`, `pipeline/assets/kg_extracted.py`, `pipeline/assets/paper_summary.py`, `pipeline/sensors.py`, `data/partitions.json`
- Modify: `pipeline/definitions.py`, `pipeline/jobs.py`
- Modify: `scripts/discover_partitions.py`, `scripts/snapshot_vault.py` (delete — legacy-DB driven)
- Test: `tests/test_definitions.py`

- [ ] **Step 1: Write failing test for the new Definitions**

Replace `tests/test_definitions.py` with:
```python
from pipeline.definitions import defs

def test_defs_has_raw_blob_only_so_far():
    names = {a.key.to_user_string() for a in defs.get_all_asset_specs()}
    assert "raw_blob" in names
    assert "legacy_graph_mirror" not in names
    assert "structural_overlay" not in names
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_definitions.py -v`
Expected: FAIL (old assets still referenced / import errors).

- [ ] **Step 3: Delete the enrichment files**

```bash
git rm pipeline/assets/legacy_mirror.py pipeline/assets/structural_overlay.py \
       pipeline/assets/pdf_blob.py pipeline/assets/v1_md_blob.py \
       pipeline/assets/kg_extracted.py pipeline/assets/paper_summary.py \
       pipeline/sensors.py scripts/discover_partitions.py scripts/snapshot_vault.py
git rm -f data/partitions.json
```

- [ ] **Step 4: Rewrite `pipeline/definitions.py`**

```python
from dagster import Definitions

from pipeline.assets import raw_blob
from pipeline.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env,
)

defs = Definitions(
    assets=[raw_blob.raw_blob],
    resources={
        "neo4j_new": new_neo4j_from_env(),
        "minio": minio_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
    },
)
```

- [ ] **Step 5: Rewrite `pipeline/jobs.py`**

```python
from dagster import AssetSelection, define_asset_job

from pipeline.assets import raw_blob

ingest_document = define_asset_job(
    name="ingest_document",
    selection=AssetSelection.assets(raw_blob.raw_blob),
    description="Per-document ingestion across the asset graph (extended in later tasks).",
)
```

- [ ] **Step 6: Run, verify pass**

Run: `uv run pytest tests/test_definitions.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: delete enrichment assets; rewire Definitions for builder pipeline"
```

---

## Phase 2 — Parse, chunk, embed

**Phase exit state:** each ingested PDF is parsed (Docling, 2-mode) to markdown+LaTeX, chunked equation-safely, embedded, and written as `Chunk` nodes linked to a `Document` in `6b371650`.

### Task 8: `parsed_document` asset (Docling, 2-mode)

**Files:**
- Create: `pipeline/parsing.py` (Docling wrapper + mode routing)
- Create: `pipeline/assets/parsed_document.py`
- Modify: `pipeline/storage.py` (add `PARSED_BUCKET = "parsed"`)
- Test: `tests/test_parsing.py`

- [ ] **Step 1: Write failing tests for mode routing (pure logic)**

`tests/test_parsing.py`:
```python
from pipeline.parsing import needs_ocr, ParseResult

def test_needs_ocr_true_when_no_text_layer():
    assert needs_ocr(extractable_chars=0, page_count=10) is True

def test_needs_ocr_false_for_rich_text_layer():
    assert needs_ocr(extractable_chars=50000, page_count=10) is False

def test_parse_result_flags_empty():
    assert ParseResult(markdown="", mode="text").is_empty is True
    assert ParseResult(markdown="# Title\n\neq $$x$$", mode="text").is_empty is False
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_parsing.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/parsing.py`**

```python
"""Docling parsing with text/OCR mode routing. Output: markdown with LaTeX equations."""
from __future__ import annotations

from dataclasses import dataclass

# Threshold: avg chars/page below this ⇒ assume scanned/image ⇒ OCR.
MIN_CHARS_PER_PAGE = 100


def needs_ocr(extractable_chars: int, page_count: int) -> bool:
    if page_count <= 0:
        return True
    return (extractable_chars / page_count) < MIN_CHARS_PER_PAGE


@dataclass
class ParseResult:
    markdown: str
    mode: str  # "text" | "ocr"

    @property
    def is_empty(self) -> bool:
        return len(self.markdown.strip()) < 20


def parse_pdf(path: str) -> ParseResult:
    """Convert a PDF to markdown+LaTeX. Tries text mode; falls back to OCR/VLM mode."""
    from docling.document_converter import DocumentConverter

    conv = DocumentConverter()
    doc = conv.convert(path).document
    md = doc.export_to_markdown()
    pages = getattr(doc, "num_pages", lambda: 1)() if callable(getattr(doc, "num_pages", None)) else 1
    if needs_ocr(extractable_chars=len(md), page_count=max(pages, 1)):
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.document_converter import PdfFormatOption
        from docling.datamodel.base_models import InputFormat

        opts = PdfPipelineOptions()
        opts.do_ocr = True
        ocr_conv = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=opts)}
        )
        md = ocr_conv.convert(path).document.export_to_markdown()
        return ParseResult(markdown=md, mode="ocr")
    return ParseResult(markdown=md, mode="text")
```

Note: confirm the exact OCR option names against the installed Docling version during implementation (Gate A used the same library); adjust the `PdfPipelineOptions` block if the API differs, keeping the `parse_pdf` signature and `ParseResult` contract identical.

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_parsing.py -v`
Expected: PASS

- [ ] **Step 5: Implement `pipeline/assets/parsed_document.py`**

```python
"""parsed_document: Docling → markdown+LaTeX in MinIO. Quarantine on empty parse."""
from __future__ import annotations

import tempfile
from pathlib import Path

import botocore.exceptions
from dagster import MaterializeResult, MetadataValue, asset

from pipeline.parsing import parse_pdf
from pipeline.partitions import documents_partitions_def
from pipeline.storage import PARSED_BUCKET, RAW_BUCKET


class QuarantineError(Exception):
    """Raised when a document cannot be parsed to usable text."""


@asset(partitions_def=documents_partitions_def(), deps=["raw_blob"],
       required_resource_keys={"minio"})
def parsed_document(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")
    with tempfile.TemporaryDirectory() as td:
        pdf_path = Path(td) / f"{key}.pdf"
        pdf_path.write_bytes(obj["Body"].read())
        result = parse_pdf(str(pdf_path))
    if result.is_empty:
        raise QuarantineError(
            f"{key}: Docling produced empty output (likely image-only or corrupt). "
            "Surfaced, not skipped."
        )
    s3.put_object(Bucket=PARSED_BUCKET, Key=f"{key}.md", Body=result.markdown.encode("utf-8"))
    return MaterializeResult(metadata={
        "key": f"{PARSED_BUCKET}/{key}.md",
        "mode": result.mode,
        "chars": MetadataValue.int(len(result.markdown)),
    })
```

Add `PARSED_BUCKET = "parsed"` to `pipeline/storage.py`. Add the `parsed` and `raw` buckets to the `minio-init` service in `docker-compose.yml` (mirror the existing bucket-creation lines).

- [ ] **Step 6: Register asset + run tests**

Add `parsed_document.parsed_document` to `defs.assets` and `ingest_document` selection. Run: `uv run pytest tests/test_parsing.py tests/test_definitions.py -v` → PASS.

- [ ] **Step 7: Commit**

```bash
git add pipeline/parsing.py pipeline/assets/parsed_document.py pipeline/storage.py \
        pipeline/definitions.py pipeline/jobs.py docker-compose.yml tests/test_parsing.py
git commit -m "feat(asset): parsed_document via Docling with text/OCR routing"
```

---

### Task 9: Equation-aware chunker

**Files:**
- Create: `pipeline/chunking.py`
- Test: `tests/test_chunking.py`

- [ ] **Step 1: Write failing tests**

`tests/test_chunking.py`:
```python
from pipeline.chunking import split_markdown, _segments

def test_segments_keep_display_math_intact():
    md = "para one\n\n$$\na = b\n+ c\n$$\n\npara two"
    segs = _segments(md)
    assert "$$\na = b\n+ c\n$$" in segs

def test_split_never_breaks_a_math_block():
    md = "x" * 100 + "\n\n$$" + ("y" * 50) + "$$\n\n" + "z" * 100
    chunks = split_markdown(md, target=120, overlap=20)
    for c in chunks:
        # a chunk that contains an opening $$ must also contain its closing $$
        assert c.count("$$") % 2 == 0

def test_split_respects_target_size_roughly():
    md = "\n\n".join(["para %d %s" % (i, "w" * 40) for i in range(20)])
    chunks = split_markdown(md, target=200, overlap=20)
    assert len(chunks) > 1
    assert all(len(c) <= 400 for c in chunks)  # never wildly over target
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_chunking.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/chunking.py`**

```python
"""Equation-aware markdown chunker. A LaTeX display block is never split."""
from __future__ import annotations

import re

_MATH_BLOCK = re.compile(r"\$\$.*?\$\$|\\begin\{(\w+)\}.*?\\end\{\1\}", re.DOTALL)


def _segments(md: str) -> list[str]:
    """Split into atomic segments: math blocks stay whole; prose splits on blank lines."""
    segments: list[str] = []
    pos = 0
    for m in _MATH_BLOCK.finditer(md):
        before = md[pos:m.start()]
        for para in re.split(r"\n\s*\n", before):
            if para.strip():
                segments.append(para.strip())
        segments.append(m.group(0))
        pos = m.end()
    for para in re.split(r"\n\s*\n", md[pos:]):
        if para.strip():
            segments.append(para.strip())
    return segments


def split_markdown(md: str, target: int = 4000, overlap: int = 600) -> list[str]:
    """Accumulate atomic segments into ~target-sized chunks with overlap, never splitting math."""
    segs = _segments(md)
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for seg in segs:
        if cur and cur_len + len(seg) > target:
            chunks.append("\n\n".join(cur))
            # build overlap from the tail of the previous chunk
            tail, tlen = [], 0
            for s in reversed(cur):
                if tlen + len(s) > overlap:
                    break
                tail.insert(0, s)
                tlen += len(s)
            cur, cur_len = list(tail), tlen
        cur.append(seg)
        cur_len += len(seg)
    if cur:
        chunks.append("\n\n".join(cur))
    return chunks
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_chunking.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pipeline/chunking.py tests/test_chunking.py
git commit -m "feat: equation-aware markdown chunker"
```

---

### Task 10: `chunks` + `chunk_embeddings` (artifacts — no Neo4j write)

> **Spec §5.5/§5.6:** these assets produce **artifacts only**; `graph_write` (Task 14) is the sole writer of `Chunk` nodes.

**Files:**
- Create: `pipeline/embedding.py` (OpenAI embedding helper)
- Create: `pipeline/assets/chunks.py` (split + embed → MinIO artifact)
- Modify: `pipeline/storage.py` (add `CHUNKS_BUCKET = "chunks"`)
- Test: `tests/test_embedding.py`

- [ ] **Step 1: Write failing test for the embedding helper (mocked)**

`tests/test_embedding.py`:
```python
from unittest.mock import MagicMock
from pipeline.embedding import embed_texts

def test_embed_texts_batches_and_returns_vectors():
    client = MagicMock()
    client.embeddings.create.return_value = MagicMock(
        data=[MagicMock(embedding=[0.1, 0.2]), MagicMock(embedding=[0.3, 0.4])]
    )
    out = embed_texts(client, ["a", "b"], model="text-embedding-3-small")
    assert out == [[0.1, 0.2], [0.3, 0.4]]
    client.embeddings.create.assert_called_once()
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_embedding.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/embedding.py`**

```python
"""OpenAI embedding helper."""
from __future__ import annotations


def embed_texts(client, texts: list[str], model: str) -> list[list[float]]:
    if not texts:
        return []
    resp = client.embeddings.create(model=model, input=texts)
    return [d.embedding for d in resp.data]
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_embedding.py -v`
Expected: PASS

- [ ] **Step 5: Implement `pipeline/assets/chunks.py`** (artifact only)

```python
"""chunks: split parsed markdown + embed → MinIO artifact. No Neo4j write here;
graph_write (Task 14) creates the Chunk nodes from this artifact."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset
from openai import OpenAI

from pipeline.chunking import split_markdown
from pipeline.embedding import embed_texts
from pipeline.partitions import documents_partitions_def
from pipeline.storage import CHUNKS_BUCKET, PARSED_BUCKET


@asset(partitions_def=documents_partitions_def(), deps=["parsed_document"],
       required_resource_keys={"minio", "openai"})
def chunks(context) -> MaterializeResult:
    key = context.partition_key  # document id = file SHA-256
    s3 = context.resources.minio.get_client()
    md = s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.md")["Body"].read().decode("utf-8")
    parts = split_markdown(md)

    cfg = context.resources.openai
    client = OpenAI(api_key=cfg.api_key)
    vectors = embed_texts(client, parts, model=cfg.embedding_model)

    artifact = [{"id": f"{key}:{i}", "position": i, "text": t, "embedding": v}
                for i, (t, v) in enumerate(zip(parts, vectors))]
    s3.put_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(artifact).encode("utf-8"))
    return MaterializeResult(metadata={"chunks": MetadataValue.int(len(artifact))})
```

Add `CHUNKS_BUCKET = "chunks"` to `pipeline/storage.py` and the `chunks` bucket to `docker-compose.yml`'s `minio-init`.

- [ ] **Step 6: Register asset, run unit tests**

Add `chunks.chunks` to `defs.assets` + the `ingest_document` job. Run: `uv run pytest tests/test_embedding.py tests/test_chunking.py -v` → PASS.

- [ ] **Step 7: Commit**

```bash
git add pipeline/embedding.py pipeline/assets/chunks.py pipeline/storage.py \
        pipeline/definitions.py pipeline/jobs.py docker-compose.yml tests/test_embedding.py
git commit -m "feat(asset): chunks + embeddings as MinIO artifact (no Neo4j write)"
```

---

## Phase 3 — Triage/enrich, extract, resolve, write, analyse

**Phase exit state:** full graph build — papers enriched from Semantic Scholar with citations, typed concepts + definitions + results extracted and resolved against existing nodes, and a research-skill-shaped analysis written. The daily schedule drives it end-to-end.

### Task 11: `research_port` (vendored S2 + Cypher) + `triage_metadata`

> **Spec §5.4/§15:** vendor `research_tools.py` logic into `pipeline/research_port.py` (NOT a runtime dependency; source `~/Projects/alethograph/skills/research/scripts/research_tools.py` @ `0f22fa6`). triage computes the **Paper identity** (DOI > arXiv-no-version > title), writes Paper/Author, quarantines duplicate-paper-different-bytes, and **stashes** references for `graph_write`'s CITES backfill — no CITES written here.

**Files:**
- Create: `pipeline/research_port.py` (vendored S2 client + paper-id helpers + Cypher constants)
- Create: `pipeline/assets/triage_metadata.py`
- Modify: `pipeline/storage.py` (add `TRIAGE_BUCKET = "triage"`)
- Test: `tests/test_research_port.py`

- [ ] **Step 1: Write failing tests (pure helpers + mocked HTTP)**

`tests/test_research_port.py`:
```python
from unittest.mock import MagicMock, patch
from pipeline.research_port import (
    compute_paper_id, strip_arxiv_version, lookup_by_arxiv, top_reference_records,
)

def test_compute_paper_id_prefers_doi():
    assert compute_paper_id("10.1/AbC", "2401.1v2", "Title") == "doi:10.1/abc"

def test_compute_paper_id_strips_arxiv_version_when_no_doi():
    assert compute_paper_id(None, "2401.12345v2", "Title") == "arxiv:2401.12345"

def test_compute_paper_id_falls_back_to_normalized_title():
    assert compute_paper_id(None, None, "  Deep   BSDE ") == "title:deep bsde"

def test_strip_arxiv_version():
    assert strip_arxiv_version("2401.12345v3") == "2401.12345"

@patch("pipeline.research_port.requests.get")
def test_lookup_by_arxiv_maps_fields(mock_get):
    mock_get.return_value = MagicMock(status_code=200, json=lambda: {
        "paperId": "abc", "title": "T", "abstract": "A", "year": 2020,
        "citationCount": 5, "influentialCitationCount": 2,
        "tldr": {"text": "tl;dr"}, "authors": [{"name": "X", "authorId": "1"}],
    })
    p = lookup_by_arxiv("2001.00001")
    assert p["s2_id"] == "abc" and p["tldr"] == "tl;dr" and p["authors"][0]["name"] == "X"

def test_top_reference_records_sorts_by_influential():
    raw = [
        {"citedPaper": {"paperId": "a", "title": "A", "externalIds": {"DOI": "d1"},
                        "influentialCitationCount": 1}},
        {"citedPaper": {"paperId": "b", "title": "B", "externalIds": {"ArXiv": "x2"},
                        "influentialCitationCount": 9}},
    ]
    top = top_reference_records(raw, limit=1)
    assert top[0]["s2_id"] == "b" and top[0]["arxiv_id"] == "x2"
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/research_port.py`**

```python
"""Vendored from research_tools.py (~/Projects/alethograph/skills/research/scripts/
research_tools.py @ 0f22fa6). CLI/argparse and the ~/.claude/research-neo4j.json default
connection stripped; callers pass the pipeline's Neo4j driver. NOT a runtime dependency."""
from __future__ import annotations

import re

import requests

BASE = "https://api.semanticscholar.org/graph/v1"
FIELDS = "paperId,title,abstract,year,venue,externalIds,citationCount,influentialCitationCount,tldr,authors"
REF_FIELDS = "title,externalIds,influentialCitationCount"


# --- paper identity (spec §5.4) --------------------------------------------------------
def strip_arxiv_version(arxiv_id: str) -> str:
    return re.sub(r"v\d+$", "", arxiv_id)


def normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", title.strip().lower())


def compute_paper_id(doi: str | None, arxiv_id: str | None, title: str | None) -> str:
    if doi:
        return "doi:" + doi.strip().lower()
    if arxiv_id:
        return "arxiv:" + strip_arxiv_version(arxiv_id.strip().lower())
    if title:
        return "title:" + normalize_title(title)
    raise ValueError("cannot form paper id: no doi/arxiv/title")


# --- Semantic Scholar (vendored) -------------------------------------------------------
def _paper_json_to_record(j: dict) -> dict:
    ext = j.get("externalIds") or {}
    return {
        "s2_id": j.get("paperId"), "title": j.get("title"), "abstract": j.get("abstract"),
        "year": j.get("year"), "venue": j.get("venue"),
        "doi": ext.get("DOI"), "arxiv_id": ext.get("ArXiv"),
        "citation_count": j.get("citationCount"),
        "influential_citation_count": j.get("influentialCitationCount"),
        "tldr": (j.get("tldr") or {}).get("text"),
        "authors": [{"name": a.get("name"), "s2_author_id": a.get("authorId")}
                    for a in (j.get("authors") or [])],
    }


def lookup_by_arxiv(arxiv_id: str) -> dict | None:
    r = requests.get(f"{BASE}/paper/arXiv:{arxiv_id}", params={"fields": FIELDS}, timeout=20)
    return _paper_json_to_record(r.json()) if r.status_code == 200 else None


def lookup_by_doi(doi: str) -> dict | None:
    r = requests.get(f"{BASE}/paper/DOI:{doi}", params={"fields": FIELDS}, timeout=20)
    return _paper_json_to_record(r.json()) if r.status_code == 200 else None


def references(s2_id: str) -> list[dict]:
    r = requests.get(f"{BASE}/paper/{s2_id}/references",
                     params={"fields": REF_FIELDS, "limit": 100}, timeout=20)
    return r.json().get("data", []) if r.status_code == 200 else []


def top_reference_records(raw_refs: list[dict], limit: int = 3) -> list[dict]:
    recs = []
    for ref in raw_refs:
        cp = ref.get("citedPaper") or {}
        ext = cp.get("externalIds") or {}
        recs.append({
            "s2_id": cp.get("paperId"),
            "doi": ext.get("DOI"),
            "arxiv_id": ext.get("ArXiv"),
            "title_norm": normalize_title(cp["title"]) if cp.get("title") else None,
            "influential_count": cp.get("influentialCitationCount") or 0,
        })
    return sorted(recs, key=lambda r: r["influential_count"], reverse=True)[:limit]


# --- vendored Cypher (db-add-paper / db-cite-paper) ------------------------------------
WRITE_PAPER = """
MERGE (p:Paper {id: $id})
SET p.title=$title, p.year=$year, p.arxiv_id=$arxiv_id, p.doi=$doi, p.s2_id=$s2_id,
    p.abstract=$abstract, p.tldr=$tldr, p.citation_count=$citation_count,
    p.influential_citation_count=$influential_citation_count, p.document_id=$document_id
WITH p
UNWIND $authors AS author
  MERGE (a:Author {name: author.name})
  SET a.s2_author_id = coalesce(author.s2_author_id, a.s2_author_id)
  MERGE (a)-[:AUTHORED]->(p)
"""

# Used by graph_write (forward pass). The cited match is by any present identifier.
CITE_FORWARD = """
MATCH (citing:Paper {id: $citing_id})
MATCH (cited:Paper)
  WHERE ($s2_id  IS NOT NULL AND cited.s2_id  = $s2_id)
     OR ($doi    IS NOT NULL AND cited.doi    = $doi)
     OR ($arxiv  IS NOT NULL AND cited.arxiv_id = $arxiv)
MERGE (citing)-[:CITES]->(cited)
"""
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS

- [ ] **Step 5: Implement `pipeline/assets/triage_metadata.py`**

```python
"""triage_metadata: confirm paper, establish Paper identity, S2-enrich, write Paper+Author,
quarantine duplicate-paper-different-bytes, and stash references for graph_write's backfill."""
from __future__ import annotations

import json

from dagster import MaterializeResult, asset
from openai import OpenAI

from pipeline import research_port as rp
from pipeline.assets.parsed_document import QuarantineError
from pipeline.partitions import documents_partitions_def
from pipeline.storage import PARSED_BUCKET, TRIAGE_BUCKET

FRONTMATTER_PROMPT = (
    "You are extracting bibliographic metadata from the first page of a document. "
    "Return strict JSON: {\"is_paper\": bool, \"title\": str, \"authors\": [str], "
    "\"year\": int|null, \"arxiv_id\": str|null, \"doi\": str|null}. "
    "is_paper is false for non-papers (slides, notes, books)."
)
DUP_CHECK = "MATCH (p:Paper {id:$pid}) RETURN p.document_id AS doc"


def _extract_frontmatter(client, model: str, head: str) -> dict:
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": FRONTMATTER_PROMPT},
                  {"role": "user", "content": head[:6000]}],
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


@asset(partitions_def=documents_partitions_def(), deps=["parsed_document"],
       required_resource_keys={"minio", "neo4j_new", "openai"})
def triage_metadata(context) -> MaterializeResult:
    key = context.partition_key  # document id
    s3 = context.resources.minio.get_client()
    md = s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.md")["Body"].read().decode("utf-8")

    cfg = context.resources.openai
    client = OpenAI(api_key=cfg.api_key)
    fm = _extract_frontmatter(client, cfg.extraction_model, md)
    if not fm.get("is_paper"):
        context.log.warning(f"{key}: triage says not a paper; stopping branch")
        return MaterializeResult(metadata={"is_paper": False})

    rec = None
    if fm.get("arxiv_id"):
        rec = rp.lookup_by_arxiv(fm["arxiv_id"])
    if rec is None and fm.get("doi"):
        rec = rp.lookup_by_doi(fm["doi"])
    rec = rec or {}

    doi = rec.get("doi") or fm.get("doi")
    arxiv = rec.get("arxiv_id") or fm.get("arxiv_id")
    title = rec.get("title") or fm.get("title")
    paper_id = rp.compute_paper_id(doi, arxiv, title)

    paper = {
        "id": paper_id, "document_id": key, "title": title,
        "year": rec.get("year") or fm.get("year"), "arxiv_id": arxiv, "doi": doi,
        "s2_id": rec.get("s2_id"), "abstract": rec.get("abstract"), "tldr": rec.get("tldr"),
        "citation_count": rec.get("citation_count"),
        "influential_citation_count": rec.get("influential_citation_count"),
        "authors": rec.get("authors") or [{"name": n, "s2_author_id": None}
                                          for n in (fm.get("authors") or [])],
    }

    new = context.resources.neo4j_new
    with new.get_driver().session(database=new.database) as s:
        row = s.run(DUP_CHECK, pid=paper_id).single()
        if row and row["doc"] and row["doc"] != key:
            raise QuarantineError(
                f"{key}: duplicate-paper-different-bytes — paper {paper_id} already "
                f"ingested from document {row['doc']}")
        s.run(rp.WRITE_PAPER, **paper)

    refs = rp.top_reference_records(rp.references(rec["s2_id"]), limit=3) if rec.get("s2_id") else []
    identifiers = {"s2_id": rec.get("s2_id"), "doi": doi, "arxiv_id": arxiv,
                   "title_norm": rp.normalize_title(title) if title else None}
    s3.put_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.json",
                  Body=json.dumps({"paper_id": paper_id, "s2_id": rec.get("s2_id"),
                                   "identifiers": identifiers, "references": refs}).encode("utf-8"))
    return MaterializeResult(metadata={"is_paper": True, "paper_id": paper_id,
                                       "references": len(refs)})
```

Add `TRIAGE_BUCKET = "triage"` to `pipeline/storage.py` + the bucket to `docker-compose.yml`. `QuarantineError` comes from Task 8's `parsed_document` (or move it to a shared `pipeline/errors.py` and import from both).

- [ ] **Step 6: Register, run unit tests**

Add `triage_metadata` to `defs.assets` + the `ingest_document` job. Run: `uv run pytest tests/test_research_port.py -v` → PASS.

- [ ] **Step 7: Commit**

```bash
git add pipeline/research_port.py pipeline/assets/triage_metadata.py pipeline/storage.py \
        pipeline/definitions.py pipeline/jobs.py docker-compose.yml tests/test_research_port.py
git commit -m "feat(asset): vendored research_port + triage_metadata (paper identity, dedup, refs stash)"
```

---

### Task 12: `extracted_graph` — typed concepts, definitions, results

**Files:**
- Create: `pipeline/extraction.py` (prompt, JSON parsing, schema validation)
- Create: `pipeline/assets/extracted_graph.py`
- Test: `tests/test_extraction.py`

- [ ] **Step 1: Write failing tests for schema validation + concept typing (pure)**

`tests/test_extraction.py`:
```python
from pipeline.extraction import validate_triples, ExtractionResult, parse_extraction

def test_validate_triples_drops_illegal_patterns():
    triples = [("Paper", "DISCUSSES", "Concept"), ("Paper", "AUTHORED", "Concept")]
    assert validate_triples(triples) == [("Paper", "DISCUSSES", "Concept")]

def test_parse_extraction_reads_concepts_with_kind():
    payload = {
        "concepts": [{"name": "Wrong-Way Risk", "kind": "concept"},
                     {"name": "Deep BSDE Solver", "kind": "method"}],
        "definitions": [{"term": "WWR", "statement": "$P(\\tau)$ ..."}],
        "results": [{"name": "Thm 1", "kind": "theorem", "statement": "$x=y$"}],
    }
    r = parse_extraction(payload)
    assert isinstance(r, ExtractionResult)
    assert ("Wrong-Way Risk", "concept") in [(c.name, c.kind) for c in r.concepts]
    assert r.results[0].kind == "theorem"

def test_parse_extraction_rejects_unknown_result_kind():
    import pytest
    with pytest.raises(ValueError):
        parse_extraction({"results": [{"name": "x", "kind": "conjecture", "statement": "y"}]})
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_extraction.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/extraction.py`**

```python
"""LLM extraction against the alethograph schema. Prompts ported from the research skill
+ spec/03-extraction-prompts.md scaffold; alethograph label vocabulary + few-shots."""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from pipeline.schema import PATTERNS

VALID_RESULT_KINDS = {"theorem", "lemma", "proposition", "corollary"}
VALID_CONCEPT_KINDS = {"concept", "method"}

SYSTEM_PROMPT = """You are an information-extraction assistant for academic papers in \
quantitative finance / stochastics. From the chunk, extract:
- concepts: 3-7 major theoretical ideas/objects/frameworks (kind="concept") or implementable \
algorithms/techniques (kind="method"). Each must be self-contained.
- definitions: formal definitions, with the term and the statement (preserve LaTeX).
- results: theorems/lemmas/propositions/corollaries, with name (e.g. "Theorem 3.2"), kind, \
and statement (preserve LaTeX).
Return STRICT JSON: {"concepts":[{"name","kind"}],"definitions":[{"term","statement"}],\
"results":[{"name","kind","statement"}]}. Emit nothing not asserted by the text."""


@dataclass
class Concept:
    name: str
    kind: str  # concept | method


@dataclass
class Definition:
    term: str
    statement: str


@dataclass
class Result:
    name: str
    kind: str  # theorem | lemma | proposition | corollary
    statement: str


@dataclass
class ExtractionResult:
    concepts: list[Concept] = field(default_factory=list)
    definitions: list[Definition] = field(default_factory=list)
    results: list[Result] = field(default_factory=list)


def validate_triples(triples: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    allowed = set(PATTERNS)
    return [t for t in triples if t in allowed]


def parse_extraction(payload: dict) -> ExtractionResult:
    concepts = []
    for c in payload.get("concepts", []):
        kind = c.get("kind", "concept")
        if kind not in VALID_CONCEPT_KINDS:
            raise ValueError(f"bad concept kind: {kind}")
        concepts.append(Concept(name=c["name"].strip(), kind=kind))
    definitions = [Definition(term=d["term"].strip(), statement=d["statement"])
                   for d in payload.get("definitions", [])]
    results = []
    for r in payload.get("results", []):
        if r.get("kind") not in VALID_RESULT_KINDS:
            raise ValueError(f"bad result kind: {r.get('kind')}")
        results.append(Result(name=r.get("name", ""), kind=r["kind"], statement=r["statement"]))
    return ExtractionResult(concepts=concepts, definitions=definitions, results=results)


def extract_from_chunk(client, model: str, chunk: str) -> ExtractionResult:
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": SYSTEM_PROMPT},
                  {"role": "user", "content": chunk[:12000]}],
        response_format={"type": "json_object"},
    )
    return parse_extraction(json.loads(resp.choices[0].message.content))


def merge_results(parts: list[ExtractionResult]) -> ExtractionResult:
    seen_c, concepts = set(), []
    for p in parts:
        for c in p.concepts:
            if c.name.lower() not in seen_c:
                seen_c.add(c.name.lower())
                concepts.append(c)
    definitions = [d for p in parts for d in p.definitions]
    results = [r for p in parts for r in p.results]
    return ExtractionResult(concepts=concepts, definitions=definitions, results=results)
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_extraction.py -v`
Expected: PASS

- [ ] **Step 5: Implement `pipeline/assets/extracted_graph.py`**

```python
"""extracted_graph: run extraction over this paper's chunk artifact; emit candidate entities.
Reads the chunk artifact (not Neo4j — chunks are artifact-only now). Output stashed in MinIO
JSON feeds resolved_entities + graph_write."""
from __future__ import annotations

import json
from dataclasses import asdict

from dagster import MaterializeResult, MetadataValue, asset
from openai import OpenAI

from pipeline.extraction import extract_from_chunk, merge_results
from pipeline.partitions import documents_partitions_def
from pipeline.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET


@asset(partitions_def=documents_partitions_def(), deps=["chunks", "triage_metadata"],
       required_resource_keys={"minio", "openai"})
def extracted_graph(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    chunk_rows = json.loads(s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.json")["Body"].read())
    texts = [c["text"] for c in sorted(chunk_rows, key=lambda c: c["position"]) if c["text"]]

    cfg = context.resources.openai
    client = OpenAI(api_key=cfg.api_key)
    merged = merge_results([extract_from_chunk(client, cfg.extraction_model, t) for t in texts])

    payload = {
        "concepts": [asdict(c) for c in merged.concepts],
        "definitions": [asdict(d) for d in merged.definitions],
        "results": [asdict(r) for r in merged.results],
    }
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={
        "concepts": MetadataValue.int(len(merged.concepts)),
        "definitions": MetadataValue.int(len(merged.definitions)),
        "results": MetadataValue.int(len(merged.results)),
    })
```

Note: add `EXTRACTED_BUCKET = "extracted"` to `pipeline/storage.py` (alongside `CHUNKS_BUCKET`) and add the `extracted` bucket to the `minio-init` service in `docker-compose.yml`.

- [ ] **Step 6: Register, run unit tests**

Add to `defs.assets` + job. Run: `uv run pytest tests/test_extraction.py -v` → PASS.

- [ ] **Step 7: Commit**

```bash
git add pipeline/extraction.py pipeline/assets/extracted_graph.py pipeline/storage.py \
        pipeline/definitions.py pipeline/jobs.py docker-compose.yml tests/test_extraction.py
git commit -m "feat(asset): extracted_graph (typed concepts, definitions, results)"
```

---

### Task 13: `resolved_entities` (decide-only) + pgvector resolver

> **Spec §5.8/§7:** `resolved_entities` *decides only* — NN-query pgvector + write the decision row. It does **not** write Neo4j or upsert entity embeddings; `graph_write` (Task 14) owns both (single-writer rule). The resolved artifact carries each candidate's embedding so `graph_write` can upsert it for newly-created canonicals.

**Files:**
- Create: `pipeline/resolver.py` (decision logic + pgvector store)
- Modify: `pipeline/resources.py` (add `PostgresResource` + factory; register in `definitions.py`)
- Create: `scripts/init_postgres.py` (pgvector extension + tables)
- Test: `tests/test_resolver.py`

- [ ] **Step 1: Write failing tests for the decision logic (pure)**

`tests/test_resolver.py`:
```python
from pipeline.resolver import decide, Decision

def test_decide_merges_above_high():
    assert decide(0.95, high=0.9, low=0.6) == Decision.MERGE

def test_decide_creates_below_low():
    assert decide(0.4, high=0.9, low=0.6) == Decision.CREATE

def test_decide_ambiguous_band_creates_and_flags():
    assert decide(0.75, high=0.9, low=0.6) == Decision.CREATE_FLAGGED
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_resolver.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement decision logic in `pipeline/resolver.py`**

```python
"""Entity resolution: conservative thresholds, split-when-unsure, decisions recorded."""
from __future__ import annotations

import enum


class Decision(enum.Enum):
    MERGE = "merge"
    CREATE = "create"
    CREATE_FLAGGED = "create_flagged"  # ambiguous band → create new but flag for review


def decide(score: float, high: float = 0.90, low: float = 0.60) -> Decision:
    if score >= high:
        return Decision.MERGE
    if score < low:
        return Decision.CREATE
    return Decision.CREATE_FLAGGED
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_resolver.py -v`
Expected: PASS

- [ ] **Step 5: Add the pgvector store + Postgres resource**

Append to `pipeline/resolver.py`:
```python
def nearest(cur, label: str, embedding: list[float]) -> tuple[str, float] | None:
    """Return (canonical_name, cosine_similarity) of the closest same-label entity, or None."""
    cur.execute(
        "SELECT canonical, 1 - (embedding <=> %s::vector) AS sim "
        "FROM entity_embeddings WHERE label = %s ORDER BY embedding <=> %s::vector LIMIT 1",
        (embedding, label, embedding),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def record_decision(cur, candidate: str, matched_to: str | None, label: str,
                    score: float, action: str, run_id: str) -> None:
    cur.execute(
        "INSERT INTO resolution_decisions "
        "(candidate, matched_to, label, score, action, run_id) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (candidate, matched_to, label, score, action, run_id),
    )


def upsert_embedding(cur, canonical: str, label: str, embedding: list[float]) -> None:
    cur.execute(
        "INSERT INTO entity_embeddings (canonical, label, embedding) VALUES (%s,%s,%s::vector) "
        "ON CONFLICT (canonical, label) DO UPDATE SET embedding = EXCLUDED.embedding",
        (canonical, label, embedding),
    )
```

Add to `pipeline/resources.py`:
```python
class PostgresResource(ConfigurableResource):
    """Postgres (shares the Dagster metadata instance) for pgvector entity resolution."""
    dsn: str

    def connect(self):
        import psycopg
        return psycopg.connect(self.dsn)


def postgres_from_env() -> "PostgresResource":
    return PostgresResource(dsn=os.environ["RESOLVER_POSTGRES_DSN"])
```
Register `"postgres": postgres_from_env()` in `definitions.py` resources.

- [ ] **Step 6: Implement `scripts/init_postgres.py`**

```python
"""Create the pgvector extension + resolver tables."""
from __future__ import annotations

import os
import psycopg

DDL = [
    "CREATE EXTENSION IF NOT EXISTS vector",
    "CREATE TABLE IF NOT EXISTS entity_embeddings ("
    " canonical text, label text, embedding vector(1536),"
    " PRIMARY KEY (canonical, label))",
    "CREATE TABLE IF NOT EXISTS resolution_decisions ("
    " id bigserial PRIMARY KEY, candidate text, matched_to text, label text,"
    " score double precision, action text, run_id text, ts timestamptz DEFAULT now())",
    "CREATE TABLE IF NOT EXISTS alias_map ("
    " alias text, label text, canonical text, PRIMARY KEY (alias, label))",
    "CREATE TABLE IF NOT EXISTS pending_citations ("
    " id bigserial PRIMARY KEY, citing_paper_id text NOT NULL,"
    " ref_doi text, ref_arxiv_id text, ref_title_norm text, ref_s2_id text,"
    " influential_count int DEFAULT 0, created_ts timestamptz DEFAULT now(),"
    " resolved bool DEFAULT false)",
]


def main() -> None:
    with psycopg.connect(os.environ["RESOLVER_POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:
            for stmt in DDL:
                cur.execute(stmt)
        conn.commit()
    print("resolver schema ready")


if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Implement `pipeline/assets/resolved_entities.py`**

```python
"""resolved_entities: DECIDE ONLY. For each candidate Concept, NN-query pgvector and record the
decision row in Postgres. Writes no Neo4j and upserts no embedding — graph_write owns both
(single-writer, spec §7). Emits resolved concepts (with embeddings) for graph_write."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset
from openai import OpenAI

from pipeline.embedding import embed_texts
from pipeline.partitions import documents_partitions_def
from pipeline.resolver import Decision, decide, nearest, record_decision
from pipeline.storage import EXTRACTED_BUCKET


@asset(partitions_def=documents_partitions_def(), deps=["extracted_graph"],
       required_resource_keys={"minio", "openai", "postgres"})
def resolved_entities(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    payload = json.loads(s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.json")["Body"].read())

    cfg = context.resources.openai
    client = OpenAI(api_key=cfg.api_key)
    concepts = payload.get("concepts", [])
    names = [c["name"] for c in concepts]
    vecs = embed_texts(client, names, model=cfg.embedding_model)

    resolved = []
    counts = {"merge": 0, "create": 0, "create_flagged": 0}
    with context.resources.postgres.connect() as conn:
        with conn.cursor() as cur:
            for c, v in zip(concepts, vecs):
                hit = nearest(cur, "Concept", v)
                if hit is None:
                    action, canonical, score = Decision.CREATE, c["name"], 0.0
                else:
                    matched, score = hit
                    action = decide(score)
                    canonical = matched if action == Decision.MERGE else c["name"]
                counts[action.value] += 1
                record_decision(cur, c["name"], canonical if action == Decision.MERGE else None,
                                "Concept", score, action.value, context.run_id)
                resolved.append({
                    "name": canonical, "kind": c["kind"], "action": action.value,
                    # graph_write upserts this embedding for newly-created canonicals.
                    "embedding": v if action != Decision.MERGE else None,
                })
        conn.commit()  # ONLY decision rows are written here — no Neo4j, no embedding upsert.

    payload["concepts"] = resolved
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={k: MetadataValue.int(v) for k, v in counts.items()})
```

- [ ] **Step 8: Register, run unit tests**

Add asset + resource. Run: `uv run pytest tests/test_resolver.py -v` → PASS.

- [ ] **Step 9: Commit**

```bash
git add pipeline/resolver.py pipeline/resources.py pipeline/assets/resolved_entities.py \
        scripts/init_postgres.py pipeline/definitions.py pipeline/jobs.py tests/test_resolver.py
git commit -m "feat(asset): resolved_entities (pgvector resolver + decision trail)"
```

---

### Task 14: `graph_write` — sole writer (Chunks, Concepts+pgvector, Def/Result, CITES)

> **Spec §5.9:** the single writer of the *derived* graph. Reads the chunk / resolved / triage artifacts; writes Chunk nodes, Concept nodes **+ pgvector embedding upsert for new canonicals (one unit)**, Definition/Result nodes (paper-local content-hash ids), and CITES (forward + backward via `pending_citations`). Needs Neo4j **and** Postgres.

**Files:**
- Create: `pipeline/assets/graph_write.py`
- Test: `tests/test_graph_write.py`

- [ ] **Step 1: Write failing tests for the pure builders**

`tests/test_graph_write.py`:
```python
from pipeline.assets.graph_write import (
    concept_rows, definition_rows, result_rows, normalize_statement, def_id,
)

def test_concept_rows_carry_kind_tag():
    rows = concept_rows([{"name": "WWR", "kind": "method", "action": "create", "embedding": [0.1]}])
    assert rows[0]["name"] == "WWR" and rows[0]["tags"] == ["method"]

def test_normalize_statement_collapses_and_lowercases():
    assert normalize_statement("  Let  $X$\n be ") == "let $x$ be"

def test_def_id_is_deterministic_and_paper_local():
    a = def_id("paper1", "Let $X$ be a martingale.")
    b = def_id("paper1", "let   $x$   be a martingale. ")
    assert a == b and a.startswith("paper1:def:")

def test_result_rows_use_kind_in_id():
    rows = result_rows("p1", [{"name": "Thm 1", "kind": "theorem", "statement": "$x=y$"}])
    assert rows[0]["id"].startswith("p1:theorem:")
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_graph_write.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/assets/graph_write.py`**

```python
"""graph_write: the SOLE writer of the derived graph (spec §5.9). Reads chunk/resolved/triage
artifacts and writes, all idempotent MERGE: Chunk nodes (+emb), Concept nodes (+ pgvector
embedding upsert for new canonicals, as one unit), Definition/Result nodes (paper-local
content-hash ids), and CITES (forward + backward backfill via pending_citations)."""
from __future__ import annotations

import hashlib
import json
import re

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.partitions import documents_partitions_def
from pipeline.resolver import upsert_embedding
from pipeline.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET, TRIAGE_BUCKET


# --- pure builders ------------------------------------------------------------
def normalize_statement(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _hash12(s: str) -> str:
    return hashlib.sha1(normalize_statement(s).encode("utf-8")).hexdigest()[:12]


def def_id(paper_id: str, statement: str) -> str:
    return f"{paper_id}:def:{_hash12(statement)}"


def result_id(paper_id: str, kind: str, statement: str) -> str:
    return f"{paper_id}:{kind}:{_hash12(statement)}"


def concept_rows(concepts: list[dict]) -> list[dict]:
    return [{"name": c["name"], "tags": [c["kind"]]} for c in concepts]


def definition_rows(paper_id: str, defs: list[dict]) -> list[dict]:
    return [{"id": def_id(paper_id, d["statement"]), "term": d["term"],
             "statement": d["statement"]} for d in defs]


def result_rows(paper_id: str, results: list[dict]) -> list[dict]:
    return [{"id": result_id(paper_id, r["kind"], r["statement"]), "name": r.get("name", ""),
             "kind": r["kind"], "statement": r["statement"]} for r in results]


# --- Cypher -------------------------------------------------------------------
WRITE_CHUNKS = """
MERGE (d:Document {id:$doc_id}) SET d.paper_id = $paper_id
WITH d
UNWIND $rows AS row
  MERGE (c:Chunk {id: row.id})
  SET c.text = row.text, c.position = row.position, c.embedding = row.embedding
  MERGE (c)-[:BELONGS_TO]->(d)
"""

WRITE_CONCEPTS = """
MATCH (p:Paper {id:$paper_id})
UNWIND $rows AS row
  MERGE (c:Concept {name: row.name})
  SET c.tags = row.tags
  MERGE (p)-[:DISCUSSES]->(c)
  MERGE (c)-[:DERIVED_FROM]->(p)
"""

WRITE_DEFINITIONS = """
MATCH (p:Paper {id:$paper_id})
UNWIND $rows AS row
  MERGE (d:Definition {id: row.id})
  SET d.term = row.term, d.statement = row.statement
  MERGE (p)-[:STATES]->(d)
"""

WRITE_RESULTS = """
MATCH (p:Paper {id:$paper_id})
UNWIND $rows AS row
  MERGE (r:Result {id: row.id})
  SET r.name = row.name, r.kind = row.kind, r.statement = row.statement
  MERGE (p)-[:STATES]->(r)
"""

FIND_CITED = """
MATCH (cited:Paper)
  WHERE ($s2 IS NOT NULL AND cited.s2_id=$s2)
     OR ($doi IS NOT NULL AND cited.doi=$doi)
     OR ($arxiv IS NOT NULL AND cited.arxiv_id=$arxiv)
RETURN cited.id AS id LIMIT 1
"""
MERGE_CITES = "MATCH (a:Paper {id:$citing}),(b:Paper {id:$cited}) MERGE (a)-[:CITES]->(b)"

_MATCH_PENDING = ("ref_s2_id=%s OR ref_doi=%s OR ref_arxiv_id=%s OR ref_title_norm=%s")


@asset(partitions_def=documents_partitions_def(),
       deps=["resolved_entities", "chunks", "triage_metadata"],
       required_resource_keys={"minio", "neo4j_new", "postgres"})
def graph_write(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    resolved = json.loads(s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json")["Body"].read())
    chunks = json.loads(s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.json")["Body"].read())
    triage = json.loads(s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.json")["Body"].read())

    paper_id = triage["paper_id"]
    ids = triage.get("identifiers", {})
    concepts = resolved.get("concepts", [])
    crows = concept_rows(concepts)
    drows = definition_rows(paper_id, resolved.get("definitions", []))
    rrows = result_rows(paper_id, resolved.get("results", []))

    new = context.resources.neo4j_new
    with new.get_driver().session(database=new.database) as s:
        s.run(WRITE_CHUNKS, doc_id=key, paper_id=paper_id, rows=chunks)
        s.run(WRITE_CONCEPTS, paper_id=paper_id, rows=crows)
        s.run(WRITE_DEFINITIONS, paper_id=paper_id, rows=drows)
        s.run(WRITE_RESULTS, paper_id=paper_id, rows=rrows)

        with context.resources.postgres.connect() as conn:
            with conn.cursor() as cur:
                # pgvector embedding upsert for newly-created Concepts (one unit with the node)
                for c in concepts:
                    if c.get("action") != "merge" and c.get("embedding") is not None:
                        upsert_embedding(cur, c["name"], "Concept", c["embedding"])
                # forward: this paper → its references
                for ref in triage.get("references", []):
                    found = s.run(FIND_CITED, s2=ref.get("s2_id"), doi=ref.get("doi"),
                                  arxiv=ref.get("arxiv_id")).single()
                    if found:
                        s.run(MERGE_CITES, citing=paper_id, cited=found["id"])
                    else:
                        cur.execute(
                            "INSERT INTO pending_citations (citing_paper_id, ref_doi, "
                            "ref_arxiv_id, ref_title_norm, ref_s2_id, influential_count) "
                            "VALUES (%s,%s,%s,%s,%s,%s)",
                            (paper_id, ref.get("doi"), ref.get("arxiv_id"),
                             ref.get("title_norm"), ref.get("s2_id"),
                             ref.get("influential_count", 0)))
                # backward: prior pending refs that point at THIS paper
                params = (ids.get("s2_id"), ids.get("doi"), ids.get("arxiv_id"), ids.get("title_norm"))
                cur.execute(
                    f"SELECT citing_paper_id FROM pending_citations WHERE NOT resolved AND ({_MATCH_PENDING})",
                    params)
                for (citing_id,) in cur.fetchall():
                    s.run(MERGE_CITES, citing=citing_id, cited=paper_id)
                cur.execute(
                    f"UPDATE pending_citations SET resolved=true WHERE NOT resolved AND ({_MATCH_PENDING})",
                    params)
            conn.commit()
    return MaterializeResult(metadata={
        "chunks": MetadataValue.int(len(chunks)),
        "concepts": MetadataValue.int(len(crows)),
        "definitions": MetadataValue.int(len(drows)),
        "results": MetadataValue.int(len(rrows)),
    })
```

Note: `Definition DEFINES Concept` / `Result USES Concept` edges (schema §6) are a **deferred best-effort follow-up** (term→concept matching); v1 writes the nodes + `STATES` edges only. The `triage` artifact must include an `identifiers` dict (`s2_id`/`doi`/`arxiv_id`/`title_norm`) — add it to Task 11's stash (the `references` already carry per-ref identifiers).

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_graph_write.py -v`
Expected: PASS

- [ ] **Step 5: Register + commit**

Add asset to `defs.assets` + the `ingest_document` job.
```bash
git add pipeline/assets/graph_write.py pipeline/definitions.py pipeline/jobs.py tests/test_graph_write.py
git commit -m "feat(asset): graph_write — sole writer (chunks, concepts+pgvector, def/result, CITES backfill)"
```

---

### Task 15: `paper_analysis` — research-skill template

**Files:**
- Create: `pipeline/analysis.py` (standing brief + prompt + JSON shape)
- Create: `pipeline/assets/paper_analysis.py`
- Modify: `pipeline/storage.py` (add `ANALYSIS_BUCKET = "analysis"`)
- Test: `tests/test_analysis.py`

- [ ] **Step 1: Write failing tests for the analysis shape (pure)**

`tests/test_analysis.py`:
```python
from pipeline.analysis import ANALYSIS_FIELDS, validate_analysis

def test_analysis_fields_match_research_skill_template():
    assert ANALYSIS_FIELDS == [
        "summary", "key_contributions", "methodology", "key_findings",
        "important_references", "atomic_notes", "definitions", "results",
    ]

def test_validate_analysis_requires_all_fields():
    import pytest
    with pytest.raises(ValueError):
        validate_analysis({"summary": "x"})
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run pytest tests/test_analysis.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `pipeline/analysis.py`**

```python
"""Per-paper analysis matching the research skill's note template. Math kept as LaTeX."""
from __future__ import annotations

# Fixed standing brief replaces the skill's interactive per-paper learning goal (spec §15).
STANDING_BRIEF = (
    "Summarise for a quantitative-finance researcher tracking XVA, stochastic analysis, "
    "and machine-learning methods. Emphasise mathematical contributions and how results connect."
)

ANALYSIS_FIELDS = [
    "summary", "key_contributions", "methodology", "key_findings",
    "important_references", "atomic_notes", "definitions", "results",
]

SYSTEM_PROMPT = (
    "Produce a structured analysis of this paper as STRICT JSON with keys: "
    + ", ".join(ANALYSIS_FIELDS) + ". "
    "summary: 2-3 paragraphs. key_contributions/key_findings/important_references/atomic_notes: "
    "arrays of strings. definitions/results: arrays of objects with statements in LaTeX. "
    f"Audience brief: {STANDING_BRIEF}"
)


def validate_analysis(obj: dict) -> dict:
    missing = [f for f in ANALYSIS_FIELDS if f not in obj]
    if missing:
        raise ValueError(f"analysis missing fields: {missing}")
    return obj
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_analysis.py -v`
Expected: PASS

- [ ] **Step 5: Implement `pipeline/assets/paper_analysis.py`**

```python
"""paper_analysis: Claude writes the structured analysis; stored as Summary node + MinIO JSON."""
from __future__ import annotations

import json

from dagster import MaterializeResult, asset

from pipeline.analysis import SYSTEM_PROMPT, validate_analysis
from pipeline.partitions import documents_partitions_def
from pipeline.storage import ANALYSIS_BUCKET, PARSED_BUCKET, TRIAGE_BUCKET

WRITE_SUMMARY = """
MATCH (p:Paper {id:$paper_id})
MERGE (sm:Summary {id: $paper_id})
SET sm.json = $json
MERGE (p)-[:HAS_SUMMARY]->(sm)
"""


@asset(partitions_def=documents_partitions_def(),
       deps=["parsed_document", "extracted_graph", "triage_metadata"],
       required_resource_keys={"minio", "neo4j_new", "anthropic"})
def paper_analysis(context) -> MaterializeResult:
    key = context.partition_key  # document id
    s3 = context.resources.minio.get_client()
    md = s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.md")["Body"].read().decode("utf-8")
    paper_id = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.json")["Body"].read())["paper_id"]

    client = context.resources.anthropic.get_client()
    msg = client.messages.create(
        model=context.resources.anthropic.summary_model,
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": md[:120000]}],
    )
    raw = msg.content[0].text
    analysis = validate_analysis(json.loads(raw))

    s3.put_object(Bucket=ANALYSIS_BUCKET, Key=f"{key}.json",
                  Body=json.dumps(analysis).encode("utf-8"))
    new = context.resources.neo4j_new
    with new.get_driver().session(database=new.database) as s:
        s.run(WRITE_SUMMARY, paper_id=paper_id, json=json.dumps(analysis))
    return MaterializeResult(metadata={"analysis_key": f"{ANALYSIS_BUCKET}/{key}.json",
                                       "paper_id": paper_id})
```

Add `ANALYSIS_BUCKET = "analysis"` to `pipeline/storage.py` + the bucket in `docker-compose.yml`. (Claude may wrap JSON in prose; if so, strip to the outermost `{...}` before `json.loads` — add that in implementation and a unit test for the stripper.)

- [ ] **Step 6: Register, run unit tests**

Add asset to `defs.assets` + job. Run: `uv run pytest tests/test_analysis.py -v` → PASS.

- [ ] **Step 7: Commit**

```bash
git add pipeline/analysis.py pipeline/assets/paper_analysis.py pipeline/storage.py \
        pipeline/definitions.py pipeline/jobs.py docker-compose.yml tests/test_analysis.py
git commit -m "feat(asset): paper_analysis matching the research-skill template"
```

---

### Task 16: Wire job + schedule; end-to-end integration

**Files:**
- Modify: `pipeline/definitions.py` (register schedule + all assets)
- Modify: `pipeline/jobs.py` (`ingest_document` selects the full asset set)
- Create: `tests/integration/test_end_to_end.py`
- Create/Modify: `docs/operations.md` (run instructions)

- [ ] **Step 1: Finalize `ingest_document` selection**

`pipeline/jobs.py`:
```python
from dagster import AssetSelection, define_asset_job

from pipeline.assets import (
    raw_blob, parsed_document, triage_metadata, chunks,
    extracted_graph, resolved_entities, graph_write, paper_analysis,
)

ingest_document = define_asset_job(
    name="ingest_document",
    selection=AssetSelection.assets(
        raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
        chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
        graph_write.graph_write, paper_analysis.paper_analysis,
    ),
    description="Full per-document build: raw → parse → triage → chunk → extract → resolve → write → analyse.",
)
```

- [ ] **Step 2: Register everything in `pipeline/definitions.py`**

```python
from dagster import Definitions

from pipeline.assets import (
    raw_blob, parsed_document, triage_metadata, chunks,
    extracted_graph, resolved_entities, graph_write, paper_analysis,
)
from pipeline.jobs import ingest_document
from pipeline.schedules import daily_ingest_schedule
from pipeline.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env, postgres_from_env,
)

defs = Definitions(
    assets=[
        raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
        chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
        graph_write.graph_write, paper_analysis.paper_analysis,
    ],
    jobs=[ingest_document],
    schedules=[daily_ingest_schedule],
    resources={
        "neo4j_new": new_neo4j_from_env(),
        "minio": minio_from_env(),
        "openai": OpenAILLMResource(),
        "anthropic": AnthropicResource(),
        "postgres": postgres_from_env(),
    },
)
```

- [ ] **Step 3: Update `tests/test_definitions.py`**

```python
from pipeline.definitions import defs

def test_full_asset_set_registered():
    names = {a.key.to_user_string() for a in defs.get_all_asset_specs()}
    assert {"raw_blob", "parsed_document", "triage_metadata", "chunks",
            "extracted_graph", "resolved_entities", "graph_write", "paper_analysis"} <= names
```

Run: `uv run pytest tests/test_definitions.py -v` → PASS.

- [ ] **Step 4: Write the end-to-end integration test**

`tests/integration/test_end_to_end.py`:
```python
import pytest
from dagster import materialize

from pipeline.assets import (raw_blob, parsed_document, triage_metadata, chunks,
                             extracted_graph, resolved_entities, graph_write, paper_analysis)
from pipeline.resources import (new_neo4j_from_env, minio_from_env, OpenAILLMResource,
                                AnthropicResource, postgres_from_env)
from pipeline.partitions import DOCUMENTS_PARTITION


@pytest.mark.integration
def test_one_paper_end_to_end(tmp_path):
    """Requires SOURCE_DIR with one fixture PDF, services up, and its hash registered."""
    # Register the fixture's content-hash partition, then materialize the full graph.
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = "FIXTURE_HASH"  # replace with the fixture PDF's sha256
    instance.add_dynamic_partitions(DOCUMENTS_PARTITION, [key])
    result = materialize(
        [raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
         chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
         graph_write.graph_write, paper_analysis.paper_analysis],
        partition_key=key,
        resources={"neo4j_new": new_neo4j_from_env(), "minio": minio_from_env(),
                   "openai": OpenAILLMResource(), "anthropic": AnthropicResource(),
                   "postgres": postgres_from_env()},
        instance=instance,
    )
    assert result.success
    new = new_neo4j_from_env()
    with new.get_driver().session(database=new.database) as s:
        # Paper is keyed by paper_id; the Document (this file) carries document_id = key.
        assert s.run("MATCH (p:Paper {document_id:$k}) RETURN count(p) AS n",
                     k=key).single()["n"] == 1
        assert s.run("MATCH (:Paper {document_id:$k})-[:HAS_SUMMARY]->(:Summary) "
                     "RETURN count(*) AS n", k=key).single()["n"] == 1
        assert s.run("MATCH (c:Chunk)-[:BELONGS_TO]->(:Document {id:$k}) RETURN count(c) AS n",
                     k=key).single()["n"] > 0
```

- [ ] **Step 5: Verify idempotency — including Definition/Result (re-run yields no duplicates)**

Add to the same file:
```python
_ASSETS = [raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
           chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
           graph_write.graph_write, paper_analysis.paper_analysis]


def _res():
    return {"neo4j_new": new_neo4j_from_env(), "minio": minio_from_env(),
            "openai": OpenAILLMResource(), "anthropic": AnthropicResource(),
            "postgres": postgres_from_env()}


@pytest.mark.integration
def test_rerun_is_idempotent():
    from dagster import DagsterInstance, materialize
    instance = DagsterInstance.get()
    key = "FIXTURE_HASH"

    def counts():
        new = new_neo4j_from_env()
        with new.get_driver().session(database=new.database) as s:
            return {
                "paper": s.run("MATCH (p:Paper {document_id:$k}) RETURN count(p) AS n", k=key).single()["n"],
                "def": s.run("MATCH (:Paper {document_id:$k})-[:STATES]->(d:Definition) RETURN count(d) AS n", k=key).single()["n"],
                "res": s.run("MATCH (:Paper {document_id:$k})-[:STATES]->(r:Result) RETURN count(r) AS n", k=key).single()["n"],
            }

    materialize(_ASSETS, partition_key=key, resources=_res(), instance=instance)
    first = counts()
    materialize(_ASSETS, partition_key=key, resources=_res(), instance=instance)
    assert counts() == first          # content-hash ids ⇒ no duplicate Definition/Result on re-run
    assert first["paper"] == 1
```

- [ ] **Step 5b: Verify CITES backward backfill (ingest B, then A which cites B)**

```python
@pytest.mark.integration
def test_citation_backfill_b_then_a():
    """B is ingested first; A (which references B) is ingested second. The CITES edge must
    appear via graph_write's backward pending_citations pass."""
    from dagster import DagsterInstance, materialize
    instance = DagsterInstance.get()
    key_b, key_a = "FIXTURE_B_HASH", "FIXTURE_A_HASH"  # A's references include B
    for k in (key_b, key_a):
        instance.add_dynamic_partitions(DOCUMENTS_PARTITION, [k])
        materialize(_ASSETS, partition_key=k, resources=_res(), instance=instance)
    new = new_neo4j_from_env()
    with new.get_driver().session(database=new.database) as s:
        n = s.run("MATCH (a:Paper {document_id:$a})-[:CITES]->(b:Paper {document_id:$b}) "
                  "RETURN count(*) AS n", a=key_a, b=key_b).single()["n"]
        assert n == 1
```

- [ ] **Step 6: Run the full unit suite**

Run: `uv run pytest -v`
Expected: all non-integration tests PASS.

- [ ] **Step 7: Document operations**

Update `docs/operations.md`: env vars (`SOURCE_DIR`, `RESOLVER_POSTGRES_DSN`, the `NEO4J_NEW_*`/`MINIO_*`/`OPENAI_API_KEY`/`ANTHROPIC_API_KEY`), the one-time `reset_graph.py --yes` + `init_neo4j.py` + `init_postgres.py` bootstrap, `docker compose up -d`, and how the daily schedule registers partitions. Note the `--run-integration` flag.

- [ ] **Step 8: Commit**

```bash
git add pipeline/definitions.py pipeline/jobs.py tests/test_definitions.py \
        tests/integration/test_end_to_end.py docs/operations.md
git commit -m "feat: wire ingest_document job + daily schedule; end-to-end integration tests"
```

---

## Self-review notes (author)

- **Aligned with spec revision (2026-05-27):** vendoring into `research_port.py` (Task 11); two-level Document/Paper identity + duplicate-paper quarantine (Tasks 11/16); paper-local content-hash `Definition`/`Result` ids (Task 14) dropped from resolution (Task 13); `chunks`/embeddings artifact-only with `graph_write` as sole Neo4j writer (Tasks 10/14); decide-only `resolved_entities` (Task 13); `pending_citations` forward/backward backfill (Tasks 13/14); `max_concurrent_runs=1` invariant (already in `docker/dagster.yaml`).
- **Spec coverage:** §4 DAG → Tasks 6,8,10,11,12,13,14,15; §5 components → one task each; §5.4 vendored S2 + paper identity → Task 11; §5.9 single-writer + CITES backfill → Task 14; §6 schema → Task 2; §6 Def/Result ids → Task 14; §7 resolver (Concept-only, decide-only) → Task 13; §8 models → resources + Tasks 11/12/15; §9 analysis output → Task 15 (canonical JSON; website render is the agreed downstream task, out of scope); §10 idempotency/quarantine → Tasks 8/11/14/16; §11 keep/delete/build → Task 7 + `research_port` in Task 11; §12 gates → Phase 0; §13 testing → per-task unit tests + Task 16 integration (incl. Def/Result idempotency + B-then-A citation backfill); §15 parity → Tasks 11,12,15.
- **Deferred-by-design (no task, per spec non-goals):** books, topic-DAG inference, researcher auto-linking, idea seeds, human-review UI, website adapter, cloud sources, local LLMs. Also deferred within v1: `Definition DEFINES Concept` / `Result USES Concept` edge population (best-effort term→concept matching — Task 14 note).
- **Type consistency:** `documents_partitions_def()`/`DOCUMENTS_PARTITION`; `ParseResult`; `ExtractionResult`/`Concept`/`Definition`/`Result`; `Decision`/`decide()`; `compute_paper_id`/`normalize_title`/`def_id`/`result_id`; bucket constants (`RAW_BUCKET`/`PARSED_BUCKET`/`CHUNKS_BUCKET`/`TRIAGE_BUCKET`/`EXTRACTED_BUCKET`/`ANALYSIS_BUCKET`); artifact shapes — chunk row `{id,position,text,embedding}`, resolved concept `{name,kind,action,embedding}`, triage `{paper_id,s2_id,identifiers,references}` — are defined once and reused consistently across tasks.
- **Known implementation-time confirmations:** exact Docling OCR option names (Task 8 note); Claude JSON-fencing stripper (Task 15 note); `QuarantineError` shared between Task 8 and Task 11 (import from `parsed_document` or move to `pipeline/errors.py`).
