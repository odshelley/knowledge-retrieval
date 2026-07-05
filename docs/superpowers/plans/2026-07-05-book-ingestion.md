# Book Ingestion Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A parallel Dagster asset lineage that ingests born-digital book PDFs into the same Neo4j graph as papers, with Book→Chapter→Section structure, chapter-partitioned LLM extraction, and entity resolution shared with the paper pipeline.

**Architecture:** Two new dynamic partition sets (`books` keyed by PDF SHA-256; `book_chapters` keyed by `{sha}:chNN`). Book-level assets parse per-page text + the PDF outline, build the chapter/section tree, chunk per section with page attribution, and write structure + chunk embeddings to Neo4j. Chapter-level assets run the existing extraction models per chunk, resolve concepts through the *same* `resolve_concepts()` stack, and write Section-STATES statements with book-native labels. Spec: `docs/superpowers/specs/2026-07-05-book-ingestion-design.md`.

**Tech Stack:** Python 3.12, Dagster 1.9.5, pypdfium2 5.8.0 (via docling), Neo4j (Aura), MinIO (boto3), Postgres/pgvector, OpenAI + Anthropic SDKs, pytest, reportlab (new dev dep, test fixtures only).

## Global Constraints

- Run everything from the repo root; tests via `uv run pytest <path> -v`. Unit tests must pass with no network and no services (`addopts = "-m 'not integration'"` already excludes integration tests).
- Paper-pipeline behavior must not change. The ONLY shared-file edits allowed: `pipeline/graph/schema.py` (additive), `pipeline/extraction/extraction.py` (one new optional field `Definition.name` with default `""`), `pipeline/ingest/source.py` (additive function), `pipeline/runtime/partitions.py` (additive), `scripts/init_neo4j.py` + `scripts/reset_graph.py` (import path fix), `pipeline/definitions.py` / `pipeline/runtime/jobs.py` (additive registration), `docker-compose.yml` + `.env.example` (additive), `pyproject.toml` (add reportlab to dev extras).
- No new docker services or MinIO buckets. Book artifacts reuse existing buckets with distinct key suffixes: `RAW/{sha}.pdf`, `PARSED/{sha}.pages.json`, `TRIAGE/{sha}.book.json`, `TRIAGE/{sha}.structure.json`, `CHUNKS/{sha}.book.json`, `EXTRACTED/{sha}:chNN.json` and `EXTRACTED/{sha}:chNN.resolved.json`.
- All Neo4j writes are idempotent `MERGE` on stable content-derived ids.
- Single-writer invariant stands: `max_concurrent_runs=1` in `docker/dagster.yaml` is not touched; sensors only submit runs.
- Identity: `book_id = "isbn:" + isbn13` else `"title:" + normalize_title(title)`. Chapter node id `{book_id}:chNN` (NN zero-padded 2). Section node id `{book_id}:chNN:sMM`. Partition keys use the PDF sha: chapter partition = `{sha}:chNN`.
- Book-sourced `Definition`/`Result` nodes set BOTH `name` and `label` to the printed label (e.g. `"Theorem 3.1"`) plus a `page` int property; ids are chapter-local: `{book_id}:chNN:def:{hash12}` / `{book_id}:chNN:{kind}:{hash12}` via the existing `def_id`/`result_id` helpers with owner `{book_id}:chNN`.
- Ruff line length 100, `from __future__ import annotations` at top of every new module, docstring style matching existing files.
- Commit after every task with the message given in the task. Do not push or open PRs mid-plan.

## File Structure

New package `pipeline/books/` (pure logic, no Dagster imports):
- `pipeline/books/__init__.py` — empty
- `pipeline/books/identity.py` — ISBN normalization + `compute_book_id`
- `pipeline/books/parsing.py` — per-page text + TOC extraction (pypdfium2)
- `pipeline/books/outline.py` — TOC/heading-fallback → chapter/section tree + artifact dicts
- `pipeline/books/metadata.py` — frontmatter prompt, ISBN regex, metadata validation
- `pipeline/books/chunking.py` — page-aware section chunker (reuses `_segments`)
- `pipeline/books/extraction.py` — context-note builder, page attribution, chapter payload assembly
- `pipeline/books/write.py` — all book Cypher + row builders

New assets (one file each, in `pipeline/assets/`): `book_raw_blob.py`, `book_parsed.py`, `book_metadata.py`, `book_structure.py`, `book_chunks.py`, `book_structure_write.py`, `book_chapter_extraction.py`, `book_chapter_resolved.py`, `book_chapter_graph_write.py`.

New runtime: `pipeline/runtime/sensors.py` (books_sensor + book_chapters_sensor). Jobs added in `pipeline/runtime/jobs.py`; registration in `pipeline/definitions.py`.

Tests: `tests/fixtures/make_book_pdf.py`, `tests/test_book_identity.py`, `tests/test_book_parsing.py`, `tests/test_book_outline.py`, `tests/test_book_metadata.py`, `tests/test_book_chunking.py`, `tests/test_book_write.py`, `tests/test_book_extraction.py`, `tests/test_book_partitions.py`, `tests/test_book_definitions.py`, `tests/integration/test_book_end_to_end.py`.

---

### Task 1: Schema extensions + fix broken init scripts

**Files:**
- Modify: `pipeline/graph/schema.py`
- Modify: `scripts/init_neo4j.py:19` (approx — the `from pipeline.schema import` line)
- Modify: `scripts/reset_graph.py:9-10` (the `pipeline.schema` / `pipeline.cypher` imports)
- Test: `tests/test_schema.py` (append)

**Interfaces:**
- Consumes: nothing new.
- Produces: `NODE_TYPES` includes `"Chapter"`, `"Section"`, `"Document"`; `RELATIONSHIP_TYPES` includes `"HAS_DOCUMENT"`, `"HAS_CHAPTER"`, `"HAS_SECTION"`, `"PART_OF"`; `PATTERNS` includes the seven new triples below; `iter_init_statements()` includes `chapter_id` and `section_id` uniqueness constraints. Later tasks rely on these labels/rel names verbatim.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_schema.py`:

```python
def test_book_pipeline_node_types_present():
    from pipeline.graph.schema import NODE_TYPES
    for label in ("Chapter", "Section", "Document"):
        assert label in NODE_TYPES


def test_book_pipeline_relationship_types_present():
    from pipeline.graph.schema import RELATIONSHIP_TYPES
    for rel in ("HAS_DOCUMENT", "HAS_CHAPTER", "HAS_SECTION", "PART_OF"):
        assert rel in RELATIONSHIP_TYPES


def test_book_pipeline_patterns_present():
    from pipeline.graph.schema import PATTERNS
    expected = [
        ("Book", "HAS_DOCUMENT", "Document"),
        ("Paper", "HAS_DOCUMENT", "Document"),
        ("Book", "HAS_CHAPTER", "Chapter"),
        ("Chapter", "HAS_SECTION", "Section"),
        ("Chunk", "BELONGS_TO", "Document"),
        ("Chunk", "PART_OF", "Section"),
        ("Section", "STATES", "Definition"),
        ("Section", "STATES", "Result"),
    ]
    for triple in expected:
        assert triple in PATTERNS


def test_init_cypher_has_chapter_and_section_constraints():
    from pipeline.graph.schema import iter_init_statements
    joined = " ".join(" ".join(s.split()) for s in iter_init_statements())
    assert "CREATE CONSTRAINT chapter_id IF NOT EXISTS" in joined
    assert "CREATE CONSTRAINT section_id IF NOT EXISTS" in joined


def test_init_scripts_import_current_module_paths():
    # Regression: scripts still importing pipeline.schema / pipeline.cypher broke at c636533.
    import pathlib
    for script in ("scripts/init_neo4j.py", "scripts/reset_graph.py"):
        text = pathlib.Path(script).read_text()
        assert "pipeline.schema" not in text.replace("pipeline.graph.schema", "")
        assert "pipeline.cypher" not in text.replace("pipeline.graph.cypher", "")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_schema.py -v`
Expected: the five new tests FAIL (missing labels/rels/constraints and stale imports); pre-existing tests PASS.

- [ ] **Step 3: Implement.** In `pipeline/graph/schema.py`:
  - `NODE_TYPES`: append `"Chapter"`, `"Section"`, `"Document"` after `"Summary"`.
  - `RELATIONSHIP_TYPES`: append `"HAS_DOCUMENT"`, `"HAS_CHAPTER"`, `"HAS_SECTION"`, `"PART_OF"` after `"HAS_SUMMARY"`.
  - `PATTERNS`: append:

```python
    ("Paper",      "HAS_DOCUMENT", "Document"),
    ("Book",       "HAS_DOCUMENT", "Document"),
    ("Book",       "HAS_CHAPTER",  "Chapter"),
    ("Chapter",    "HAS_SECTION",  "Section"),
    ("Chunk",      "BELONGS_TO",   "Document"),
    ("Chunk",      "PART_OF",      "Section"),
    ("Section",    "STATES",       "Definition"),
    ("Section",    "STATES",       "Result"),
```

  - `INIT_CYPHER`: append before the closing `"""`:

```
CREATE CONSTRAINT chapter_id IF NOT EXISTS
  FOR (c:Chapter) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT section_id IF NOT EXISTS
  FOR (s:Section) REQUIRE s.id IS UNIQUE;
```

  - In `scripts/init_neo4j.py` and `scripts/reset_graph.py`, replace `from pipeline.schema import ...` with `from pipeline.graph.schema import ...` and `from pipeline.cypher import ...` with `from pipeline.graph.cypher import ...` (read both scripts first; keep imported names unchanged).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_schema.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Full unit suite + commit**

Run: `uv run pytest`
Expected: PASS.

```bash
git add pipeline/graph/schema.py scripts/init_neo4j.py scripts/reset_graph.py tests/test_schema.py
git commit -m "feat(schema): Chapter/Section/Document labels, book hierarchy rels, fix init script imports"
```

---

### Task 2: Book identity (`pipeline/books/identity.py`)

**Files:**
- Create: `pipeline/books/__init__.py` (empty), `pipeline/books/identity.py`
- Test: `tests/test_book_identity.py`

**Interfaces:**
- Consumes: `pipeline.graph.research_port.normalize_title` (existing: collapses whitespace, lowercases).
- Produces: `normalize_isbn(raw: str) -> str | None` (digits/X only, valid length 10 or 13, else None); `compute_book_id(isbn: str | None, title: str | None) -> str` (raises `ValueError` when both missing); `chapter_node_id(book_id: str, n: int) -> str` = `f"{book_id}:ch{n:02d}"`; `section_node_id(book_id: str, ch: int, s: int) -> str` = `f"{book_id}:ch{ch:02d}:s{s:02d}"`.

- [ ] **Step 1: Write the failing tests** — `tests/test_book_identity.py`:

```python
import pytest

from pipeline.books.identity import (
    chapter_node_id, compute_book_id, normalize_isbn, section_node_id,
)


def test_normalize_isbn_strips_hyphens_and_spaces():
    assert normalize_isbn("978-3-16-148410-0") == "9783161484100"
    assert normalize_isbn("978 3 16 148410 0") == "9783161484100"


def test_normalize_isbn_accepts_isbn10_with_check_x():
    assert normalize_isbn("0-8044-2957-X") == "080442957X"


def test_normalize_isbn_rejects_wrong_length_or_garbage():
    assert normalize_isbn("1234") is None
    assert normalize_isbn("not an isbn") is None
    assert normalize_isbn("") is None


def test_compute_book_id_prefers_isbn_over_title():
    assert compute_book_id("978-3-16-148410-0", "Some Title") == "isbn:9783161484100"


def test_compute_book_id_falls_back_to_normalized_title():
    assert compute_book_id(None, "  Financial   Modelling ") == "title:financial modelling"
    assert compute_book_id("garbage", "Financial Modelling") == "title:financial modelling"


def test_compute_book_id_raises_without_either():
    with pytest.raises(ValueError):
        compute_book_id(None, None)


def test_chapter_and_section_node_ids_zero_pad():
    assert chapter_node_id("isbn:9783161484100", 3) == "isbn:9783161484100:ch03"
    assert section_node_id("isbn:9783161484100", 3, 2) == "isbn:9783161484100:ch03:s02"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_identity.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.books'`.

- [ ] **Step 3: Implement.** Create empty `pipeline/books/__init__.py`, then `pipeline/books/identity.py`:

```python
"""Book identity: isbn: > title:, mirroring compute_paper_id's doi > arxiv > title idiom."""
from __future__ import annotations

import re

from pipeline.graph.research_port import normalize_title

_ISBN_CHARS = re.compile(r"[^0-9Xx]")


def normalize_isbn(raw: str) -> str | None:
    """Strip separators; accept only well-formed ISBN-10/13 shapes (no checksum validation)."""
    if not raw:
        return None
    digits = _ISBN_CHARS.sub("", raw).upper()
    if len(digits) == 13 and digits.isdigit():
        return digits
    if len(digits) == 10 and digits[:9].isdigit() and (digits[9].isdigit() or digits[9] == "X"):
        return digits
    return None


def compute_book_id(isbn: str | None, title: str | None) -> str:
    norm = normalize_isbn(isbn) if isbn else None
    if norm:
        return "isbn:" + norm
    if title:
        return "title:" + normalize_title(title)
    raise ValueError("cannot form book id: no isbn/title")


def chapter_node_id(book_id: str, n: int) -> str:
    return f"{book_id}:ch{n:02d}"


def section_node_id(book_id: str, ch: int, s: int) -> str:
    return f"{book_id}:ch{ch:02d}:s{s:02d}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_identity.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/ tests/test_book_identity.py
git commit -m "feat(books): book identity — isbn > title, chapter/section node ids"
```

---

### Task 3: Per-page parsing + TOC extraction (`pipeline/books/parsing.py`) and the fixture book PDF

**Files:**
- Modify: `pyproject.toml` (add `"reportlab>=4.2"` to `[project.optional-dependencies] dev`)
- Create: `tests/fixtures/__init__.py` (empty), `tests/fixtures/make_book_pdf.py`
- Create: `pipeline/books/parsing.py`
- Modify: `tests/conftest.py` (append fixture)
- Test: `tests/test_book_parsing.py`

**Interfaces:**
- Consumes: `pipeline.ingest.parsing.needs_ocr` (existing).
- Produces:
  - `TocEntry` dataclass: `level: int` (0 = chapter), `title: str`, `page_index: int` (0-based).
  - `BookParse` dataclass: `pages: list[str]`, `toc: list[TocEntry]`, `mode: str` ("text" | "vlm"); property `is_empty: bool` (total stripped chars < 10).
  - `parse_book_pdf(path: str) -> BookParse`.
  - Test fixture `book_pdf` (session-scoped path to a generated 5-page, 2-chapter, 4-section PDF with outline bookmarks and an ISBN on page 1).
- The fixture PDF's exact content contract (later tasks depend on it): page 1 = title/ISBN front matter; Chapter 1 "Levy Processes" starts page 2 (outline level 0) with section "1.1 Definitions" (level 1) on page 2 and "1.2 First Results" on page 3; Chapter 2 "Poisson Processes" starts page 4 with "2.1 Counting Processes" on page 4 and "2.2 Compound Sums" on page 5. Page 2 contains the literal sentences `Definition 1.1 (Levy process).` and a definition of a Lévy process; page 3 contains `Theorem 1.2` depending on `Definition 1.1`.

- [ ] **Step 1: Add the dev dependency**

In `pyproject.toml` under `dev = [`, add the line `"reportlab>=4.2",` after `"pytest-asyncio>=0.24",`. Run: `uv sync --extra dev` (or `uv sync` if the project uses dev-default; verify with `uv run python -c "import reportlab; print(reportlab.Version)"`). Expected: prints a version ≥ 4.2.

- [ ] **Step 2: Write the fixture generator** — `tests/fixtures/make_book_pdf.py`:

```python
"""Generate the deterministic fixture book PDF (5 pages, 2 chapters, outline bookmarks).

Used by unit tests (via the book_pdf conftest fixture) and by the integration suite
(scripts drop the output into BOOKS_SOURCE_DIR). reportlab is a dev-only dependency.
"""
from __future__ import annotations

from pathlib import Path

FILLER = "This line pads the page so the parser does not classify it as scanned. " * 2

PAGES: list[tuple[list[tuple[str, str, int]], list[str]]] = [
    # (bookmarks on this page: [(title, key, level)], lines)
    ([], [
        "Stochastic Processes: A Tiny Book",
        "First Edition",
        "Osian Fixture",
        "ISBN 978-3-16-148410-0",
        "Tiny Press, 2026",
        FILLER, FILLER,
    ]),
    ([("Chapter 1 Levy Processes", "ch1", 0), ("1.1 Definitions", "s11", 1)], [
        "Chapter 1 Levy Processes",
        "1.1 Definitions",
        "Definition 1.1 (Levy process). A Levy process is a stochastic process",
        "with stationary and independent increments and cadlag paths, started at zero.",
        FILLER, FILLER,
    ]),
    ([("1.2 First Results", "s12", 1)], [
        "1.2 First Results",
        "Theorem 1.2. Every Levy process has an infinitely divisible marginal",
        "distribution at each fixed time. The proof uses Definition 1.1.",
        FILLER, FILLER,
    ]),
    ([("Chapter 2 Poisson Processes", "ch2", 0), ("2.1 Counting Processes", "s21", 1)], [
        "Chapter 2 Poisson Processes",
        "2.1 Counting Processes",
        "Definition 2.1 (Poisson process). A Poisson process is a Levy process",
        "whose increments follow a Poisson distribution.",
        FILLER, FILLER,
    ]),
    ([("2.2 Compound Sums", "s22", 1)], [
        "2.2 Compound Sums",
        "Theorem 2.2. A compound Poisson process is a Levy process.",
        "This depends on Theorem 1.2 and Definition 2.1.",
        FILLER, FILLER,
    ]),
]


def make_book_pdf(path: Path) -> Path:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    c = canvas.Canvas(str(path), pagesize=A4)
    for bookmarks, lines in PAGES:
        for title, key, level in bookmarks:
            c.bookmarkPage(key)
            c.addOutlineEntry(title, key, level=level)
        text = c.beginText(72, 780)
        for line in lines:
            text.textLine(line)
        c.drawText(text)
        c.showPage()
    c.save()
    return path


if __name__ == "__main__":
    import sys
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("tiny-book.pdf")
    print(make_book_pdf(out))
```

Append to `tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def book_pdf(tmp_path_factory):
    from tests.fixtures.make_book_pdf import make_book_pdf
    return make_book_pdf(tmp_path_factory.mktemp("bookpdf") / "tiny-book.pdf")
```

Also create empty `tests/fixtures/__init__.py`.

- [ ] **Step 3: Write the failing tests** — `tests/test_book_parsing.py`:

```python
from pipeline.books.parsing import parse_book_pdf


def test_parse_book_pdf_returns_one_string_per_page(book_pdf):
    parsed = parse_book_pdf(str(book_pdf))
    assert parsed.mode == "text"
    assert not parsed.is_empty
    assert len(parsed.pages) == 5
    assert "ISBN 978-3-16-148410-0" in parsed.pages[0]
    assert "Definition 1.1" in parsed.pages[1]


def test_parse_book_pdf_reads_outline_with_levels_and_pages(book_pdf):
    parsed = parse_book_pdf(str(book_pdf))
    flat = [(e.level, e.title, e.page_index) for e in parsed.toc]
    assert (0, "Chapter 1 Levy Processes", 1) in flat
    assert (1, "1.1 Definitions", 1) in flat
    assert (1, "1.2 First Results", 2) in flat
    assert (0, "Chapter 2 Poisson Processes", 3) in flat
    assert (1, "2.2 Compound Sums", 4) in flat
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_parsing.py -v`
Expected: FAIL — `No module named 'pipeline.books.parsing'`.

- [ ] **Step 5: Implement** — `pipeline/books/parsing.py`:

```python
"""Book PDF → per-page text + outline (TOC). pypdfium2, same engine as paper parsing,
but pages are kept separate (page provenance) and bookmarks are read for structure."""
from __future__ import annotations

from dataclasses import dataclass, field

from pipeline.ingest.parsing import needs_ocr


@dataclass
class TocEntry:
    level: int        # 0 = chapter-level bookmark
    title: str
    page_index: int   # 0-based


@dataclass
class BookParse:
    pages: list[str]
    toc: list[TocEntry] = field(default_factory=list)
    mode: str = "text"

    @property
    def is_empty(self) -> bool:
        return sum(len(p.strip()) for p in self.pages) < 10


def parse_book_pdf(path: str) -> BookParse:
    import pypdfium2 as pdfium

    pdf = pdfium.PdfDocument(path)
    try:
        pages = []
        for i in range(len(pdf)):
            page = pdf[i]
            textpage = page.get_textpage()
            pages.append(textpage.get_text_range())
            textpage.close()
            page.close()
        toc = []
        for bm in pdf.get_toc():
            dest = bm.get_dest()
            if dest is None:
                continue  # bookmark without a destination — cannot be placed, skip
            toc.append(TocEntry(level=bm.level, title=bm.get_title().strip(),
                                page_index=dest.get_index()))
    finally:
        pdf.close()

    total = sum(len(p) for p in pages)
    mode = "vlm" if needs_ocr(extractable_chars=total, page_count=max(len(pages), 1)) else "text"
    return BookParse(pages=pages, toc=toc, mode=mode)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_parsing.py -v`
Expected: ALL PASS. If `get_dest()` raises or returns unexpected shape, debug with
`uv run python -c "from tests.fixtures.make_book_pdf import make_book_pdf; from pathlib import Path; p=make_book_pdf(Path('/tmp/t.pdf')); import pypdfium2 as f; d=f.PdfDocument(str(p)); [print(b.level, b.get_title(), b.get_dest() and b.get_dest().get_index()) for b in d.get_toc()]"` — the API was verified against pypdfium2 5.8.0 (`PdfBookmark.level/.get_title()/.get_dest().get_index()`).

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock tests/fixtures/ tests/conftest.py pipeline/books/parsing.py tests/test_book_parsing.py
git commit -m "feat(books): per-page PDF parsing with outline extraction + fixture book generator"
```

---

### Task 4: Outline → Chapter/Section tree (`pipeline/books/outline.py`)

**Files:**
- Create: `pipeline/books/outline.py`
- Test: `tests/test_book_outline.py`

**Interfaces:**
- Consumes: `TocEntry` from Task 3; `chapter_node_id`/`section_node_id` from Task 2.
- Produces:
  - `SectionNode` dataclass: `number: str` (e.g. `"1.2"`), `title: str`, `page_start: int`, `page_end: int` (1-based inclusive).
  - `ChapterNode` dataclass: `number: int`, `title: str`, `page_start: int`, `page_end: int`, `sections: list[SectionNode]`.
  - `build_structure(toc: list[TocEntry], n_pages: int) -> list[ChapterNode]` — raises `NoStructureError` if < 2 chapter-level entries.
  - `detect_headings(pages: list[str]) -> list[TocEntry]` — regex fallback when the PDF has no outline.
  - `structure_artifact(book_id: str, sha: str, chapters: list[ChapterNode]) -> dict` — the JSON-serializable dict stored in `TRIAGE/{sha}.structure.json`, shape: `{"book_id": ..., "chapters": [{"id", "key", "number", "title", "page_start", "page_end", "sections": [{"id", "number", "title", "page_start", "page_end"}]}]}` where `key = f"{sha}:ch{number:02d}"`.
  - `NoStructureError(Exception)`.
- Semantics later tasks rely on: pages before the first chapter become chapter 0 "Front Matter" with one section numbered `"0.0"`; content in a chapter before its first bookmark section becomes a synthetic section `"{n}.0"` titled like the chapter; a chapter's `page_end` = next chapter's `page_start - 1` (last chapter ends at `n_pages`); same rule for sections within a chapter, clamped so `page_end >= page_start`.

- [ ] **Step 1: Write the failing tests** — `tests/test_book_outline.py`:

```python
import pytest

from pipeline.books.outline import (
    NoStructureError, build_structure, detect_headings, structure_artifact,
)
from pipeline.books.parsing import TocEntry


TOC = [
    TocEntry(0, "Chapter 1 Levy Processes", 1),
    TocEntry(1, "1.1 Definitions", 1),
    TocEntry(1, "1.2 First Results", 2),
    TocEntry(0, "Chapter 2 Poisson Processes", 3),
    TocEntry(1, "2.1 Counting Processes", 3),
    TocEntry(1, "2.2 Compound Sums", 4),
]


def test_build_structure_two_chapters_with_page_ranges():
    chapters = build_structure(TOC, n_pages=5)
    # front matter (page 1) + 2 real chapters
    assert [c.number for c in chapters] == [0, 1, 2]
    front, ch1, ch2 = chapters
    assert front.title == "Front Matter" and front.page_start == 1 and front.page_end == 1
    assert ch1.page_start == 2 and ch1.page_end == 3
    assert ch2.page_start == 4 and ch2.page_end == 5


def test_build_structure_sections_numbers_and_ranges():
    chapters = build_structure(TOC, n_pages=5)
    ch1 = chapters[1]
    assert [(s.number, s.title) for s in ch1.sections] == [
        ("1.1", "1.1 Definitions"), ("1.2", "1.2 First Results")]
    assert ch1.sections[0].page_start == 2 and ch1.sections[0].page_end == 2
    assert ch1.sections[1].page_start == 3 and ch1.sections[1].page_end == 3


def test_build_structure_synthesizes_leading_section():
    # Chapter bookmark on page 1 but first section bookmark only on page 3.
    toc = [TocEntry(0, "Chapter 1 Alpha", 0), TocEntry(1, "1.1 Later", 2),
           TocEntry(0, "Chapter 2 Beta", 4)]
    chapters = build_structure(toc, n_pages=6)
    ch1 = chapters[0]  # no front matter: chapter 1 starts on page 1
    assert [s.number for s in ch1.sections] == ["1.0", "1.1"]
    assert ch1.sections[0].title == "Chapter 1 Alpha"
    assert ch1.sections[0].page_start == 1 and ch1.sections[0].page_end == 2


def test_build_structure_deep_levels_fold_into_sections():
    # level >= 2 (subsections) are ignored, not treated as sections.
    toc = TOC + [TocEntry(2, "1.1.1 Sub", 1)]
    chapters = build_structure(toc, n_pages=5)
    assert [s.number for s in chapters[1].sections] == ["1.1", "1.2"]


def test_build_structure_requires_two_chapters():
    with pytest.raises(NoStructureError):
        build_structure([TocEntry(0, "Only Chapter", 0)], n_pages=3)
    with pytest.raises(NoStructureError):
        build_structure([], n_pages=3)


def test_sections_sharing_a_page_clamp_page_end():
    toc = [TocEntry(0, "Chapter 1 A", 0), TocEntry(1, "1.1 X", 0), TocEntry(1, "1.2 Y", 0),
           TocEntry(0, "Chapter 2 B", 1)]
    ch1 = build_structure(toc, n_pages=2)[0]
    assert ch1.sections[0].page_end >= ch1.sections[0].page_start


def test_detect_headings_fallback_finds_chapter_lines():
    pages = ["Preface text " * 20,
             "Chapter 1 Introduction\n" + "body " * 40,
             "more body " * 40,
             "Chapter 2 Advanced Topics\n" + "body " * 40]
    toc = detect_headings(pages)
    assert [(e.level, e.page_index) for e in toc] == [(0, 1), (0, 3)]
    assert toc[0].title == "Chapter 1 Introduction"


def test_structure_artifact_shape_and_ids():
    chapters = build_structure(TOC, n_pages=5)
    art = structure_artifact("isbn:9783161484100", "f" * 64, chapters)
    assert art["book_id"] == "isbn:9783161484100"
    ch1 = art["chapters"][1]
    assert ch1["id"] == "isbn:9783161484100:ch01"
    assert ch1["key"] == "f" * 64 + ":ch01"
    assert ch1["sections"][0]["id"] == "isbn:9783161484100:ch01:s01"
    assert ch1["sections"][0]["number"] == "1.1"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_outline.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement** — `pipeline/books/outline.py`:

```python
"""Outline (or heading-fallback) → Chapter/Section tree with 1-based inclusive page ranges.

Level-0 bookmarks are chapters, level-1 are sections, deeper levels are ignored. Pages
before the first chapter become chapter 0 "Front Matter"; chapter content before its first
section becomes a synthetic section "{n}.0". A node's page_end is the next sibling's
page_start - 1 (clamped to >= page_start; last node runs to the end of its parent range).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from pipeline.books.identity import chapter_node_id, section_node_id
from pipeline.books.parsing import TocEntry


class NoStructureError(Exception):
    """Raised when neither the outline nor heading detection yields >= 2 chapters."""


@dataclass
class SectionNode:
    number: str
    title: str
    page_start: int
    page_end: int


@dataclass
class ChapterNode:
    number: int
    title: str
    page_start: int
    page_end: int
    sections: list[SectionNode] = field(default_factory=list)


_CHAPTER_HEADING = re.compile(r"^\s*(?:Chapter|CHAPTER)\s+(\d+|[IVXLC]+)\b.*$", re.MULTILINE)
_SECTION_NUMBER = re.compile(r"^(\d+(?:\.\d+)+)\b")


def detect_headings(pages: list[str]) -> list[TocEntry]:
    """Fallback when the PDF has no outline: 'Chapter N ...' at the top of a page (first
    400 chars) marks a chapter start. Sections are not recoverable reliably — chapters only."""
    toc = []
    for i, text in enumerate(pages):
        m = _CHAPTER_HEADING.search(text[:400])
        if m:
            toc.append(TocEntry(level=0, title=m.group(0).strip(), page_index=i))
    return toc


def _section_number(title: str, chapter_no: int, ordinal: int) -> str:
    m = _SECTION_NUMBER.match(title)
    return m.group(1) if m else f"{chapter_no}.{ordinal}"


def build_structure(toc: list[TocEntry], n_pages: int) -> list[ChapterNode]:
    chapter_entries = [e for e in toc if e.level == 0]
    if len(chapter_entries) < 2:
        raise NoStructureError(
            f"only {len(chapter_entries)} chapter-level outline entries — need >= 2")

    chapters: list[ChapterNode] = []
    if chapter_entries[0].page_index > 0:
        fm_end = chapter_entries[0].page_index  # 1-based end = 0-based start of ch1
        chapters.append(ChapterNode(
            number=0, title="Front Matter", page_start=1, page_end=fm_end,
            sections=[SectionNode("0.0", "Front Matter", 1, fm_end)]))

    for n, entry in enumerate(chapter_entries, start=1):
        page_start = entry.page_index + 1
        if n < len(chapter_entries):
            page_end = max(page_start, chapter_entries[n].page_index)  # next ch's 0-based
            # start, read as a 1-based page number, IS the previous page
        else:
            page_end = max(page_start, n_pages)
        ch = ChapterNode(number=n, title=entry.title, page_start=page_start, page_end=page_end)

        # section entries belonging to this chapter: level-1 entries positionally between
        # this chapter entry and the next chapter entry in the ORIGINAL toc order
        i0 = toc.index(entry)
        i1 = toc.index(chapter_entries[n]) if n < len(chapter_entries) else len(toc)
        sec_entries = [e for e in toc[i0 + 1:i1] if e.level == 1]

        secs: list[SectionNode] = []
        if not sec_entries or sec_entries[0].page_index + 1 > page_start:
            lead_end = (sec_entries[0].page_index if sec_entries else page_end)
            secs.append(SectionNode(f"{n}.0", entry.title, page_start,
                                    max(page_start, lead_end)))
        for j, se in enumerate(sec_entries):
            s_start = se.page_index + 1
            s_end = (sec_entries[j + 1].page_index if j + 1 < len(sec_entries) else page_end)
            secs.append(SectionNode(_section_number(se.title, n, j + 1), se.title,
                                    s_start, max(s_start, s_end)))
        ch.sections = secs
        chapters.append(ch)
    return chapters


def structure_artifact(book_id: str, sha: str, chapters: list[ChapterNode]) -> dict:
    out = {"book_id": book_id, "chapters": []}
    for ch in chapters:
        out["chapters"].append({
            "id": chapter_node_id(book_id, ch.number),
            "key": f"{sha}:ch{ch.number:02d}",
            "number": ch.number, "title": ch.title,
            "page_start": ch.page_start, "page_end": ch.page_end,
            "sections": [{
                "id": section_node_id(book_id, ch.number, s_i),
                "number": s.number, "title": s.title,
                "page_start": s.page_start, "page_end": s.page_end,
            } for s_i, s in enumerate(ch.sections, start=0 if ch.sections
                                      and ch.sections[0].number.endswith(".0") else 1)],
        })
    return out
```

NOTE for the implementer: the enumeration trick in `structure_artifact` (synthetic `.0` section gets `s00`) and the page-range arithmetic in `build_structure` are the fiddly parts — the tests in Step 1 pin the intended behavior precisely; adjust the implementation until they pass rather than adjusting the tests. In particular `test_build_structure_two_chapters_with_page_ranges` fixes: chapter page_end = (next chapter's 0-based page_index) as a 1-based page number.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_outline.py tests/test_book_parsing.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/outline.py tests/test_book_outline.py
git commit -m "feat(books): outline → chapter/section tree with page ranges + heading fallback"
```

---

### Task 5: Partitions, book source dir, `book_raw_blob` + `book_parsed` assets

**Files:**
- Modify: `pipeline/runtime/partitions.py`
- Modify: `pipeline/ingest/source.py`
- Create: `pipeline/assets/book_raw_blob.py`, `pipeline/assets/book_parsed.py`
- Test: `tests/test_book_partitions.py`

**Interfaces:**
- Consumes: `parse_book_pdf` (Task 3); existing `list_pdf_files`, `file_partition_key`, `hash_bytes`, `_upload_if_absent` (import from `pipeline.assets.raw_blob`), `QuarantineError` (from `pipeline.assets.parsed_document`).
- Produces:
  - `partitions.py`: `BOOKS_PARTITION = "books"`, `BOOK_CHAPTERS_PARTITION = "book_chapters"`, `books_partitions_def()`, `book_chapters_partitions_def()` (both `DynamicPartitionsDefinition`), `chapter_partition_key(book_sha: str, n: int) -> str` = `f"{book_sha}:ch{n:02d}"`, `split_chapter_key(key: str) -> tuple[str, int]` (inverse; raises `ValueError` on malformed).
  - `source.py`: `books_source_dir() -> Path` reading env `BOOKS_SOURCE_DIR` (raise `RuntimeError` if unset, mirroring `source_dir()`).
  - Asset `book_raw_blob` (partitions `books`, resource `minio`) → `RAW/{sha}.pdf`.
  - Asset `book_parsed` (partitions `books`, deps `["book_raw_blob"]`, resource `minio`) → `PARSED/{sha}.pages.json` = `{"pages": [...], "toc": [{"level", "title", "page_index"}], "mode": "text"}`; raises `QuarantineError` on `mode == "vlm"` or empty parse.

- [ ] **Step 1: Write the failing tests** — `tests/test_book_partitions.py`:

```python
import pytest

from pipeline.runtime.partitions import (
    BOOK_CHAPTERS_PARTITION, BOOKS_PARTITION,
    book_chapters_partitions_def, books_partitions_def,
    chapter_partition_key, split_chapter_key,
)


def test_partition_set_names_are_distinct_from_documents():
    assert BOOKS_PARTITION == "books"
    assert BOOK_CHAPTERS_PARTITION == "book_chapters"
    assert books_partitions_def().name == "books"
    assert book_chapters_partitions_def().name == "book_chapters"


def test_chapter_partition_key_round_trips():
    key = chapter_partition_key("a" * 64, 3)
    assert key == "a" * 64 + ":ch03"
    assert split_chapter_key(key) == ("a" * 64, 3)


def test_chapter_keys_sort_in_chapter_order():
    keys = [chapter_partition_key("a" * 64, n) for n in (10, 2, 1)]
    assert sorted(keys) == [chapter_partition_key("a" * 64, n) for n in (1, 2, 10)]


def test_split_chapter_key_rejects_malformed():
    with pytest.raises(ValueError):
        split_chapter_key("nochapterhere")


def test_books_source_dir_requires_env(monkeypatch):
    from pipeline.ingest.source import books_source_dir
    monkeypatch.delenv("BOOKS_SOURCE_DIR", raising=False)
    with pytest.raises(RuntimeError):
        books_source_dir()
    monkeypatch.setenv("BOOKS_SOURCE_DIR", "/tmp/books")
    assert str(books_source_dir()) == "/tmp/books"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_partitions.py -v`
Expected: FAIL — ImportError on the new names.

- [ ] **Step 3: Implement.** Append to `pipeline/runtime/partitions.py`:

```python
BOOKS_PARTITION = "books"
BOOK_CHAPTERS_PARTITION = "book_chapters"


def books_partitions_def() -> DynamicPartitionsDefinition:
    return DynamicPartitionsDefinition(name=BOOKS_PARTITION)


def book_chapters_partitions_def() -> DynamicPartitionsDefinition:
    return DynamicPartitionsDefinition(name=BOOK_CHAPTERS_PARTITION)


def chapter_partition_key(book_sha: str, n: int) -> str:
    return f"{book_sha}:ch{n:02d}"


def split_chapter_key(key: str) -> tuple[str, int]:
    sha, sep, ch = key.rpartition(":ch")
    if not sep or not ch.isdigit():
        raise ValueError(f"malformed chapter partition key: {key!r}")
    return sha, int(ch)
```

Append to `pipeline/ingest/source.py`:

```python
def books_source_dir() -> Path:
    value = os.environ.get("BOOKS_SOURCE_DIR")
    if not value:
        raise RuntimeError(
            "BOOKS_SOURCE_DIR is not set — point it at the folder of book PDFs to ingest "
            "(see .env.example)."
        )
    return Path(value).expanduser()
```

Create `pipeline/assets/book_raw_blob.py`:

```python
"""book_raw_blob: ensure this book's PDF is in MinIO, keyed by content hash."""
from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.raw_blob import _upload_if_absent
from pipeline.ingest.source import books_source_dir, list_pdf_files
from pipeline.runtime.partitions import books_partitions_def, hash_bytes
from pipeline.runtime.storage import RAW_BUCKET


@asset(partitions_def=books_partitions_def(), required_resource_keys={"minio"})
def book_raw_blob(context) -> MaterializeResult:
    key = context.partition_key  # = content hash
    match, data = None, b""
    for p in list_pdf_files(books_source_dir()):
        candidate = p.read_bytes()
        if hash_bytes(candidate) == key:
            match, data = p, candidate
            break
    if match is None:
        raise ValueError(f"no source book PDF matches partition {key}")
    s3 = context.resources.minio.get_client()
    uploaded = _upload_if_absent(s3, RAW_BUCKET, f"{key}.pdf", data)
    return MaterializeResult(metadata={
        "key": f"{RAW_BUCKET}/{key}.pdf",
        "source_filename": match.name,
        "size_bytes": MetadataValue.int(len(data)),
        "uploaded": uploaded,
    })
```

Create `pipeline/assets/book_parsed.py`:

```python
"""book_parsed: per-page text + outline → MinIO. Quarantine scans and empty parses."""
from __future__ import annotations

import dataclasses
import json
import tempfile
from pathlib import Path

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.parsing import parse_book_pdf
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import PARSED_BUCKET, RAW_BUCKET


@asset(partitions_def=books_partitions_def(), deps=["book_raw_blob"],
       required_resource_keys={"minio"})
def book_parsed(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")
    with tempfile.TemporaryDirectory() as td:
        pdf_path = Path(td) / f"{key}.pdf"
        body = obj["Body"]
        try:
            with pdf_path.open("wb") as f:
                while chunk := body.read(1024 * 1024):
                    f.write(chunk)
        finally:
            body.close()
        parsed = parse_book_pdf(str(pdf_path))
    if parsed.mode == "vlm":
        raise QuarantineError(f"{key}: needs-ocr — no usable text layer (scanned book?).")
    if parsed.is_empty:
        raise QuarantineError(f"{key}: empty parse — corrupt or image-only PDF.")
    artifact = {"pages": parsed.pages,
                "toc": [dataclasses.asdict(e) for e in parsed.toc],
                "mode": parsed.mode}
    s3.put_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json",
                  Body=json.dumps(artifact).encode("utf-8"))
    return MaterializeResult(metadata={
        "key": f"{PARSED_BUCKET}/{key}.pages.json",
        "pages": MetadataValue.int(len(parsed.pages)),
        "toc_entries": MetadataValue.int(len(parsed.toc)),
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_partitions.py tests/test_source.py tests/test_partitions.py -v`
Expected: ALL PASS (existing source/partition tests prove no paper-path regression).

- [ ] **Step 5: Commit**

```bash
git add pipeline/runtime/partitions.py pipeline/ingest/source.py pipeline/assets/book_raw_blob.py pipeline/assets/book_parsed.py tests/test_book_partitions.py
git commit -m "feat(books): books/book_chapters partitions, BOOKS_SOURCE_DIR, raw+parsed assets"
```

---

### Task 6: Book metadata (`pipeline/books/metadata.py` + `book_metadata` asset)

**Files:**
- Create: `pipeline/books/metadata.py`, `pipeline/assets/book_metadata.py`
- Test: `tests/test_book_metadata.py`

**Interfaces:**
- Consumes: `compute_book_id` (Task 2), `QuarantineError`.
- Produces:
  - `metadata.py`: `BOOK_FRONTMATTER_PROMPT` (str), `find_isbn(text: str) -> str | None` (regex, returns raw match), `frontmatter_head(pages: list[str], max_pages: int = 10, max_chars: int = 8000) -> str`, `extract_book_frontmatter(client, model, head, timeout=60.0) -> dict` (OpenAI JSON-object call, mirrors `triage_metadata._extract_frontmatter`), `book_record(fm: dict, pages: list[str], document_id: str) -> dict` — merges LLM frontmatter with the regex ISBN (regex wins), computes `book_id`, raises `ValueError` if no isbn AND no title.
  - Asset `book_metadata` (partitions `books`, deps `["book_parsed"]`, resources `minio`, `neo4j_new`, `openai`) → `TRIAGE/{sha}.book.json` = the `book_record` dict: `{"book_id", "document_id", "title", "authors": [str], "year", "edition", "publisher", "isbn"}`. Quarantines on invalid LLM JSON and on duplicate-book-different-bytes (Cypher `MATCH (b:Book {id:$bid}) RETURN b.document_id AS doc`).

- [ ] **Step 1: Write the failing tests** — `tests/test_book_metadata.py`:

```python
import pytest

from pipeline.books.metadata import book_record, find_isbn, frontmatter_head


def test_find_isbn_hyphenated_and_labeled():
    assert find_isbn("Some Press\nISBN 978-3-16-148410-0\n2026") == "978-3-16-148410-0"
    assert find_isbn("ISBN-13: 9783161484100") == "9783161484100"


def test_find_isbn_none_when_absent():
    assert find_isbn("no identifiers here, just prose 123") is None


def test_frontmatter_head_limits_pages_and_chars():
    pages = [f"page {i} " + "x" * 3000 for i in range(20)]
    head = frontmatter_head(pages)
    assert "page 0" in head and "page 12" not in head
    assert len(head) <= 8000


def test_book_record_regex_isbn_wins_and_id_computed():
    fm = {"title": "Tiny Book", "authors": ["A. Author"], "year": 2026,
          "edition": "1st", "publisher": "Tiny Press", "isbn": None}
    pages = ["Tiny Book\nISBN 978-3-16-148410-0"]
    rec = book_record(fm, pages, document_id="f" * 64)
    assert rec["book_id"] == "isbn:9783161484100"
    assert rec["isbn"] == "9783161484100"
    assert rec["document_id"] == "f" * 64
    assert rec["authors"] == ["A. Author"]


def test_book_record_title_fallback():
    rec = book_record({"title": "Tiny Book", "authors": []}, ["no isbn"], "f" * 64)
    assert rec["book_id"] == "title:tiny book"
    assert rec["isbn"] is None


def test_book_record_raises_without_title_or_isbn():
    with pytest.raises(ValueError):
        book_record({"title": None, "authors": []}, ["nothing"], "f" * 64)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_metadata.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement.** `pipeline/books/metadata.py`:

```python
"""Book frontmatter: LLM extraction over the opening pages + deterministic ISBN regex.
No Semantic Scholar / external lookup (spec §4 — v1)."""
from __future__ import annotations

import json
import re

from pipeline.books.identity import compute_book_id, normalize_isbn

BOOK_FRONTMATTER_PROMPT = (
    "You are extracting bibliographic metadata from the opening pages of a BOOK. "
    "Return strict JSON: {\"title\": str, \"authors\": [str], \"year\": int|null, "
    "\"edition\": str|null, \"publisher\": str|null, \"isbn\": str|null}. "
    "Prefer the ISBN-13 from the copyright page if present."
)

_ISBN_RE = re.compile(r"ISBN(?:-1[03])?:?\s*([0-9][0-9\- ]{8,16}[0-9Xx])")


def find_isbn(text: str) -> str | None:
    m = _ISBN_RE.search(text)
    return m.group(1).strip() if m else None


def frontmatter_head(pages: list[str], max_pages: int = 10, max_chars: int = 8000) -> str:
    return "\n\n".join(pages[:max_pages])[:max_chars]


def extract_book_frontmatter(client, model: str, head: str, timeout: float = 60.0) -> dict:
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": BOOK_FRONTMATTER_PROMPT},
                  {"role": "user", "content": head}],
        response_format={"type": "json_object"},
        timeout=timeout,
    )
    return json.loads(resp.choices[0].message.content)


def book_record(fm: dict, pages: list[str], document_id: str) -> dict:
    raw_isbn = find_isbn(frontmatter_head(pages)) or fm.get("isbn")
    isbn = normalize_isbn(raw_isbn) if raw_isbn else None
    title = fm.get("title")
    book_id = compute_book_id(isbn, title)  # raises ValueError if neither
    return {
        "book_id": book_id, "document_id": document_id, "title": title,
        "authors": [a for a in (fm.get("authors") or []) if a],
        "year": fm.get("year"), "edition": fm.get("edition"),
        "publisher": fm.get("publisher"), "isbn": isbn,
    }
```

`pipeline/assets/book_metadata.py`:

```python
"""book_metadata: book identity from frontmatter (LLM + ISBN regex), duplicate check.
DECIDES identity only — Book/Author nodes are written by book_structure_write."""
from __future__ import annotations

import json

from dagster import MaterializeResult, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.metadata import book_record, extract_book_frontmatter, frontmatter_head
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import PARSED_BUCKET, TRIAGE_BUCKET

DUP_CHECK_BOOK = "MATCH (b:Book {id:$bid}) RETURN b.document_id AS doc"


@asset(partitions_def=books_partitions_def(), deps=["book_parsed"],
       required_resource_keys={"minio", "neo4j_new", "openai"})
def book_metadata(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    pages = json.loads(
        s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json")["Body"].read())["pages"]

    cfg = context.resources.openai
    client = cfg.get_client()
    try:
        fm = extract_book_frontmatter(client, cfg.extraction_model, frontmatter_head(pages),
                                      timeout=cfg.request_timeout)
        rec = book_record(fm, pages, document_id=key)
    except (json.JSONDecodeError, ValueError, KeyError, AttributeError) as exc:
        raise QuarantineError(f"{key}: book frontmatter extraction failed") from exc

    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(DUP_CHECK_BOOK, bid=rec["book_id"]).single()
        if row and row["doc"] and row["doc"] != key:
            raise QuarantineError(
                f"{key}: duplicate-book-different-bytes — book {rec['book_id']} already "
                f"ingested from document {row['doc']}")

    s3.put_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.book.json",
                  Body=json.dumps(rec).encode("utf-8"))
    return MaterializeResult(metadata={"book_id": rec["book_id"],
                                       "title": rec.get("title") or "",
                                       "isbn": rec.get("isbn") or ""})
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_metadata.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/metadata.py pipeline/assets/book_metadata.py tests/test_book_metadata.py
git commit -m "feat(books): frontmatter metadata + ISBN regex, book_metadata asset with dup check"
```

---

### Task 7: `book_structure` asset (registers chapter partitions)

**Files:**
- Create: `pipeline/assets/book_structure.py`
- Test: append one test to `tests/test_book_outline.py` (fallback path selection) — the asset body is thin IO around Task 4 logic.

**Interfaces:**
- Consumes: `PARSED/{sha}.pages.json`, `TRIAGE/{sha}.book.json`; `build_structure`, `detect_headings`, `structure_artifact`, `NoStructureError` (Task 4); `BOOK_CHAPTERS_PARTITION`, `chapter_partition_key` (Task 5).
- Produces: `TRIAGE/{sha}.structure.json` (shape defined in Task 4); registers dynamic partitions `{sha}:chNN` for every chapter INCLUDING chapter 0 (front matter gets structure + chunks + RAG but is a normal extraction chapter — cheap, and skipping it would special-case everything downstream). Helper `choose_toc(toc_entries: list[TocEntry], pages: list[str]) -> list[TocEntry]` in `pipeline/books/outline.py`: returns the bookmark TOC if it has >= 2 level-0 entries, else `detect_headings(pages)`.

- [ ] **Step 1: Write the failing test** — append to `tests/test_book_outline.py`:

```python
def test_choose_toc_prefers_outline_falls_back_to_headings():
    from pipeline.books.outline import choose_toc
    pages = ["Chapter 1 Intro\n" + "x" * 200, "y" * 200, "Chapter 2 More\n" + "x" * 200]
    assert [e.page_index for e in choose_toc([], pages)] == [0, 2]          # fallback
    outline = [TocEntry(0, "Chapter 1 A", 0), TocEntry(0, "Chapter 2 B", 2)]
    assert choose_toc(outline, pages) == outline                            # outline wins
    one_entry = [TocEntry(0, "Chapter 1 A", 0)]
    assert [e.title for e in choose_toc(one_entry, pages)] == [
        "Chapter 1 Intro", "Chapter 2 More"]                                # thin outline → fallback
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_book_outline.py::test_choose_toc_prefers_outline_falls_back_to_headings -v`
Expected: FAIL — `choose_toc` not defined.

- [ ] **Step 3: Implement.** Append to `pipeline/books/outline.py`:

```python
def choose_toc(toc_entries: list[TocEntry], pages: list[str]) -> list[TocEntry]:
    """Outline bookmarks when they carry >= 2 chapters, else heading detection."""
    if sum(1 for e in toc_entries if e.level == 0) >= 2:
        return toc_entries
    return detect_headings(pages)
```

Create `pipeline/assets/book_structure.py`:

```python
"""book_structure: chapter/section tree from outline (or heading fallback); registers one
book_chapters dynamic partition per chapter. Quarantine when no structure is recoverable."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.outline import NoStructureError, build_structure, choose_toc, structure_artifact
from pipeline.books.parsing import TocEntry
from pipeline.runtime.partitions import BOOK_CHAPTERS_PARTITION, books_partitions_def
from pipeline.runtime.storage import PARSED_BUCKET, TRIAGE_BUCKET


@asset(partitions_def=books_partitions_def(), deps=["book_parsed", "book_metadata"],
       required_resource_keys={"minio"})
def book_structure(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    parsed = json.loads(
        s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json")["Body"].read())
    meta = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.book.json")["Body"].read())

    toc = [TocEntry(**e) for e in parsed["toc"]]
    chosen = choose_toc(toc, parsed["pages"])
    try:
        chapters = build_structure(chosen, n_pages=len(parsed["pages"]))
    except NoStructureError as exc:
        raise QuarantineError(
            f"{key}: no-structure — outline had {len(toc)} entries, heading fallback "
            f"found {len(chosen)} chapters; cannot build chapter tree.") from exc

    artifact = structure_artifact(meta["book_id"], key, chapters)
    s3.put_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.structure.json",
                  Body=json.dumps(artifact).encode("utf-8"))

    chapter_keys = [ch["key"] for ch in artifact["chapters"]]
    context.instance.add_dynamic_partitions(BOOK_CHAPTERS_PARTITION, chapter_keys)
    return MaterializeResult(metadata={
        "book_id": meta["book_id"],
        "chapters": MetadataValue.int(len(artifact["chapters"])),
        "sections": MetadataValue.int(sum(len(c["sections"]) for c in artifact["chapters"])),
        "chapter_partitions": MetadataValue.text(", ".join(chapter_keys)),
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_outline.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/outline.py pipeline/assets/book_structure.py tests/test_book_outline.py
git commit -m "feat(books): book_structure asset — tree artifact + chapter partition registration"
```

---

### Task 8: Page-aware section chunking (`pipeline/books/chunking.py` + `book_chunks` asset)

**Files:**
- Create: `pipeline/books/chunking.py`, `pipeline/assets/book_chunks.py`
- Test: `tests/test_book_chunking.py`

**Interfaces:**
- Consumes: `pipeline.ingest.chunking._segments` and `_take_overlap` (existing private helpers — import them; same-repo use is fine), `embed_texts`.
- Produces:
  - `ChunkSpan` dataclass: `text: str`, `page_start: int`, `page_end: int`.
  - `split_pages(pages: list[tuple[int, str]], target: int = 4000, overlap: int = 600) -> list[ChunkSpan]` — same packing/overlap semantics as `split_markdown` but over (page_no, segment) pairs, tracking min/max page per chunk. Math blocks are atomic *within* a page (a display block split across a physical page boundary arrives as two segments — same limitation papers accept at chunk bounds).
  - `section_chunk_rows(sha, chapter, section, pages) -> list[dict]` — rows `{"id": f"{sha}:ch{NN}:s{MM}:{i}", "chapter_key", "section_id", "position": i, "text", "page_start", "page_end"}` (embedding added by the asset). `chapter`/`section` are the artifact dicts from Task 4; `pages` is the full 0-indexed page list; the section's 1-based `page_start..page_end` select from it.
  - Asset `book_chunks` (partitions `books`, deps `["book_structure"]`, resources `minio`, `openai`) → `CHUNKS/{sha}.book.json` = flat list of rows with `"embedding"` added; embeds in batches of 128.

- [ ] **Step 1: Write the failing tests** — `tests/test_book_chunking.py`:

```python
from pipeline.books.chunking import ChunkSpan, section_chunk_rows, split_pages


def test_split_pages_single_short_page_one_chunk():
    spans = split_pages([(3, "Only paragraph.")])
    assert spans == [ChunkSpan(text="Only paragraph.", page_start=3, page_end=3)]


def test_split_pages_tracks_page_range_across_pages():
    p1 = "para one. " * 30      # ~300 chars
    p2 = "para two. " * 30
    spans = split_pages([(1, p1), (2, p2)], target=10_000)
    assert len(spans) == 1
    assert spans[0].page_start == 1 and spans[0].page_end == 2


def test_split_pages_overflow_splits_and_pages_attributed():
    pages = [(n, f"page {n} sentence. " * 40) for n in (1, 2, 3, 4)]  # ~720 chars each
    spans = split_pages(pages, target=1500, overlap=0)
    assert len(spans) >= 2
    assert spans[0].page_start == 1
    assert spans[-1].page_end == 4
    for s in spans:
        assert s.page_start <= s.page_end


def test_split_pages_keeps_math_block_atomic():
    math = "$$\n" + "x = y\n" * 50 + "$$"          # oversized display block
    spans = split_pages([(1, "before.\n\n" + math + "\n\nafter.")], target=100, overlap=0)
    assert any(s.text == math for s in spans)       # never split


def test_split_pages_overlap_carries_trailing_segment_and_its_page():
    seg_a = "alpha. " * 20                            # ~140 chars, page 1
    seg_b = "beta. " * 20                             # page 2
    seg_c = "gamma. " * 20                            # page 3
    spans = split_pages([(1, seg_a), (2, seg_b), (3, seg_c)], target=300, overlap=150)
    assert len(spans) == 2
    assert spans[1].page_start == 2                   # overlap re-seeds from page 2's segment


def test_section_chunk_rows_ids_and_metadata():
    chapter = {"key": "f" * 64 + ":ch01", "number": 1}
    section = {"id": "isbn:x:ch01:s01", "number": "1.1", "title": "1.1 Defs",
               "page_start": 2, "page_end": 2}
    pages = ["front", "Definition 1.1 text here. " * 10, "later page"]
    rows = section_chunk_rows("f" * 64, chapter, section, pages)
    assert rows[0]["id"] == "f" * 64 + ":ch01:s01:0"
    assert rows[0]["chapter_key"] == "f" * 64 + ":ch01"
    assert rows[0]["section_id"] == "isbn:x:ch01:s01"
    assert rows[0]["page_start"] == 2 and rows[0]["page_end"] == 2
    assert "Definition 1.1" in rows[0]["text"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_chunking.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement** — `pipeline/books/chunking.py`:

```python
"""Page-aware section chunker. Same equation-atomic packing as ingest.chunking.split_markdown,
but each segment carries its page number so every chunk gets a page range."""
from __future__ import annotations

from dataclasses import dataclass

from pipeline.ingest.chunking import _segments


@dataclass
class ChunkSpan:
    text: str
    page_start: int
    page_end: int


def _pack(items: list[tuple[int, str]]) -> ChunkSpan:
    return ChunkSpan(text="\n\n".join(seg for _, seg in items),
                     page_start=min(p for p, _ in items),
                     page_end=max(p for p, _ in items))


def _take_overlap_pairs(items: list[tuple[int, str]], budget: int) -> tuple[list[tuple[int, str]], int]:
    tail: list[tuple[int, str]] = []
    total = 0
    for page, seg in reversed(items):
        if total + len(seg) > budget:
            break
        tail.insert(0, (page, seg))
        total += len(seg)
    return tail, total


def split_pages(pages: list[tuple[int, str]], target: int = 4000,
                overlap: int = 600) -> list[ChunkSpan]:
    """`pages` = [(1-based page_no, page_text), ...] in order. Mirrors split_markdown's
    accumulate/flush/overlap loop over (page, segment) pairs."""
    tagged: list[tuple[int, str]] = []
    for page_no, text in pages:
        tagged.extend((page_no, seg) for seg in _segments(text))

    chunks: list[ChunkSpan] = []
    cur: list[tuple[int, str]] = []
    cur_len = 0
    for pair in tagged:
        if cur and cur_len + len(pair[1]) > target:
            chunks.append(_pack(cur))
            cur, cur_len = _take_overlap_pairs(cur, overlap)
        cur.append(pair)
        cur_len += len(pair[1])
    if cur:
        chunks.append(_pack(cur))
    return chunks


def section_chunk_rows(sha: str, chapter: dict, section: dict, pages: list[str]) -> list[dict]:
    """Chunk one section (artifact dicts from structure_artifact; `pages` is the full
    0-indexed page-text list). Embeddings are added by the asset afterwards."""
    page_pairs = [(n, pages[n - 1]) for n in range(section["page_start"],
                                                   min(section["page_end"], len(pages)) + 1)]
    s_suffix = section["id"].rsplit(":", 1)[-1]  # "s01"
    return [{
        "id": f"{chapter['key']}:{s_suffix}:{i}",
        "chapter_key": chapter["key"],
        "section_id": section["id"],
        "position": i,
        "text": span.text,
        "page_start": span.page_start,
        "page_end": span.page_end,
    } for i, span in enumerate(split_pages(page_pairs))]
```

Create `pipeline/assets/book_chunks.py`:

```python
"""book_chunks: section-aware chunking + embeddings → MinIO artifact. No Neo4j writes;
book_structure_write creates Chunk nodes from this artifact."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.books.chunking import section_chunk_rows
from pipeline.embedding import embed_texts
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import CHUNKS_BUCKET, PARSED_BUCKET, TRIAGE_BUCKET

_EMBED_BATCH = 128


@asset(partitions_def=books_partitions_def(), deps=["book_structure"],
       required_resource_keys={"minio", "openai"})
def book_chunks(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    pages = json.loads(
        s3.get_object(Bucket=PARSED_BUCKET, Key=f"{key}.pages.json")["Body"].read())["pages"]
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.structure.json")["Body"].read())

    rows: list[dict] = []
    for chapter in structure["chapters"]:
        for section in chapter["sections"]:
            rows.extend(section_chunk_rows(key, chapter, section, pages))
    for i, row in enumerate(rows):  # global position across the book, stable ordering
        row["position"] = i

    cfg = context.resources.openai
    client = cfg.get_client()
    texts = [r["text"] for r in rows]
    vectors: list[list[float]] = []
    for start in range(0, len(texts), _EMBED_BATCH):
        vectors.extend(embed_texts(client, texts[start:start + _EMBED_BATCH],
                                   model=cfg.embedding_model, timeout=cfg.request_timeout))
    for row, vec in zip(rows, vectors, strict=True):
        row["embedding"] = vec

    s3.put_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.book.json",
                  Body=json.dumps(rows).encode("utf-8"))
    return MaterializeResult(metadata={"chunks": MetadataValue.int(len(rows))})
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_chunking.py tests/test_chunking.py -v`
Expected: ALL PASS (paper chunker untouched).

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/chunking.py pipeline/assets/book_chunks.py tests/test_book_chunking.py
git commit -m "feat(books): page-aware section chunking + book_chunks asset with batched embeddings"
```

---

### Task 9: Structure write Cypher (`pipeline/books/write.py` part 1 + `book_structure_write` asset)

**Files:**
- Create: `pipeline/books/write.py`, `pipeline/assets/book_structure_write.py`
- Test: `tests/test_book_write.py`

**Interfaces:**
- Consumes: artifacts from Tasks 6–8.
- Produces (in `pipeline/books/write.py`):
  - `WRITE_BOOK` — MERGE Book by id, SET title/year/edition/publisher/isbn/document_id, MERGE Authors + AUTHORED (authors are plain name strings).
  - `WRITE_BOOK_DOCUMENT` — MERGE Document {id:$doc_id} SET d.book_id; MERGE (b)-[:HAS_DOCUMENT]->(d).
  - `WRITE_CHAPTERS` — UNWIND $rows: MERGE Chapter by id, SET number/title/page_start/page_end, MERGE (b)-[:HAS_CHAPTER {order}]->(ch).
  - `WRITE_SECTIONS` — UNWIND $rows: MERGE Section by id, SET number/title/page_start/page_end, MERGE (ch)-[:HAS_SECTION {order}]->(s) (rows carry `chapter_id`).
  - `WRITE_BOOK_CHUNKS` — UNWIND $rows: MERGE Chunk by id, SET text/position/embedding/page_start/page_end, MERGE (c)-[:BELONGS_TO]->(d), MERGE (c)-[:PART_OF]->(s) (rows carry `section_id`; document matched once via $doc_id).
  - `chapter_rows(structure: dict) -> list[dict]` and `section_rows(structure: dict) -> list[dict]` — flatten the structure artifact for UNWIND.
  - Asset `book_structure_write` (partitions `books`, deps `["book_metadata", "book_structure", "book_chunks"]`, resources `minio`, `neo4j_new`).

- [ ] **Step 1: Write the failing tests** — `tests/test_book_write.py`:

```python
from pipeline.books.write import (
    WRITE_BOOK, WRITE_BOOK_CHUNKS, WRITE_BOOK_DOCUMENT, WRITE_CHAPTERS, WRITE_SECTIONS,
    chapter_rows, section_rows,
)

STRUCTURE = {
    "book_id": "isbn:9783161484100",
    "chapters": [
        {"id": "isbn:9783161484100:ch01", "key": "f" * 64 + ":ch01", "number": 1,
         "title": "Chapter 1", "page_start": 2, "page_end": 3,
         "sections": [{"id": "isbn:9783161484100:ch01:s01", "number": "1.1",
                       "title": "1.1 Defs", "page_start": 2, "page_end": 2}]},
        {"id": "isbn:9783161484100:ch02", "key": "f" * 64 + ":ch02", "number": 2,
         "title": "Chapter 2", "page_start": 4, "page_end": 5, "sections": []},
    ],
}


def test_chapter_rows_flatten_with_order():
    rows = chapter_rows(STRUCTURE)
    assert rows[0]["id"] == "isbn:9783161484100:ch01" and rows[0]["order"] == 1
    assert rows[1]["order"] == 2 and rows[1]["page_end"] == 5


def test_section_rows_carry_chapter_id_and_order():
    rows = section_rows(STRUCTURE)
    assert rows == [{"id": "isbn:9783161484100:ch01:s01", "chapter_id": "isbn:9783161484100:ch01",
                     "number": "1.1", "title": "1.1 Defs", "page_start": 2, "page_end": 2,
                     "order": 1}]


def test_write_book_merges_authors_and_sets_document_id():
    c = " ".join(WRITE_BOOK.split())
    assert "MERGE (b:Book {id: $id})" in c
    assert "b.document_id=$document_id" in c.replace(" =", "=").replace("= ", "=")
    assert "MERGE (a:Author {name: author})" in c
    assert "MERGE (a)-[:AUTHORED]->(b)" in c


def test_write_book_document_links_has_document():
    c = " ".join(WRITE_BOOK_DOCUMENT.split())
    assert "MERGE (d:Document {id:$doc_id})" in c
    assert "MERGE (b)-[:HAS_DOCUMENT]->(d)" in c


def test_write_chapters_and_sections_hierarchy_edges():
    ch = " ".join(WRITE_CHAPTERS.split())
    assert "MERGE (ch:Chapter {id: row.id})" in ch
    assert "MERGE (b)-[:HAS_CHAPTER {order: row.order}]->(ch)" in ch
    se = " ".join(WRITE_SECTIONS.split())
    assert "MATCH (ch:Chapter {id: row.chapter_id})" in se
    assert "MERGE (ch)-[:HAS_SECTION {order: row.order}]->(s)" in se


def test_write_book_chunks_belongs_to_and_part_of():
    c = " ".join(WRITE_BOOK_CHUNKS.split())
    assert "MERGE (c:Chunk {id: row.id})" in c
    assert "c.page_start = row.page_start" in c
    assert "MERGE (c)-[:BELONGS_TO]->(d)" in c
    assert "MATCH (s:Section {id: row.section_id})" in c
    assert "MERGE (c)-[:PART_OF]->(s)" in c
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_write.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement** — `pipeline/books/write.py` (part 1; Task 12 appends the statement Cypher):

```python
"""All book-pipeline Cypher + row builders. Structure writes here; statement (Definition/
Result/Concept) writes appended for book_chapter_graph_write. Everything MERGE/idempotent."""
from __future__ import annotations

WRITE_BOOK = """
MERGE (b:Book {id: $id})
SET b.title=$title, b.year=$year, b.edition=$edition, b.publisher=$publisher,
    b.isbn=$isbn, b.document_id=$document_id
WITH b
UNWIND $authors AS author
  MERGE (a:Author {name: author})
  MERGE (a)-[:AUTHORED]->(b)
"""

WRITE_BOOK_DOCUMENT = """
MATCH (b:Book {id: $id})
MERGE (d:Document {id:$doc_id}) SET d.book_id = $id
MERGE (b)-[:HAS_DOCUMENT]->(d)
"""

WRITE_CHAPTERS = """
MATCH (b:Book {id: $id})
UNWIND $rows AS row
  MERGE (ch:Chapter {id: row.id})
  SET ch.number = row.number, ch.title = row.title,
      ch.page_start = row.page_start, ch.page_end = row.page_end
  MERGE (b)-[:HAS_CHAPTER {order: row.order}]->(ch)
"""

WRITE_SECTIONS = """
UNWIND $rows AS row
  MATCH (ch:Chapter {id: row.chapter_id})
  MERGE (s:Section {id: row.id})
  SET s.number = row.number, s.title = row.title,
      s.page_start = row.page_start, s.page_end = row.page_end
  MERGE (ch)-[:HAS_SECTION {order: row.order}]->(s)
"""

WRITE_BOOK_CHUNKS = """
MATCH (d:Document {id: $doc_id})
UNWIND $rows AS row
  MATCH (s:Section {id: row.section_id})
  MERGE (c:Chunk {id: row.id})
  SET c.text = row.text, c.position = row.position, c.embedding = row.embedding,
      c.page_start = row.page_start, c.page_end = row.page_end
  MERGE (c)-[:BELONGS_TO]->(d)
  MERGE (c)-[:PART_OF]->(s)
"""


def chapter_rows(structure: dict) -> list[dict]:
    return [{"id": ch["id"], "number": ch["number"], "title": ch["title"],
             "page_start": ch["page_start"], "page_end": ch["page_end"],
             "order": ch["number"]} for ch in structure["chapters"]]


def section_rows(structure: dict) -> list[dict]:
    rows = []
    for ch in structure["chapters"]:
        for i, s in enumerate(ch["sections"], start=1):
            rows.append({"id": s["id"], "chapter_id": ch["id"], "number": s["number"],
                         "title": s["title"], "page_start": s["page_start"],
                         "page_end": s["page_end"], "order": i})
    return rows
```

Create `pipeline/assets/book_structure_write.py`:

```python
"""book_structure_write: writes Book/Author/Document/Chapter/Section/Chunk (+embeddings).
After this asset, vector RAG covers the whole book — extraction has not started yet."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.books.write import (
    WRITE_BOOK, WRITE_BOOK_CHUNKS, WRITE_BOOK_DOCUMENT, WRITE_CHAPTERS, WRITE_SECTIONS,
    chapter_rows, section_rows,
)
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import CHUNKS_BUCKET, TRIAGE_BUCKET

_CHUNK_WRITE_BATCH = 200  # embeddings are ~12 KB each; keep bolt messages bounded


@asset(partitions_def=books_partitions_def(),
       deps=["book_metadata", "book_structure", "book_chunks"],
       required_resource_keys={"minio", "neo4j_new"})
def book_structure_write(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    meta = json.loads(s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.book.json")["Body"].read())
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.structure.json")["Body"].read())
    chunk_rows = json.loads(
        s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{key}.book.json")["Body"].read())

    crows = chapter_rows(structure)
    srows = section_rows(structure)
    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        s.run(WRITE_BOOK, id=meta["book_id"], title=meta.get("title"), year=meta.get("year"),
              edition=meta.get("edition"), publisher=meta.get("publisher"),
              isbn=meta.get("isbn"), document_id=key, authors=meta.get("authors") or [])
        s.run(WRITE_BOOK_DOCUMENT, id=meta["book_id"], doc_id=key)
        s.run(WRITE_CHAPTERS, id=meta["book_id"], rows=crows)
        s.run(WRITE_SECTIONS, rows=srows)
        for start in range(0, len(chunk_rows), _CHUNK_WRITE_BATCH):
            s.run(WRITE_BOOK_CHUNKS, doc_id=key,
                  rows=chunk_rows[start:start + _CHUNK_WRITE_BATCH])
    return MaterializeResult(metadata={
        "book_id": meta["book_id"],
        "chapters": MetadataValue.int(len(crows)),
        "sections": MetadataValue.int(len(srows)),
        "chunks": MetadataValue.int(len(chunk_rows)),
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_write.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/write.py pipeline/assets/book_structure_write.py tests/test_book_write.py
git commit -m "feat(books): structure write — Book/Chapter/Section/Chunk with PART_OF + HAS_DOCUMENT"
```

---

### Task 10: Chapter extraction (`pipeline/books/extraction.py` + `Definition.name` + `book_chapter_extraction` asset)

**Files:**
- Modify: `pipeline/extraction/extraction.py` (add ONE field to `Definition`)
- Create: `pipeline/books/extraction.py`, `pipeline/assets/book_chapter_extraction.py`
- Test: `tests/test_book_extraction.py`

**Interfaces:**
- Consumes: `extract_from_chunk`, `extract_from_chunk_anthropic`, `merge_results`, `ExtractionResult`, `normalize_statement`; chunk artifact (Task 8), structure artifact (Task 4), `split_chapter_key` (Task 5).
- Produces:
  - `Definition.name: str = ""` — new Pydantic field, description: `'Label of the definition as printed, e.g. "Definition 2.14". Empty string if the text gives no label.'` Paper path ignores it (definition_rows doesn't read it) — no behavior change.
  - `pipeline/books/extraction.py`:
    - `chunk_with_context(book_title: str, chapter: dict, section: dict, text: str) -> str` — prepends a clearly-delimited context header to the chunk text (NOT the system prompt, so Anthropic's system-prompt cache stays warm).
    - `attach_pages(merged: ExtractionResult, chunk_extractions: list[tuple[ExtractionResult, int]]) -> tuple[list[dict], list[dict]]` — returns `(definitions, results)` as dicts with `"page"` added: first page (chunk `page_start`) where each statement appeared, keyed by `normalize_statement` (definitions) / `(kind, normalize_statement)` (results); `page` is `None` if unmatched.
    - `flatten_concepts(section_merges: list[ExtractionResult]) -> list[dict]` — chapter-wide dedup by lowercased name, order-preserving.
    - `chapter_payload(book_id: str, chapter: dict, section_outputs: list[dict], concepts: list[dict]) -> dict` — `{"book_id", "chapter_id", "concepts", "sections": [{"section_id", "definitions", "results"}]}`.
  - Asset `book_chapter_extraction` (partitions `book_chapters`, resources `minio`, `openai`, `anthropic`) → `EXTRACTED/{sha}:chNN.json`. Honors `EXTRACTION_PROVIDER` exactly like `extracted_graph`; logs per-chunk progress in the same format.

- [ ] **Step 1: Write the failing tests** — `tests/test_book_extraction.py`:

```python
from pipeline.books.extraction import (
    attach_pages, chapter_payload, chunk_with_context, flatten_concepts,
)
from pipeline.extraction.extraction import (
    Concept, Definition, ExtractionResult, Result, merge_results,
)


def test_definition_model_accepts_printed_label():
    d = Definition(term="Levy process", statement="$X_t$ has independent increments.",
                   name="Definition 1.1")
    assert d.name == "Definition 1.1"
    assert Definition(term="t", statement="s").name == ""   # default, paper path unaffected


def test_chunk_with_context_prepends_header_keeps_text():
    chapter = {"number": 3, "title": "Chapter 3 Convergence"}
    section = {"number": "3.2", "title": "3.2 Tightness"}
    out = chunk_with_context("Tiny Book", chapter, section, "The chunk body.")
    assert out.endswith("The chunk body.")
    assert '"Tiny Book"' in out and "Chapter 3" in out and "3.2" in out
    assert "exactly as printed" in out            # label-capture instruction present


def test_attach_pages_first_seen_page_wins():
    d = Definition(term="X", statement="$s_1$", name="Definition 1.1")
    r = Result(kind="theorem", statement="$t_1$", name="Theorem 1.2")
    per_chunk = [
        (ExtractionResult(definitions=[d]), 12),
        (ExtractionResult(definitions=[d], results=[r]), 13),   # d repeats on page 13
    ]
    merged = merge_results([e for e, _ in per_chunk])
    defs, results = attach_pages(merged, per_chunk)
    assert defs[0]["page"] == 12                                # first-seen
    assert results[0]["page"] == 13
    assert defs[0]["name"] == "Definition 1.1"


def test_flatten_concepts_dedups_across_sections_case_insensitive():
    a = ExtractionResult(concepts=[Concept(name="Levy process")])
    b = ExtractionResult(concepts=[Concept(name="levy PROCESS"), Concept(name="Martingale")])
    flat = flatten_concepts([a, b])
    assert [c["name"] for c in flat] == ["Levy process", "Martingale"]


def test_chapter_payload_shape():
    payload = chapter_payload(
        "isbn:x", {"id": "isbn:x:ch01"},
        section_outputs=[{"section_id": "isbn:x:ch01:s01", "definitions": [], "results": []}],
        concepts=[{"name": "Levy process", "kind": "concept"}])
    assert payload["book_id"] == "isbn:x" and payload["chapter_id"] == "isbn:x:ch01"
    assert payload["sections"][0]["section_id"] == "isbn:x:ch01:s01"
    assert payload["concepts"][0]["name"] == "Levy process"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_extraction.py -v`
Expected: FAIL — `Definition` has no `name` (TypeError/ValidationError) and module missing.

- [ ] **Step 3: Implement.** In `pipeline/extraction/extraction.py`, add to `class Definition` (after `term`):

```python
    name: str = Field(
        default="",
        description='Label of the definition as printed, e.g. "Definition 2.14". '
        "Empty string if the text gives no label.",
    )
```

Create `pipeline/books/extraction.py`:

```python
"""Chapter extraction plumbing: context headers (user-message side, so provider prompt
caches stay warm), page attribution, chapter payload assembly. Reuses the paper models."""
from __future__ import annotations

from pipeline.extraction.extraction import ExtractionResult
from pipeline.text_norm import normalize_statement

_CONTEXT_TEMPLATE = (
    "Context (metadata about where this chunk comes from — NOT part of the source text): "
    'book "{book}", Chapter {ch_no}: {ch_title}, Section {sec_no} {sec_title}. '
    "Capture each definition/result label exactly as printed in the book "
    '(e.g. "Theorem 3.1.2"), in the `name` field.\n\n---\n\n'
)


def chunk_with_context(book_title: str, chapter: dict, section: dict, text: str) -> str:
    header = _CONTEXT_TEMPLATE.format(
        book=book_title, ch_no=chapter["number"], ch_title=chapter["title"],
        sec_no=section["number"], sec_title=section["title"])
    return header + text


def attach_pages(merged: ExtractionResult,
                 chunk_extractions: list[tuple[ExtractionResult, int]],
                 ) -> tuple[list[dict], list[dict]]:
    def_pages: dict[str, int] = {}
    res_pages: dict[tuple[str, str], int] = {}
    for er, page in chunk_extractions:
        for d in er.definitions:
            def_pages.setdefault(normalize_statement(d.statement), page)
        for r in er.results:
            res_pages.setdefault((r.kind, normalize_statement(r.statement)), page)
    defs = [{**d.model_dump(), "page": def_pages.get(normalize_statement(d.statement))}
            for d in merged.definitions]
    results = [{**r.model_dump(),
                "page": res_pages.get((r.kind, normalize_statement(r.statement)))}
               for r in merged.results]
    return defs, results


def flatten_concepts(section_merges: list[ExtractionResult]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for m in section_merges:
        for c in m.concepts:
            if c.name.lower() not in seen:
                seen.add(c.name.lower())
                out.append(c.model_dump())
    return out


def chapter_payload(book_id: str, chapter: dict, section_outputs: list[dict],
                    concepts: list[dict]) -> dict:
    return {"book_id": book_id, "chapter_id": chapter["id"],
            "concepts": concepts, "sections": section_outputs}
```

Create `pipeline/assets/book_chapter_extraction.py`:

```python
"""book_chapter_extraction: LLM extraction over one chapter's chunks, per section.
Same provider switch + progress logging as extracted_graph; ~20-40 chunks per run."""
from __future__ import annotations

import json
import os
import time

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.parsed_document import QuarantineError
from pipeline.books.extraction import (
    attach_pages, chapter_payload, chunk_with_context, flatten_concepts,
)
from pipeline.extraction.extraction import extract_from_chunk, merge_results
from pipeline.extraction.extraction_anthropic import extract_from_chunk_anthropic
from pipeline.runtime.partitions import book_chapters_partitions_def, split_chapter_key
from pipeline.runtime.storage import CHUNKS_BUCKET, EXTRACTED_BUCKET, TRIAGE_BUCKET


@asset(partitions_def=book_chapters_partitions_def(),
       required_resource_keys={"minio", "openai", "anthropic"})
def book_chapter_extraction(context) -> MaterializeResult:
    pkey = context.partition_key
    sha, ch_no = split_chapter_key(pkey)
    s3 = context.resources.minio.get_client()
    meta = json.loads(s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{sha}.book.json")["Body"].read())
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{sha}.structure.json")["Body"].read())
    all_chunks = json.loads(
        s3.get_object(Bucket=CHUNKS_BUCKET, Key=f"{sha}.book.json")["Body"].read())

    chapter = next(c for c in structure["chapters"] if c["number"] == ch_no)
    chunks = sorted((r for r in all_chunks if r["chapter_key"] == pkey),
                    key=lambda r: r["position"])

    provider = os.environ.get("EXTRACTION_PROVIDER", "openai").lower()
    if provider == "anthropic":
        ar = context.resources.anthropic
        aclient = ar.get_client()
        def extract_one(t):
            return extract_from_chunk_anthropic(aclient, ar.extraction_model, t,
                                                timeout=ar.request_timeout)
        model_label = ar.extraction_model
    else:
        cfg = context.resources.openai
        oclient = cfg.get_client()
        def extract_one(t):
            return extract_from_chunk(oclient, cfg.extraction_model, t,
                                      timeout=cfg.request_timeout)
        model_label = cfg.extraction_model

    n = len(chunks)
    context.log.info(f"extraction: {n} chunks via {provider}/{model_label} (chapter {ch_no})")
    sections_by_id = {s["id"]: s for c in structure["chapters"] for s in c["sections"]}
    per_section: dict[str, list[tuple]] = {}
    try:
        for i, row in enumerate(chunks):
            section = sections_by_id[row["section_id"]]
            t0 = time.monotonic()
            er = extract_one(chunk_with_context(meta.get("title") or meta["book_id"],
                                                chapter, section, row["text"]))
            context.log.info(
                f"extraction: chunk {i + 1}/{n} done in {time.monotonic() - t0:.1f}s")
            per_section.setdefault(row["section_id"], []).append((er, row["page_start"]))

        section_outputs, section_merges = [], []
        for sec_id, pairs in per_section.items():
            merged = merge_results([er for er, _ in pairs])
            section_merges.append(merged)
            defs, results = attach_pages(merged, pairs)
            section_outputs.append({"section_id": sec_id,
                                    "definitions": defs, "results": results})
    except (json.JSONDecodeError, ValueError, KeyError, IndexError, AttributeError) as exc:
        raise QuarantineError(f"{pkey}: extraction returned unparseable/invalid JSON") from exc

    payload = chapter_payload(structure["book_id"], chapter, section_outputs,
                              flatten_concepts(section_merges))
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={
        "chunks": MetadataValue.int(n),
        "concepts": MetadataValue.int(len(payload["concepts"])),
        "definitions": MetadataValue.int(sum(len(s["definitions"]) for s in section_outputs)),
        "results": MetadataValue.int(sum(len(s["results"]) for s in section_outputs)),
        "provider": MetadataValue.text(provider),
        "model": MetadataValue.text(model_label),
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_extraction.py tests/test_extraction.py tests/test_extraction_anthropic.py -v`
Expected: ALL PASS (paper extraction tests prove `Definition.name` default breaks nothing).

- [ ] **Step 5: Commit**

```bash
git add pipeline/extraction/extraction.py pipeline/books/extraction.py pipeline/assets/book_chapter_extraction.py tests/test_book_extraction.py
git commit -m "feat(books): chapter extraction — context headers, page attribution, Definition.name label"
```

---

### Task 11: `book_chapter_resolved` asset (shared resolution)

**Files:**
- Create: `pipeline/assets/book_chapter_resolved.py`
- Test: `tests/test_book_resolved.py`

**Interfaces:**
- Consumes: `EXTRACTED/{pkey}.json` (Task 10); `resolve_concepts`, `lookup_by_key`, `nearest`, `similarity_to`, `adjudicate`, `record_decision` from `pipeline.resolution.resolver`; `resolved_concept_row` from `pipeline.assets.resolved_entities`; `embed_texts`.
- Produces: `EXTRACTED/{pkey}.resolved.json` — the Task-10 payload with `concepts` replaced by resolved rows (`{"surface", "name", "kind", "action", "embedding"}`) and `alias_registrations` added. `sections`, `book_id`, `chapter_id` pass through untouched. **This asset's body is deliberately a line-for-line mirror of `resolved_entities` — the same `resolve_concepts()` call with the same callbacks. Do not introduce a book-specific ladder.**

- [ ] **Step 1: Write the failing test** — `tests/test_book_resolved.py`:

```python
def test_book_chapter_resolved_reuses_paper_resolution_stack():
    """The Lévy-process guarantee is code reuse: the book asset must call the SAME
    resolve_concepts / resolved_concept_row as the paper asset — no parallel ladder."""
    import ast
    import pathlib

    src = pathlib.Path("pipeline/assets/book_chapter_resolved.py").read_text()
    tree = ast.parse(src)
    imports = {a.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
               for a in node.names}
    modules = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert "resolve_concepts" in imports
    assert "pipeline.resolution.resolver" in modules
    assert "resolved_concept_row" in imports          # shared row shape with graph_write
    assert "adjudicate" in imports                    # same LLM adjudicator


def test_book_chapter_resolved_passes_sections_through():
    from pipeline.assets.book_chapter_resolved import passthrough_payload
    payload = {"book_id": "isbn:x", "chapter_id": "isbn:x:ch01",
               "concepts": [{"name": "A", "kind": "concept"}],
               "sections": [{"section_id": "isbn:x:ch01:s01", "definitions": [], "results": []}]}
    out = passthrough_payload(payload, resolved_rows=[{"surface": "A", "name": "A",
                                                       "kind": "concept", "action": "create",
                                                       "embedding": [0.1]}],
                              alias_rows=[{"key": "a", "canonical": "A", "source": "det"}])
    assert out["sections"] == payload["sections"]
    assert out["chapter_id"] == "isbn:x:ch01"
    assert out["concepts"][0]["surface"] == "A"
    assert out["alias_registrations"][0]["canonical"] == "A"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_resolved.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement** — `pipeline/assets/book_chapter_resolved.py`:

```python
"""book_chapter_resolved: DECIDE ONLY — the same canonicalization+cosine+LLM ladder as
resolved_entities, against the same global pgvector tables, so a Lévy process in a book
resolves to the same Concept as a Lévy process in a paper. Writes no Neo4j/embeddings."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.resolved_entities import resolved_concept_row
from pipeline.embedding import embed_texts
from pipeline.resolution.resolver import (
    adjudicate,
    lookup_by_key,
    nearest,
    record_decision,
    resolve_concepts,
    similarity_to,
)
from pipeline.runtime.partitions import book_chapters_partitions_def
from pipeline.runtime.storage import EXTRACTED_BUCKET


def passthrough_payload(payload: dict, resolved_rows: list[dict],
                        alias_rows: list[dict]) -> dict:
    return {**payload, "concepts": resolved_rows, "alias_registrations": alias_rows}


@asset(partitions_def=book_chapters_partitions_def(), deps=["book_chapter_extraction"],
       required_resource_keys={"minio", "openai", "postgres"})
def book_chapter_resolved(context) -> MaterializeResult:
    pkey = context.partition_key
    s3 = context.resources.minio.get_client()
    payload = json.loads(
        s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.json")["Body"].read())

    cfg = context.resources.openai
    client = cfg.get_client()
    concepts = payload.get("concepts", [])
    names = [c["name"] for c in concepts]
    vecs = embed_texts(client, names, model=cfg.embedding_model, timeout=cfg.request_timeout)

    counts: dict[str, int] = {}
    with context.resources.postgres.connect() as conn:
        with conn.cursor() as cur:
            resolutions, aliases = resolve_concepts(
                concepts, vecs,
                lookup_by_key=lambda label, k: lookup_by_key(cur, label, k),
                nearest=lambda label, emb: nearest(cur, label, emb),
                similarity_to=lambda label, canon, emb: similarity_to(cur, label, canon, emb),
                adjudicate=lambda cand, canon: adjudicate(
                    client, cfg.effective_adjudication_model, cand, canon,
                    timeout=cfg.request_timeout),
            )
            for r in resolutions:
                counts[r.action] = counts.get(r.action, 0) + 1
                record_decision(cur, r.surface, r.matched_to, "Concept", r.score,
                                r.action, context.run_id, note=r.note)
        conn.commit()  # decision rows ONLY — graph_write-side owns embeddings + alias_map

    out = passthrough_payload(
        payload,
        resolved_rows=[resolved_concept_row(r.surface, r.canonical, r.kind, r.action,
                                            r.embedding) for r in resolutions],
        alias_rows=[{"key": a.key, "canonical": a.canonical, "source": a.source}
                    for a in aliases])
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.resolved.json",
                  Body=json.dumps(out).encode("utf-8"))
    return MaterializeResult(metadata={k: MetadataValue.int(v) for k, v in counts.items()})
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_resolved.py tests/test_resolved_entities.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/assets/book_chapter_resolved.py tests/test_book_resolved.py
git commit -m "feat(books): book_chapter_resolved — same resolve_concepts stack as papers"
```

---

### Task 12: Statement write (`pipeline/books/write.py` part 2 + `book_chapter_graph_write` asset)

**Files:**
- Modify: `pipeline/books/write.py` (append)
- Create: `pipeline/assets/book_chapter_graph_write.py`
- Test: append to `tests/test_book_write.py`

**Interfaces:**
- Consumes: `EXTRACTED/{pkey}.resolved.json` (Task 11); `def_id`, `result_id`, `result_name_index`, `defines_edge_rows`, `uses_edge_rows`, `WRITE_DEFINES`, `WRITE_RESULT_USES`, `WRITE_RESULT_DEPENDS` from `pipeline.assets.graph_write`; `upsert_embedding`, `upsert_alias` from `pipeline.resolution.resolver`.
- Produces (append to `pipeline/books/write.py`):
  - `WRITE_BOOK_CONCEPTS` — MATCH Book, UNWIND rows: MERGE Concept by name, SET tags, MERGE `(b)-[:COVERS]->(c)`, MERGE `(c)-[:COVERED_IN]->(b)`.
  - `WRITE_BOOK_DEFINITIONS` / `WRITE_BOOK_RESULTS` — UNWIND rows: MATCH Section by `row.section_id`, MERGE node by id, SET properties incl. `label` and `page`, MERGE `(s)-[:STATES]->(node)`.
  - `FIND_BOOK_RESULT_BY_LABEL` — `MATCH (r:Result) WHERE r.id STARTS WITH $book_prefix AND r.name = $label RETURN r.id AS id LIMIT 2`.
  - `book_definition_rows(owner: str, section_id: str, defs: list[dict]) -> list[dict]` — id via `def_id(owner, statement)`, fields `id/term/statement/label/page/section_id` (`label` = `d.get("name", "")`).
  - `book_result_rows(owner: str, section_id: str, results: list[dict]) -> list[dict]` — id via `result_id(owner, kind, statement)`, fields `id/name/label/kind/statement/page/section_id`.
  - `split_depends_on(owner: str, results: list[dict], name_index: dict[str, str]) -> tuple[list[dict], list[dict]]` — within-chapter resolution first; returns `(resolved_rows, unresolved)` where unresolved rows are `{"res_id", "label"}` for the cross-chapter Cypher lookup; self-references skipped.
  - Asset `book_chapter_graph_write` (partitions `book_chapters`, deps `["book_chapter_resolved"]`, resources `minio`, `neo4j_new`, `postgres`). `owner` = `payload["chapter_id"]`.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_book_write.py`:

```python
from pipeline.assets.graph_write import def_id, result_id, result_name_index
from pipeline.books.write import (
    FIND_BOOK_RESULT_BY_LABEL, WRITE_BOOK_CONCEPTS, WRITE_BOOK_DEFINITIONS, WRITE_BOOK_RESULTS,
    book_definition_rows, book_result_rows, split_depends_on,
)

OWNER = "isbn:9783161484100:ch01"
SEC = "isbn:9783161484100:ch01:s01"


def test_book_definition_rows_carry_label_page_section():
    rows = book_definition_rows(OWNER, SEC, [
        {"term": "Levy process", "statement": "$X$ has independent increments.",
         "name": "Definition 1.1", "page": 12, "defines": ["Levy process"]}])
    assert rows[0]["id"] == def_id(OWNER, "$X$ has independent increments.")
    assert rows[0]["id"].startswith(OWNER + ":def:")
    assert rows[0]["label"] == "Definition 1.1"
    assert rows[0]["page"] == 12 and rows[0]["section_id"] == SEC


def test_book_result_rows_ids_are_chapter_local():
    rows = book_result_rows(OWNER, SEC, [
        {"name": "Theorem 1.2", "kind": "theorem", "statement": "$x=y$", "page": 13}])
    assert rows[0]["id"] == result_id(OWNER, "theorem", "$x=y$")
    assert rows[0]["label"] == "Theorem 1.2" and rows[0]["name"] == "Theorem 1.2"


def test_split_depends_on_within_chapter_then_unresolved():
    results = [
        {"name": "Theorem 1.2", "kind": "theorem", "statement": "$a$",
         "depends_on": ["Definition 1.1", "Theorem 0.9", "Theorem 1.2"]},
        {"name": "Definition 1.1", "kind": "lemma", "statement": "$b$", "depends_on": []},
    ]
    rrows = book_result_rows(OWNER, SEC, results)
    idx = result_name_index(rrows)
    resolved, unresolved = split_depends_on(OWNER, results, idx)
    rid = result_id(OWNER, "theorem", "$a$")
    assert resolved == [{"res_id": rid, "dep_id": result_id(OWNER, "lemma", "$b$")}]
    assert unresolved == [{"res_id": rid, "label": "Theorem 0.9"}]  # cross-chapter candidate


def test_book_statement_cypher_anchors_on_section_with_label_and_page():
    d = " ".join(WRITE_BOOK_DEFINITIONS.split())
    assert "MATCH (s:Section {id: row.section_id})" in d
    assert "MERGE (s)-[:STATES]->" in d
    assert "label" in d and "page" in d
    r = " ".join(WRITE_BOOK_RESULTS.split())
    assert "MATCH (s:Section {id: row.section_id})" in r
    assert "MERGE (s)-[:STATES]->" in r


def test_book_concepts_cypher_covers_both_directions():
    c = " ".join(WRITE_BOOK_CONCEPTS.split())
    assert "MERGE (b)-[:COVERS]->(c)" in c
    assert "MERGE (c)-[:COVERED_IN]->(b)" in c


def test_find_book_result_by_label_is_prefix_scoped_and_bounded():
    q = " ".join(FIND_BOOK_RESULT_BY_LABEL.split())
    assert "STARTS WITH $book_prefix" in q
    assert "r.name = $label" in q
    assert "LIMIT 2" in q     # 2 not 1: two hits means ambiguous → caller must skip
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_write.py -v`
Expected: the new tests FAIL (names missing); Task-9 tests still PASS.

- [ ] **Step 3: Implement.** Append to `pipeline/books/write.py`:

```python
from pipeline.assets.graph_write import def_id, result_id  # noqa: E402  (chapter-local ids)

WRITE_BOOK_CONCEPTS = """
MATCH (b:Book {id:$book_id})
UNWIND $rows AS row
  MERGE (c:Concept {name: row.name})
  SET c.tags = row.tags
  MERGE (b)-[:COVERS]->(c)
  MERGE (c)-[:COVERED_IN]->(b)
"""

WRITE_BOOK_DEFINITIONS = """
UNWIND $rows AS row
  MATCH (s:Section {id: row.section_id})
  MERGE (d:Definition {id: row.id})
  SET d.term = row.term, d.statement = row.statement, d.label = row.label,
      d.name = row.label, d.page = row.page
  MERGE (s)-[:STATES]->(d)
"""

WRITE_BOOK_RESULTS = """
UNWIND $rows AS row
  MATCH (s:Section {id: row.section_id})
  MERGE (r:Result {id: row.id})
  SET r.name = row.name, r.label = row.label, r.kind = row.kind,
      r.statement = row.statement, r.page = row.page
  MERGE (s)-[:STATES]->(r)
"""

FIND_BOOK_RESULT_BY_LABEL = """
MATCH (r:Result) WHERE r.id STARTS WITH $book_prefix AND r.name = $label
RETURN r.id AS id LIMIT 2
"""


def book_definition_rows(owner: str, section_id: str, defs: list[dict]) -> list[dict]:
    return [{"id": def_id(owner, d["statement"]), "term": d["term"],
             "statement": d["statement"], "label": d.get("name", ""),
             "page": d.get("page"), "section_id": section_id} for d in defs]


def book_result_rows(owner: str, section_id: str, results: list[dict]) -> list[dict]:
    return [{"id": result_id(owner, r["kind"], r["statement"]), "name": r.get("name", ""),
             "label": r.get("name", ""), "kind": r["kind"], "statement": r["statement"],
             "page": r.get("page"), "section_id": section_id} for r in results]


def split_depends_on(owner: str, results: list[dict],
                     name_index: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """Within-chapter DEPENDS_ON via the collision-safe name index; anything not found is
    returned as an unresolved (res_id, label) for the cross-chapter Cypher lookup."""
    resolved, unresolved = [], []
    for r in results:
        rid = result_id(owner, r["kind"], r["statement"])
        for dep_label in r.get("depends_on", []):
            dep = name_index.get(dep_label)
            if dep == rid:
                continue  # self-reference
            if dep is not None:
                resolved.append({"res_id": rid, "dep_id": dep})
            else:
                unresolved.append({"res_id": rid, "label": dep_label})
    return resolved, unresolved
```

Create `pipeline/assets/book_chapter_graph_write.py`:

```python
"""book_chapter_graph_write: writes Concepts (COVERS/COVERED_IN), Definitions/Results
(Section-STATES, chapter-local ids, printed labels + pages), DEFINES/USES edges, and
DEPENDS_ON with cross-chapter back-reference lookup by (book prefix, label). Owns the
pgvector embedding + alias_map upserts for this chapter, mirroring graph_write."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.graph_write import (
    WRITE_DEFINES, WRITE_RESULT_DEPENDS, WRITE_RESULT_USES,
    concept_rows, defines_edge_rows, result_name_index, uses_edge_rows,
)
from pipeline.books.write import (
    FIND_BOOK_RESULT_BY_LABEL, WRITE_BOOK_CONCEPTS, WRITE_BOOK_DEFINITIONS, WRITE_BOOK_RESULTS,
    book_definition_rows, book_result_rows, split_depends_on,
)
from pipeline.resolution.resolver import upsert_alias, upsert_embedding
from pipeline.runtime.partitions import book_chapters_partitions_def
from pipeline.runtime.storage import EXTRACTED_BUCKET


@asset(partitions_def=book_chapters_partitions_def(), deps=["book_chapter_resolved"],
       required_resource_keys={"minio", "neo4j_new", "postgres"})
def book_chapter_graph_write(context) -> MaterializeResult:
    pkey = context.partition_key
    s3 = context.resources.minio.get_client()
    payload = json.loads(
        s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{pkey}.resolved.json")["Body"].read())

    book_id = payload["book_id"]
    owner = payload["chapter_id"]
    concepts = payload.get("concepts", [])
    crows = concept_rows(concepts)
    surface_to_canon = {c.get("surface", c["name"]).lower(): c["name"] for c in concepts}

    drows, rrows, raw_results = [], [], []
    sk_def = sk_use = 0
    def_edges, use_edges = [], []
    for sec in payload.get("sections", []):
        sid = sec["section_id"]
        drows.extend(book_definition_rows(owner, sid, sec.get("definitions", [])))
        rrows.extend(book_result_rows(owner, sid, sec.get("results", [])))
        raw_results.extend(sec.get("results", []))
        de, sd = defines_edge_rows(owner, sec.get("definitions", []), surface_to_canon)
        ue, su = uses_edge_rows(owner, sec.get("results", []), surface_to_canon)
        def_edges.extend(de); use_edges.extend(ue)
        sk_def += sd; sk_use += su

    name_index = result_name_index(rrows)
    dep_edges, unresolved = split_depends_on(owner, raw_results, name_index)

    new = context.resources.neo4j_new
    cross_linked = cross_skipped = 0
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        s.run(WRITE_BOOK_CONCEPTS, book_id=book_id, rows=crows)
        s.run(WRITE_BOOK_DEFINITIONS, rows=drows)
        s.run(WRITE_BOOK_RESULTS, rows=rrows)
        s.run(WRITE_DEFINES, rows=def_edges)
        s.run(WRITE_RESULT_USES, rows=use_edges)
        s.run(WRITE_RESULT_DEPENDS, rows=dep_edges)
        # cross-chapter back-references: label lookup scoped to this book's Result ids
        cross_rows = []
        for u in unresolved:
            hits = [rec["id"] for rec in s.run(FIND_BOOK_RESULT_BY_LABEL,
                                               book_prefix=book_id + ":", label=u["label"])]
            if len(hits) == 1 and hits[0] != u["res_id"]:
                cross_rows.append({"res_id": u["res_id"], "dep_id": hits[0]})
                cross_linked += 1
            else:
                cross_skipped += 1
                context.log.info(
                    f"depends_on skipped: {u['label']!r} → {len(hits)} matches in {book_id} "
                    "(forward reference or ambiguous label)")
        s.run(WRITE_RESULT_DEPENDS, rows=cross_rows)

        with context.resources.postgres.connect() as conn:
            with conn.cursor() as cur:
                for c in concepts:
                    if c.get("embedding") is not None:
                        upsert_embedding(cur, c["name"], "Concept", c["embedding"])
                for reg in payload.get("alias_registrations", []):
                    upsert_alias(cur, "Concept", reg["key"], reg["canonical"], reg["source"])
            conn.commit()

    return MaterializeResult(metadata={
        "concepts": MetadataValue.int(len(crows)),
        "definitions": MetadataValue.int(len(drows)),
        "results": MetadataValue.int(len(rrows)),
        "defines": MetadataValue.int(len(def_edges)),
        "uses": MetadataValue.int(len(use_edges)),
        "depends_on": MetadataValue.int(len(dep_edges) + cross_linked),
        "depends_on_cross_chapter": MetadataValue.int(cross_linked),
        "skipped_refs": MetadataValue.int(sk_def + sk_use + cross_skipped),
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_book_write.py tests/test_graph_write.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/write.py pipeline/assets/book_chapter_graph_write.py tests/test_book_write.py
git commit -m "feat(books): chapter graph write — Section-STATES, labels+pages, cross-chapter DEPENDS_ON"
```

---

### Task 13: Jobs, sensors, definitions, env + compose wiring

**Files:**
- Modify: `pipeline/runtime/jobs.py`, `pipeline/definitions.py`, `.env.example`, `docker-compose.yml`
- Create: `pipeline/runtime/sensors.py`
- Test: `tests/test_book_definitions.py`

**Interfaces:**
- Consumes: all assets from Tasks 5–12.
- Produces: jobs `ingest_book` (6 book-level assets) and `extract_book_chapter` (3 chapter assets); sensors `books_sensor` (scans `BOOKS_SOURCE_DIR`, registers `books` partitions, requests `ingest_book` runs; `SkipReason` when the env var is unset) and `book_chapters_sensor` (requests `extract_book_chapter` runs for registered chapter partitions whose book has a materialized `book_structure_write` and whose `book_chapter_graph_write` is not yet materialized; ascending key order; `run_key=partition_key` so each chapter is auto-requested exactly once — a failed chapter is re-run manually from the Dagster UI).

- [ ] **Step 1: Write the failing tests** — `tests/test_book_definitions.py`:

```python
from pipeline.definitions import defs


def test_book_jobs_registered():
    assert defs.get_job_def("ingest_book") is not None
    assert defs.get_job_def("extract_book_chapter") is not None


def test_book_sensors_registered():
    assert defs.get_sensor_def("books_sensor") is not None
    assert defs.get_sensor_def("book_chapters_sensor") is not None


def test_paper_job_untouched():
    job = defs.get_job_def("ingest_document")
    names = {ak.path[-1] for ak in job.asset_layer.executable_asset_keys}
    assert "graph_write" in names and not any(n.startswith("book_") for n in names)


def test_book_assets_registered():
    expected = {"book_raw_blob", "book_parsed", "book_metadata", "book_structure",
                "book_chunks", "book_structure_write", "book_chapter_extraction",
                "book_chapter_resolved", "book_chapter_graph_write"}
    have = {ak.path[-1] for ak in defs.get_asset_graph().get_all_asset_keys()}
    assert expected <= have
```

(If `get_asset_graph()`/`executable_asset_keys` differ on Dagster 1.9.5, use `defs.get_all_asset_specs()` / `job.asset_layer` equivalents — assert the same membership; check with `uv run python -c "from pipeline.definitions import defs; print([k for k in dir(defs) if 'asset' in k.lower()])"` before guessing.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_definitions.py -v`
Expected: FAIL — jobs/sensors not registered.

- [ ] **Step 3: Implement.** Append to `pipeline/runtime/jobs.py`:

```python
from pipeline.assets import (  # noqa: E402
    book_raw_blob, book_parsed, book_metadata, book_structure, book_chunks,
    book_structure_write, book_chapter_extraction, book_chapter_resolved,
    book_chapter_graph_write,
)

ingest_book = define_asset_job(
    name="ingest_book",
    selection=AssetSelection.assets(
        book_raw_blob.book_raw_blob, book_parsed.book_parsed, book_metadata.book_metadata,
        book_structure.book_structure, book_chunks.book_chunks,
        book_structure_write.book_structure_write,
    ),
    description="Book structure build: raw → parse(pages+toc) → metadata → structure → "
                "chunk+embed → write. RAG-ready; extraction follows per chapter.",
)

extract_book_chapter = define_asset_job(
    name="extract_book_chapter",
    selection=AssetSelection.assets(
        book_chapter_extraction.book_chapter_extraction,
        book_chapter_resolved.book_chapter_resolved,
        book_chapter_graph_write.book_chapter_graph_write,
    ),
    description="Per-chapter extraction: extract → resolve (shared ladder) → write.",
)
```

Create `pipeline/runtime/sensors.py`:

```python
"""Book sensors: discover new book PDFs; auto-enqueue chapter extraction after structure
write. Sensors only submit runs — max_concurrent_runs=1 serializes actual execution."""
from __future__ import annotations

import os

from dagster import AssetKey, RunRequest, SensorEvaluationContext, SensorResult, SkipReason, sensor

from pipeline.ingest.source import books_source_dir, list_pdf_files, file_partition_key
from pipeline.runtime.partitions import BOOK_CHAPTERS_PARTITION, BOOKS_PARTITION


@sensor(job_name="ingest_book", minimum_interval_seconds=300)
def books_sensor(context: SensorEvaluationContext):
    if not os.environ.get("BOOKS_SOURCE_DIR"):
        return SkipReason("BOOKS_SOURCE_DIR not set — book ingestion disabled")
    existing = set(context.instance.get_dynamic_partitions(BOOKS_PARTITION))
    requests, new_keys = [], []
    for pdf in list_pdf_files(books_source_dir()):
        key = file_partition_key(pdf)
        if key in existing or key in new_keys:
            continue
        new_keys.append(key)
        requests.append(RunRequest(partition_key=key, run_key=key))
    if new_keys:
        context.instance.add_dynamic_partitions(BOOKS_PARTITION, new_keys)
        context.log.info(f"registered {len(new_keys)} new book partitions")
    return SensorResult(run_requests=requests)


@sensor(job_name="extract_book_chapter", minimum_interval_seconds=60)
def book_chapters_sensor(context: SensorEvaluationContext):
    instance = context.instance
    ready_books = set(instance.get_materialized_partitions(AssetKey("book_structure_write")))
    done_chapters = set(
        instance.get_materialized_partitions(AssetKey("book_chapter_graph_write")))
    requests = []
    for ck in sorted(instance.get_dynamic_partitions(BOOK_CHAPTERS_PARTITION)):
        book_sha = ck.rpartition(":ch")[0]
        if book_sha in ready_books and ck not in done_chapters:
            # run_key=ck → each chapter auto-requested exactly once; failed chapters are
            # re-run manually from the UI rather than retry-looped by the sensor.
            requests.append(RunRequest(partition_key=ck, run_key=ck))
    if not requests:
        return SkipReason("no chapters awaiting extraction")
    return SensorResult(run_requests=requests)
```

In `pipeline/definitions.py`: extend the `pipeline.assets` import with the nine `book_*` modules, add each `book_*` asset to `assets=[...]`, import and add `ingest_book, extract_book_chapter` to `jobs=[...]`, and add `sensors=[books_sensor, book_chapters_sensor]` (import from `pipeline.runtime.sensors`).

Append to `.env.example` after the `SOURCE_DIR` block:

```
# Source folder the books sensor scans for new book PDFs. Leave unset to disable
# book ingestion (the sensor skips gracefully).
BOOKS_SOURCE_DIR=/path/to/books
```

In `docker-compose.yml`, for BOTH `dagster-webserver` and `dagster-daemon`: add env `BOOKS_SOURCE_DIR: ${BOOKS_SOURCE_DIR:-/opt/code/data/books}` and volume `- ${BOOKS_SOURCE_DIR:-./data/books}:${BOOKS_SOURCE_DIR:-/opt/code/data/books}:ro`, mirroring the SOURCE_DIR mount comment style. Create `data/books/.gitkeep` (empty file) so the default mount source exists.

- [ ] **Step 4: Run tests + sanity-load definitions**

Run: `uv run pytest tests/test_book_definitions.py tests/test_definitions.py -v`
Expected: ALL PASS.
Run: `uv run python -c "from pipeline.definitions import defs; print('assets:', len(list(defs.get_asset_graph().get_all_asset_keys())))"`
Expected: prints without error (17 assets).
Run: `docker compose config --quiet`
Expected: exit 0 (compose file still valid).

- [ ] **Step 5: Full unit suite + commit**

Run: `uv run pytest`
Expected: ALL PASS.

```bash
git add pipeline/runtime/jobs.py pipeline/runtime/sensors.py pipeline/definitions.py .env.example docker-compose.yml data/books/.gitkeep tests/test_book_definitions.py
git commit -m "feat(books): ingest_book + extract_book_chapter jobs, discovery + auto-enqueue sensors"
```

---

### Task 14: Integration tests — book end-to-end + the Lévy-process shared-concept test

**Files:**
- Create: `tests/integration/test_book_end_to_end.py`
- Test: itself (gated by `--run-integration`; needs docker stack up, Aura + OpenAI creds, and `BOOKS_SOURCE_DIR` pointing at a folder containing the generated fixture book).

**Interfaces:**
- Consumes: everything. Env contract (documented in the test docstring): `BOOKS_SOURCE_DIR` set; the fixture PDF generated into it via `uv run python tests/fixtures/make_book_pdf.py "$BOOKS_SOURCE_DIR/tiny-book.pdf"`; `INTEGRATION_BOOK_HASH` = its sha256 (`shasum -a 256 "$BOOKS_SOURCE_DIR/tiny-book.pdf"`).
- Produces: proof of the spec's acceptance criteria — structure in Neo4j, RAG-ready chunks with PART_OF, Section-STATES statements with labels/pages, idempotent re-runs, and one shared Concept node across paper- and book-sourced mentions.

- [ ] **Step 1: Write the tests** — `tests/integration/test_book_end_to_end.py`:

```python
"""Book pipeline integration tests. Setup (mirrors the paper fixtures):

    uv run python tests/fixtures/make_book_pdf.py "$BOOKS_SOURCE_DIR/tiny-book.pdf"
    export INTEGRATION_BOOK_HASH=$(shasum -a 256 "$BOOKS_SOURCE_DIR/tiny-book.pdf" | cut -d' ' -f1)
    uv run pytest tests/integration/test_book_end_to_end.py --run-integration -v
"""
import os

import pytest
from dagster import materialize

from pipeline.assets import (
    book_raw_blob, book_parsed, book_metadata, book_structure, book_chunks,
    book_structure_write, book_chapter_extraction, book_chapter_resolved,
    book_chapter_graph_write,
)
from pipeline.runtime.partitions import (
    BOOK_CHAPTERS_PARTITION, BOOKS_PARTITION, chapter_partition_key,
)
from pipeline.runtime.resources import (
    AnthropicResource, OpenAILLMResource, minio_from_env, new_neo4j_from_env, postgres_from_env,
)

_BOOK_ASSETS = [book_raw_blob.book_raw_blob, book_parsed.book_parsed,
                book_metadata.book_metadata, book_structure.book_structure,
                book_chunks.book_chunks, book_structure_write.book_structure_write]
_CHAPTER_ASSETS = [book_chapter_extraction.book_chapter_extraction,
                   book_chapter_resolved.book_chapter_resolved,
                   book_chapter_graph_write.book_chapter_graph_write]

BOOK_ID = "isbn:9783161484100"   # fixed by the fixture's ISBN line


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"missing required env var: {name}")
    return value


def _res():
    return {"neo4j_new": new_neo4j_from_env(), "minio": minio_from_env(),
            "openai": OpenAILLMResource(), "anthropic": AnthropicResource(),
            "postgres": postgres_from_env()}


def _session():
    new = new_neo4j_from_env()
    return new.get_driver().session(database=new.database)


def _ingest_book(instance, key):
    instance.add_dynamic_partitions(BOOKS_PARTITION, [key])
    result = materialize(_BOOK_ASSETS, partition_key=key, resources=_res(), instance=instance)
    assert result.success
    return result


@pytest.mark.integration
def test_book_structure_end_to_end():
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")
    _ingest_book(instance, key)
    with _session() as s:
        assert s.run("MATCH (b:Book {id:$b}) RETURN count(b) AS n", b=BOOK_ID).single()["n"] == 1
        assert s.run("MATCH (:Book {id:$b})-[:HAS_DOCUMENT]->(d:Document {id:$k}) "
                     "RETURN count(d) AS n", b=BOOK_ID, k=key).single()["n"] == 1
        # front matter + 2 chapters, 1+2+2 sections (fixture contract, Task 3)
        assert s.run("MATCH (:Book {id:$b})-[:HAS_CHAPTER]->(c) RETURN count(c) AS n",
                     b=BOOK_ID).single()["n"] == 3
        assert s.run("MATCH (:Book {id:$b})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->(s) "
                     "RETURN count(s) AS n", b=BOOK_ID).single()["n"] == 5
        # every chunk is located: BELONGS_TO document AND PART_OF a section, with pages
        orphans = s.run(
            "MATCH (c:Chunk)-[:BELONGS_TO]->(:Document {id:$k}) "
            "WHERE NOT (c)-[:PART_OF]->(:Section) OR c.page_start IS NULL "
            "RETURN count(c) AS n", k=key).single()["n"]
        assert orphans == 0
    # chapter partitions registered for the sensor to pick up
    chapters = instance.get_dynamic_partitions(BOOK_CHAPTERS_PARTITION)
    for n in (0, 1, 2):
        assert chapter_partition_key(key, n) in chapters


@pytest.mark.integration
def test_chapter_extraction_grounds_statements_in_sections():
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")
    _ingest_book(instance, key)
    ck = chapter_partition_key(key, 1)
    instance.add_dynamic_partitions(BOOK_CHAPTERS_PARTITION, [ck])
    result = materialize(_CHAPTER_ASSETS, partition_key=ck, resources=_res(),
                         instance=instance)
    assert result.success
    with _session() as s:
        # Definition 1.1 lands under a Section of chapter 1 with label + page
        row = s.run(
            "MATCH (:Book {id:$b})-[:HAS_CHAPTER]->(:Chapter {number:1})"
            "-[:HAS_SECTION]->(sec)-[:STATES]->(d:Definition) "
            "RETURN d.label AS label, d.page AS page, sec.number AS sec LIMIT 5",
            b=BOOK_ID).data()
        assert row, "no Definition attached to chapter 1 sections"
        assert any(r["label"] and r["label"].startswith("Definition 1.1") for r in row)
        assert all(r["page"] is not None for r in row)


@pytest.mark.integration
def test_levy_process_is_one_concept_across_paper_and_book_paths():
    """THE shared-resolution guarantee. Seed 'Lévy process' as if a paper created it
    (Concept node + pgvector embedding + alias), run book chapter 1 extraction, then
    assert the book attached to the SAME node and no near-duplicate Concept appeared."""
    from dagster import DagsterInstance

    from pipeline.embedding import embed_texts
    from pipeline.resolution.canonicalize import canonical_key
    from pipeline.resolution.resolver import upsert_alias, upsert_embedding

    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")

    # seed via the exact primitives graph_write uses on the paper path
    seed = "Levy process"
    cfg = OpenAILLMResource()
    vec = embed_texts(cfg.get_client(), [seed], model=cfg.embedding_model)[0]
    with _session() as s:
        s.run("MERGE (c:Concept {name:$n}) SET c.tags=['concept']", n=seed)
    pg = postgres_from_env()
    with pg.connect() as conn:
        with conn.cursor() as cur:
            upsert_embedding(cur, seed, "Concept", vec)
            upsert_alias(cur, "Concept", canonical_key(seed), seed, "integration-seed")
        conn.commit()

    _ingest_book(instance, key)
    ck = chapter_partition_key(key, 1)
    instance.add_dynamic_partitions(BOOK_CHAPTERS_PARTITION, [ck])
    assert materialize(_CHAPTER_ASSETS, partition_key=ck, resources=_res(),
                       instance=instance).success

    with _session() as s:
        n_concepts = s.run(
            "MATCH (c:Concept) WHERE toLower(c.name) CONTAINS 'levy' "
            "OR toLower(c.name) CONTAINS 'lévy' RETURN count(c) AS n").single()["n"]
        covered = s.run(
            "MATCH (:Book {id:$b})-[:COVERS]->(c:Concept {name:$n}) RETURN count(c) AS m",
            b=BOOK_ID, n=seed).single()["m"]
    assert covered == 1, "book did not attach to the seeded concept"
    assert n_concepts == 1, f"expected ONE Lévy concept, found {n_concepts} — resolution split"


@pytest.mark.integration
def test_book_rerun_is_idempotent():
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_BOOK_HASH")

    def counts():
        with _session() as s:
            return {
                "book": s.run("MATCH (b:Book {id:$b}) RETURN count(b) AS n",
                              b=BOOK_ID).single()["n"],
                "chapters": s.run("MATCH (:Book {id:$b})-[:HAS_CHAPTER]->(c) "
                                  "RETURN count(c) AS n", b=BOOK_ID).single()["n"],
                "chunks": s.run("MATCH (c:Chunk)-[:BELONGS_TO]->(:Document {id:$k}) "
                                "RETURN count(c) AS n", k=key).single()["n"],
            }

    _ingest_book(instance, key)
    first = counts()
    _ingest_book(instance, key)
    assert counts() == first
    assert first["book"] == 1
```

- [ ] **Step 2: Confirm unit runs stay green and integration tests are collected-but-skipped**

Run: `uv run pytest tests/integration/test_book_end_to_end.py -v`
Expected: 4 tests SKIPPED (needs --run-integration). Run `uv run pytest` — ALL PASS.

- [ ] **Step 3 (requires live services + user env; skip gracefully if unavailable): run the integration suite**

```bash
uv run python tests/fixtures/make_book_pdf.py "$BOOKS_SOURCE_DIR/tiny-book.pdf"
export INTEGRATION_BOOK_HASH=$(shasum -a 256 "$BOOKS_SOURCE_DIR/tiny-book.pdf" | cut -d' ' -f1)
uv run pytest tests/integration/test_book_end_to_end.py --run-integration -v
```

Expected: 4 PASS against live services (Neo4j Aura, MinIO, Postgres, OpenAI). Note: the model must extract "Levy process" from the fixture's verbatim definition sentence — if the shared-concept test flakes on model output, strengthen the fixture wording in `make_book_pdf.py` (keep the ISBN and page/bookmark layout contract intact) rather than weakening the assertion. If services are not reachable from this environment, mark the step done with a note that Step 2 verified skip-gating, and flag the integration run as pending for the user.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_book_end_to_end.py
git commit -m "test(books): integration — structure e2e, section grounding, shared Lévy concept, idempotency"
```

---

## Self-Review (performed at plan-writing time)

- **Spec coverage:** schema §3 → Task 1 + 9 + 12; identity §4 → Task 2 + 6; asset chain §5 lane 1 → Tasks 5–9, lane 2 → Tasks 10–12; orchestration §5 → Task 13; error handling §6 → quarantines in Tasks 5/6/7/10, idempotency in Task 14; testing §7 → unit tests throughout + Task 14 (the Lévy test); housekeeping (broken scripts, missing rel types) → Task 1. Out-of-scope items from §8 have no tasks — correct.
- **Types:** `structure_artifact` dict shape (Task 4) is consumed by Tasks 7, 8, 9, 10 with the same field names (`id`, `key`, `number`, `title`, `page_start`, `page_end`, `sections`). Chunk-row shape (Task 8) matches `WRITE_BOOK_CHUNKS` fields (Task 9). Chapter payload (Task 10) matches `book_chapter_resolved` passthrough (Task 11) and `book_chapter_graph_write` reads (Task 12): keys `book_id`, `chapter_id`, `concepts`, `sections[].section_id/definitions/results`.
- **Known judgment calls encoded above:** front-matter is chapter 0 and IS extracted (uniformity over micro-savings); `run_key`-once sensor semantics (no auto-retry loops); `label` and `name` both set on book statements; page attribution is chunk-first-seen (page-granular, not line-granular).
