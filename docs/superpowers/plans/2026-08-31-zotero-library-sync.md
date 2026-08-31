# Zotero Library Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist publication-venue data on `Paper` nodes at ingest time, then automatically file every ingested paper and book into the user's Zotero library under an `Alethograph` collection with normalized PDF attachments.

**Architecture:** Phase A (Tasks 1-7) extends the existing Semantic Scholar enrichment in `pipeline/graph/research_port.py` — which already *requests* `venue` and discards it — to persist venue, journal, volume, pages, and publication types, while fixing two latent bugs in the same code path. Phase B (Tasks 8-14) adds a `pipeline/zotero/` package whose pure logic (filename formatting, item construction, dedup matching) is separated from its HTTP transport, consumed by two new Dagster assets and one backfill script.

**Tech Stack:** Python 3.12, Dagster 1.9.5, Neo4j 5.x (`neo4j` driver), `requests`, pytest. No new third-party dependencies — `requests` is already declared in `pyproject.toml`.

## Global Constraints

- **No new dependencies.** `pyzotero` is explicitly rejected; hand-roll the Zotero calls with `requests`, mirroring how `research_port.py` hand-rolls Semantic Scholar.
- **Python >= 3.12**, `from __future__ import annotations` at the top of every new module (repo-wide convention).
- **Ruff line-length 100**, target `py312`. Run `uv run ruff check .` before every commit.
- **Secrets never logged.** `ZOTERO_API_KEY` is read from the environment only; never write it to asset metadata, log output, exception messages, or any committed file.
- **Neo4j has no nested-map property type.** S2's `journal` object must be flattened into separate scalar properties.
- **`s.run(rp.WRITE_PAPER, **paper)` splats the dict**, so `paper` dict keys and Cypher `$params` must match exactly or the query raises on a missing parameter.
- **`Paper.id` carries a uniqueness constraint** (`pipeline/graph/schema.py:124-125`). Any code that changes an existing `Paper.id` must check for collision first.
- **A Zotero failure must never fail an ingest run.** Transient errors log and return; only client errors (400/403/404) raise.
- **Zotero batch limit is 50** items per create request.
- **Zotero `mtime` is milliseconds**, not seconds.
- Test commands run as `uv run pytest ...` from the repo root.

## File Structure

**Phase A — modified:**
- `pipeline/graph/research_port.py` — S2 client. Gains `clean_doi`, `with_retry`, venue mapping, revised `WRITE_PAPER`.
- `pipeline/assets/triage_metadata.py` — ingest asset. Wires `clean_doi` and the new venue keys.
- `scripts/backfill_citations.py` — drops its private `_with_retry` in favour of the shared one.
- `server/queries.py` — exposes new properties to the MCP layer.

**Phase A — created:**
- `tests/test_research_port.py` — covers `clean_doi`, `with_retry`, the record mapper, and the null-overwrite regression.
- `scripts/backfill_venue.py` — re-enriches existing papers.
- `scripts/migrate_paper_ids.py` — collision-safe id relabelling for placeholder-DOI papers.

**Phase B — created (`pipeline/zotero/`):**
- `naming.py` — pure: attachment filename formatting. No imports beyond stdlib.
- `items.py` — pure: item-type mapping, creator splitting, payload construction, dedup matching.
- `client.py` — HTTP only: collections bootstrap, candidate search, library index, item create, three-step file upload.
- `push.py` — orchestration shared by both assets and the backfill script.

**Phase B — created (elsewhere):**
- `pipeline/assets/zotero_push.py`, `pipeline/assets/book_zotero_push.py` — thin Dagster wrappers over `push.py`.
- `scripts/backfill_zotero.py`
- `tests/test_zotero_naming.py`, `tests/test_zotero_items.py`, `tests/test_zotero_client.py`, `tests/test_zotero_push.py`
- `tests/integration/test_zotero_integration.py`

**Phase B — modified:**
- `pipeline/runtime/resources.py` — `ZoteroResource`.
- `pipeline/runtime/jobs.py`, `pipeline/definitions.py` — wiring.
- `.env.example` — new keys.

The pure/HTTP split matters: the existing suite tests pure functions directly with no mocking framework (see `tests/test_book_metadata.py`). Keeping `naming.py` and `items.py` free of I/O means Tasks 8-9 need no mocks at all.

---

## Task 1: `clean_doi` — reject placeholder and malformed DOIs

**Files:**
- Modify: `pipeline/graph/research_port.py` (add function after `normalize_title`, ~line 22)
- Modify: `pipeline/assets/triage_metadata.py:56` (wire into the DOI path)
- Test: `tests/test_research_port.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `clean_doi(doi: str | None) -> str | None` in `pipeline.graph.research_port`. Used by Tasks 4, 5, 6, 9, 12.

**Context:** A live audit of the 150-paper corpus found 3 DOIs that are unfilled LaTeX template placeholders (`10.1145/NNNNNNN.NNNNNNN`, `10.1017/S09624929XXXXXXXX`, `http://dx.doi.org/10.1145/0000000.0000000`). These break S2 lookup and pollute `compute_paper_id`, which derives node identity as `"doi:" + doi.strip().lower()`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_research_port.py`:

```python
import pytest

from pipeline.graph import research_port as rp


def test_clean_doi_passes_genuine_dois_through():
    assert rp.clean_doi("10.1111/jofi.13188") == "10.1111/jofi.13188"
    assert rp.clean_doi("10.1007/s00245-025-10263-5") == "10.1007/s00245-025-10263-5"
    assert rp.clean_doi("  10.1137/22m1542982  ") == "10.1137/22m1542982"


def test_clean_doi_strips_url_prefixes():
    assert rp.clean_doi("https://doi.org/10.1111/jofi.13188") == "10.1111/jofi.13188"
    assert rp.clean_doi("http://dx.doi.org/10.1111/jofi.13188") == "10.1111/jofi.13188"


def test_clean_doi_rejects_the_three_corpus_placeholders():
    # Real values found in the graph on 2026-08-31.
    assert rp.clean_doi("10.1145/NNNNNNN.NNNNNNN") is None
    assert rp.clean_doi("10.1017/S09624929XXXXXXXX") is None
    assert rp.clean_doi("http://dx.doi.org/10.1145/0000000.0000000") is None


def test_clean_doi_rejects_structurally_invalid():
    assert rp.clean_doi(None) is None
    assert rp.clean_doi("") is None
    assert rp.clean_doi("   ") is None
    assert rp.clean_doi("not-a-doi") is None
    assert rp.clean_doi("10.1145/") is None          # empty suffix
    assert rp.clean_doi("10.1/short-prefix") is None  # registrant must be 4-9 digits
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: FAIL with `AttributeError: module 'pipeline.graph.research_port' has no attribute 'clean_doi'`

- [ ] **Step 3: Write the implementation**

In `pipeline/graph/research_port.py`, directly after `normalize_title`:

```python
# A DOI is "10.<4-9 digit registrant>/<suffix>". Anything else is not a DOI.
_DOI_SHAPE = re.compile(r"^10\.\d{4,9}/\S+$")
# Unfilled LaTeX template DOIs observed in the corpus: NNNNNNN, XXXXXXXX, 0000000.
_PLACEHOLDER_DOI = re.compile(r"N{4,}|X{4,}|0{6,}", re.IGNORECASE)
_DOI_URL_PREFIX = re.compile(r"^https?://(dx\.)?doi\.org/", re.IGNORECASE)


def clean_doi(doi: str | None) -> str | None:
    """Normalize a DOI, returning None for absent, malformed, or placeholder values.

    Strips doi.org/dx.doi.org URL prefixes (stored verbatim by the frontmatter LLM, which
    breaks both S2 lookup and compute_paper_id's "doi:"+lower() identity) and rejects the
    unfilled LaTeX template DOIs that reached the graph as real identifiers.
    """
    if not doi:
        return None
    d = _DOI_URL_PREFIX.sub("", doi.strip())
    if not _DOI_SHAPE.match(d) or _PLACEHOLDER_DOI.search(d):
        return None
    return d
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Wire into `triage_metadata.py`**

In `pipeline/assets/triage_metadata.py`, replace the lookup block and DOI resolution (currently lines 51-57):

```python
    fm_doi = rp.clean_doi(fm.get("doi"))
    rec = None
    if fm.get("arxiv_id"):
        rec = rp.lookup_by_arxiv(fm["arxiv_id"])
    if rec is None and fm_doi:
        rec = rp.lookup_by_doi(fm_doi)
    rec = rec or {}

    doi = rp.clean_doi(rec.get("doi")) or fm_doi
    arxiv = rec.get("arxiv_id") or fm.get("arxiv_id")
    title = rec.get("title") or fm.get("title")
    paper_id = rp.compute_paper_id(doi, arxiv, title)
```

Note the placeholder DOI is no longer passed to `lookup_by_doi` (it would 404 and burn a request) nor to `compute_paper_id` (which would mint a junk identity).

- [ ] **Step 6: Verify the full suite still passes**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS, no new failures, no lint errors.

- [ ] **Step 7: Commit**

```bash
git add pipeline/graph/research_port.py pipeline/assets/triage_metadata.py tests/test_research_port.py
git commit -m "fix: reject placeholder and malformed DOIs before lookup and identity"
```

---

## Task 2: Promote the retry helper into the pipeline path

**Files:**
- Modify: `pipeline/graph/research_port.py` (add `with_retry`; apply to both lookups)
- Modify: `scripts/backfill_citations.py:58-66` (delete private copy, import shared)
- Test: `tests/test_research_port.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `with_retry(fn, *args, attempts: int = 3, base_sleep: float = 5.0)` in `pipeline.graph.research_port`. Used by Tasks 5, 11.

**Context:** `lookup_by_arxiv` and `lookup_by_doi` currently fold a 404, a 429, and a timeout into an identical `None` (`research_port.py:50-63`), so on S2's unauthenticated tier a throttled call is indistinguishable from a missing paper. A retry helper written for exactly this problem already exists in `scripts/backfill_citations.py:58-66` but was never promoted into the pipeline.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_research_port.py`:

```python
def test_with_retry_returns_first_truthy_without_sleeping(monkeypatch):
    slept = []
    monkeypatch.setattr(rp.time, "sleep", lambda s: slept.append(s))
    calls = []

    def fn():
        calls.append(1)
        return {"ok": True}

    assert rp.with_retry(fn) == {"ok": True}
    assert len(calls) == 1
    assert slept == []


def test_with_retry_retries_falsy_then_succeeds(monkeypatch):
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    outcomes = [None, None, {"ok": True}]

    def fn():
        return outcomes.pop(0)

    assert rp.with_retry(fn, base_sleep=0.0) == {"ok": True}


def test_with_retry_gives_up_after_attempts(monkeypatch):
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    calls = []

    def fn():
        calls.append(1)
        return None

    assert rp.with_retry(fn, attempts=3, base_sleep=0.0) is None
    assert len(calls) == 3


def test_lookup_by_arxiv_returns_none_on_404_without_retry(monkeypatch):
    calls = []

    class Resp:
        status_code = 404

        def json(self):
            return {}

    monkeypatch.setattr(rp.requests, "get", lambda *a, **k: (calls.append(1), Resp())[1])
    assert rp.lookup_by_arxiv("0000.00000") is None
    assert len(calls) == 1, "a 404 is definitive; it must not be retried"


def test_lookup_by_arxiv_retries_a_429(monkeypatch):
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    statuses = [429, 429, 200]

    class Resp:
        def __init__(self, code):
            self.status_code = code

        def json(self):
            return {"paperId": "abc", "title": "T"}

    monkeypatch.setattr(rp.requests, "get", lambda *a, **k: Resp(statuses.pop(0)))
    rec = rp.lookup_by_arxiv("2305.16261")
    assert rec is not None and rec["s2_id"] == "abc"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_research_port.py -v -k retry or 429`
Expected: FAIL — `with_retry` does not exist; `lookup_by_arxiv` does not retry.

- [ ] **Step 3: Write the implementation**

Add `import time` and `import logging` to the imports in `research_port.py`, then add above the lookups:

```python
log = logging.getLogger(__name__)

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def with_retry(fn, *args, attempts: int = 3, base_sleep: float = 5.0):
    """Retry a lookup that returns a falsy value on failure.

    S2's unauthenticated tier 429s aggressively and the lookup helpers fold any failure
    into None/[] — retry with backoff so a throttled call isn't mistaken for a missing paper.
    """
    out = None
    for i in range(attempts):
        out = fn(*args)
        if out:
            return out
        if i < attempts - 1:
            time.sleep(base_sleep * (i + 1))
    return out
```

Rewrite both lookups so the retry lives inside them and 404 stays definitive:

```python
def _get_paper(path: str) -> dict | None:
    """One S2 GET with retry on throttling/5xx. A 404 is definitive and returns immediately."""
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE}/paper/{path}", params={"fields": FIELDS}, timeout=20)
        except RequestException as exc:
            log.warning("S2 request for %s failed: %s", path, exc)
            time.sleep(5.0 * (attempt + 1))
            continue
        if r.status_code == 200:
            return _paper_json_to_record(r.json())
        if r.status_code not in _RETRYABLE_STATUS:
            log.info("S2 has no record for %s (HTTP %s)", path, r.status_code)
            return None
        log.warning("S2 throttled/erred for %s (HTTP %s), attempt %s/3",
                    path, r.status_code, attempt + 1)
        time.sleep(5.0 * (attempt + 1))
    log.warning("S2 lookup for %s exhausted retries — treating as unenriched", path)
    return None


def lookup_by_arxiv(arxiv_id: str) -> dict | None:
    return _get_paper(f"arXiv:{arxiv_id}")


def lookup_by_doi(doi: str) -> dict | None:
    return _get_paper(f"DOI:{doi}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS

- [ ] **Step 5: Delete the duplicate in `backfill_citations.py`**

Remove the `_with_retry` definition at `scripts/backfill_citations.py:58-66` and replace every call site's `_with_retry(` with `rp.with_retry(`. The module already does `from pipeline.graph import research_port as rp`.

Run: `grep -n "_with_retry" scripts/backfill_citations.py`
Expected: no output.

- [ ] **Step 6: Verify the full suite still passes**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add pipeline/graph/research_port.py scripts/backfill_citations.py tests/test_research_port.py
git commit -m "fix: retry throttled S2 lookups in the pipeline path, keep 404 definitive"
```

---

## Task 3: Stop null enrichment from overwriting good metadata

**Files:**
- Modify: `pipeline/graph/research_port.py:93-103` (`WRITE_PAPER`)
- Test: `tests/test_research_port.py`

**Interfaces:**
- Consumes: nothing.
- Produces: revised `WRITE_PAPER` Cypher. Task 4 extends the same `SET` clause; Tasks 5 and 14 reuse the query verbatim.

**Context:** `WRITE_PAPER` uses an unconditional `SET`, so re-ingesting a document whose S2 lookup returned nothing (a 429, a timeout, or a genuine absence) overwrites stored metadata with nulls. This is a live bug today and becomes worse once more fields depend on the same flaky call. The `coalesce` idiom is already used one line below for `a.s2_author_id`.

- [ ] **Step 1: Write the failing test**

This test asserts on the Cypher text rather than a live database, matching how the suite tests other query constants without Neo4j (see `tests/test_cypher.py`). Append to `tests/test_research_port.py`:

```python
ENRICHMENT_PROPS = [
    "title", "year", "arxiv_id", "doi", "s2_id",
    "abstract", "tldr", "citation_count", "influential_citation_count",
]


@pytest.mark.parametrize("prop", ENRICHMENT_PROPS)
def test_write_paper_never_nulls_out_enrichment_fields(prop):
    """A failed S2 lookup passes None for every enrichment field. An unconditional SET
    would erase good stored values; each must be guarded by coalesce."""
    assert f"p.{prop} = coalesce(${prop}, p.{prop})" in rp.WRITE_PAPER


def test_write_paper_sets_document_id_unconditionally():
    """document_id comes from the partition key, never from S2, and is never null."""
    assert "p.document_id = $document_id" in rp.WRITE_PAPER
    assert "coalesce($document_id" not in rp.WRITE_PAPER
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_research_port.py -v -k write_paper`
Expected: FAIL — the current query uses `SET p.title=$title, ...` with no `coalesce`.

- [ ] **Step 3: Write the implementation**

Replace the `SET` clause in `WRITE_PAPER`:

```python
WRITE_PAPER = """
MERGE (p:Paper {id: $id})
SET p.title = coalesce($title, p.title),
    p.year = coalesce($year, p.year),
    p.arxiv_id = coalesce($arxiv_id, p.arxiv_id),
    p.doi = coalesce($doi, p.doi),
    p.s2_id = coalesce($s2_id, p.s2_id),
    p.abstract = coalesce($abstract, p.abstract),
    p.tldr = coalesce($tldr, p.tldr),
    p.citation_count = coalesce($citation_count, p.citation_count),
    p.influential_citation_count = coalesce($influential_citation_count,
                                            p.influential_citation_count),
    p.document_id = $document_id
WITH p
UNWIND $authors AS author
  MERGE (a:Author {name: author.name})
  SET a.s2_author_id = coalesce(author.s2_author_id, a.s2_author_id)
  MERGE (a)-[:AUTHORED]->(p)
"""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS

- [ ] **Step 5: Verify the full suite still passes**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/graph/research_port.py tests/test_research_port.py
git commit -m "fix: coalesce enrichment writes so a failed S2 lookup can't null good metadata"
```

---

## Task 4: Persist venue, journal, volume, pages, and publication types

**Files:**
- Modify: `pipeline/graph/research_port.py:12` (`FIELDS`), `:36-47` (`_paper_json_to_record`), `WRITE_PAPER`
- Modify: `pipeline/assets/triage_metadata.py:62-70` (the `paper` dict)
- Test: `tests/test_research_port.py`

**Interfaces:**
- Consumes: `WRITE_PAPER` from Task 3.
- Produces: five new `Paper` properties — `venue` (string), `journal_name` (string), `volume` (string), `pages` (string), `publication_types` (list of string). Consumed by Tasks 5, 7, 9, 12.

**Context:** `FIELDS` already requests `venue` and `_paper_json_to_record` already maps it, but `triage_metadata.py` builds the write payload without it, so it never reaches the graph. `publicationTypes` and `journal` are not requested at all. `publicationVenue` is deliberately *not* requested: it returns a nested object duplicating `venue`/`journal` and adds a second structure to flatten for no gain.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_research_port.py`:

```python
S2_JOURNAL_RESPONSE = {
    "paperId": "abc123",
    "title": "Equilibrium Returns with Transaction Costs",
    "abstract": "We study...",
    "year": 2019,
    "venue": "Finance and Stochastics",
    "publicationTypes": ["JournalArticle"],
    "journal": {"name": "Finance and Stochastics", "volume": "23", "pages": "707-744"},
    "externalIds": {"DOI": "10.1007/s00780-019-00397-0", "ArXiv": "1707.08464"},
    "citationCount": 42,
    "influentialCitationCount": 5,
    "tldr": {"text": "A short summary."},
    "authors": [{"name": "Bruno Bouchard", "authorId": "1"}],
}


def test_record_maps_venue_and_flattens_journal():
    rec = rp._paper_json_to_record(S2_JOURNAL_RESPONSE)
    assert rec["venue"] == "Finance and Stochastics"
    assert rec["journal_name"] == "Finance and Stochastics"
    assert rec["volume"] == "23"
    assert rec["pages"] == "707-744"
    assert rec["publication_types"] == ["JournalArticle"]


def test_record_survives_missing_journal_and_publication_types():
    minimal = {"paperId": "x", "title": "T", "journal": None}
    rec = rp._paper_json_to_record(minimal)
    assert rec["journal_name"] is None
    assert rec["volume"] is None
    assert rec["pages"] is None
    assert rec["publication_types"] == []
    assert rec["venue"] is None


def test_fields_requests_the_venue_columns():
    for field in ("venue", "publicationTypes", "journal"):
        assert field in rp.FIELDS
    assert "publicationVenue" not in rp.FIELDS, "deliberately not requested — see spec"


@pytest.mark.parametrize("prop", ["venue", "journal_name", "volume", "pages"])
def test_write_paper_coalesces_venue_fields(prop):
    assert f"p.{prop} = coalesce(${prop}, p.{prop})" in rp.WRITE_PAPER


def test_write_paper_guards_publication_types_with_case_not_coalesce():
    """The mapper emits [] (not None) when S2 omits the field, and coalesce would treat
    an empty list as a present value, erasing good data."""
    assert "size($publication_types) > 0" in rp.WRITE_PAPER
    assert "coalesce($publication_types" not in rp.WRITE_PAPER
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_research_port.py -v -k "venue or journal or publication or fields"`
Expected: FAIL — `KeyError: 'journal_name'` and the `FIELDS`/`WRITE_PAPER` assertions.

- [ ] **Step 3: Extend `FIELDS`**

```python
FIELDS = ("paperId,title,abstract,year,venue,externalIds,citationCount,"
          "influentialCitationCount,tldr,authors,publicationTypes,journal")
```

- [ ] **Step 4: Extend `_paper_json_to_record`**

```python
def _paper_json_to_record(j: dict) -> dict:
    ext = j.get("externalIds") or {}
    journal = j.get("journal") or {}
    return {
        "s2_id": j.get("paperId"), "title": j.get("title"), "abstract": j.get("abstract"),
        "year": j.get("year"), "venue": j.get("venue"),
        "journal_name": journal.get("name"),
        "volume": journal.get("volume"),
        "pages": journal.get("pages"),
        "publication_types": j.get("publicationTypes") or [],
        "doi": ext.get("DOI"), "arxiv_id": ext.get("ArXiv"),
        "citation_count": j.get("citationCount"),
        "influential_citation_count": j.get("influentialCitationCount"),
        "tldr": (j.get("tldr") or {}).get("text"),
        "authors": [{"name": a.get("name"), "s2_author_id": a.get("authorId")}
                    for a in (j.get("authors") or [])],
    }
```

- [ ] **Step 5: Extend the `WRITE_PAPER` SET clause**

Insert before `p.document_id = $document_id`:

```cypher
    p.venue = coalesce($venue, p.venue),
    p.journal_name = coalesce($journal_name, p.journal_name),
    p.volume = coalesce($volume, p.volume),
    p.pages = coalesce($pages, p.pages),
    p.publication_types = CASE WHEN size($publication_types) > 0
                               THEN $publication_types ELSE p.publication_types END,
```

- [ ] **Step 6: Extend the `paper` dict in `triage_metadata.py`**

Add to the dict at lines 62-70 (the `**paper` splat means a missing key raises):

```python
        "venue": rec.get("venue"),
        "journal_name": rec.get("journal_name"),
        "volume": rec.get("volume"),
        "pages": rec.get("pages"),
        "publication_types": rec.get("publication_types") or [],
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS

- [ ] **Step 8: Verify the parameter set matches the query**

Run this guard, which catches the `**paper` splat mismatch class of bug:

```bash
uv run python -c "
import re
from pipeline.graph import research_port as rp
params = set(re.findall(r'\\\$(\\w+)', rp.WRITE_PAPER))
expected = {'id','document_id','title','year','arxiv_id','doi','s2_id','abstract','tldr',
            'citation_count','influential_citation_count','venue','journal_name','volume',
            'pages','publication_types','authors'}
assert params == expected, f'mismatch: {params ^ expected}'
print('WRITE_PAPER params OK')
"
```
Expected: `WRITE_PAPER params OK`

- [ ] **Step 9: Verify the full suite still passes**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add pipeline/graph/research_port.py pipeline/assets/triage_metadata.py tests/test_research_port.py
git commit -m "feat: persist S2 venue, journal, volume, pages and publication types on Paper"
```

---

## Task 5: Backfill venue data over the existing corpus

**Files:**
- Create: `scripts/backfill_venue.py`
- Test: manual dry run (this is a one-off operational script, matching `scripts/backfill_citations.py`, which has no unit tests)

**Interfaces:**
- Consumes: `rp.with_retry` (Task 2), `rp.clean_doi` (Task 1), `rp.WRITE_PAPER` (Tasks 3-4).
- Produces: nothing consumed by later tasks. Must be *run* before Task 14.

**Context:** 124 of 150 papers carry an arXiv ID or DOI and can be enriched. The remaining 26 have no identifier and stay unenriched by design. Model the script on `scripts/backfill_citations.py` — same header docstring style, same `sys.path` bootstrap, same `--dry-run` flag.

- [ ] **Step 1: Write the script**

Create `scripts/backfill_venue.py`:

```python
"""One-off backfill: re-fetch Semantic Scholar metadata for papers ingested before venue
data was persisted, and write venue/journal/volume/pages/publication_types onto the node.

Papers with neither an arXiv id nor a usable DOI cannot be enriched (S2 lookup is
identifier-only) and are reported as 'unidentifiable' rather than silently skipped.

Writes through research_port.WRITE_PAPER so coalesce semantics match the ingest asset
exactly — a paper whose lookup fails keeps whatever it already had.

Idempotent; free S2 API, 1 req/paper, retried on 429.
Run: uv run python scripts/backfill_venue.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.graph import research_port as rp

NEEDS_VENUE = """
MATCH (p:Paper)
WHERE p.venue IS NULL AND p.journal_name IS NULL
RETURN p.id AS id, p.document_id AS document_id,
       p.arxiv_id AS arxiv_id, p.doi AS doi, p.title AS title
ORDER BY p.id
"""

EXISTING_AUTHORS = """
MATCH (a:Author)-[:AUTHORED]->(p:Paper {id:$id}) RETURN collect(a.name) AS names
"""


def enrich(rec: dict, p: dict, authors: list[str]) -> dict:
    """Build WRITE_PAPER params. Every enrichment field may be None; coalesce protects
    the stored value, so a failed lookup is a no-op rather than a data loss."""
    return {
        "id": p["id"],
        "document_id": p["document_id"],
        "title": rec.get("title"),
        "year": rec.get("year"),
        "arxiv_id": rec.get("arxiv_id"),
        "doi": rp.clean_doi(rec.get("doi")),
        "s2_id": rec.get("s2_id"),
        "abstract": rec.get("abstract"),
        "tldr": rec.get("tldr"),
        "citation_count": rec.get("citation_count"),
        "influential_citation_count": rec.get("influential_citation_count"),
        "venue": rec.get("venue"),
        "journal_name": rec.get("journal_name"),
        "volume": rec.get("volume"),
        "pages": rec.get("pages"),
        "publication_types": rec.get("publication_types") or [],
        # Preserve existing authorship: WRITE_PAPER MERGEs these, so passing the names
        # already on the node is a no-op, while passing [] would simply add nothing.
        "authors": rec.get("authors") or [{"name": n, "s2_author_id": None} for n in authors],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    args = ap.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]),
    )
    database = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")

    enriched = no_record = unidentifiable = 0
    with driver, driver.session(database=database) as s:
        papers = [dict(r) for r in s.run(NEEDS_VENUE)]
        print(f"{len(papers)} papers missing venue data")

        for p in papers:
            doi = rp.clean_doi(p.get("doi"))
            arxiv = p.get("arxiv_id")
            if not arxiv and not doi:
                unidentifiable += 1
                print(f"  SKIP  {p['id']}  (no arxiv id, no usable doi)")
                continue

            rec = None
            if arxiv:
                rec = rp.with_retry(rp.lookup_by_arxiv, arxiv)
            if rec is None and doi:
                rec = rp.with_retry(rp.lookup_by_doi, doi)

            if not rec:
                no_record += 1
                print(f"  MISS  {p['id']}  (S2 returned nothing)")
                continue

            venue = rec.get("venue") or rec.get("journal_name")
            print(f"  OK    {p['id']}  venue={venue!r} "
                  f"types={rec.get('publication_types')}")
            if not args.dry_run:
                names = s.run(EXISTING_AUTHORS, id=p["id"]).single()["names"]
                s.run(rp.WRITE_PAPER, **enrich(rec, p, names))
            enriched += 1

    verb = "would enrich" if args.dry_run else "enriched"
    print(f"\n{verb}: {enriched}   no S2 record: {no_record}   "
          f"unidentifiable: {unidentifiable}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Lint and dry-run**

Run: `uv run ruff check scripts/backfill_venue.py && uv run python scripts/backfill_venue.py --dry-run`
Expected: no lint errors; a per-paper report ending in a summary line. Roughly 150 papers considered, ~26 reported `SKIP`.

Do **not** run without `--dry-run` yet — that happens in the sequencing step after review.

- [ ] **Step 3: Commit**

```bash
git add scripts/backfill_venue.py
git commit -m "feat: add backfill_venue script for pre-venue-enrichment papers"
```

---

## Task 6: Collision-safe paper-id migration

**Files:**
- Create: `scripts/migrate_paper_ids.py`
- Test: manual dry run

**Interfaces:**
- Consumes: `rp.clean_doi` (Task 1), `rp.compute_paper_id`.
- Produces: nothing consumed by later tasks.

**Context:** Task 1 changes what `compute_paper_id` produces for the papers whose DOI is a placeholder or URL-prefixed: `"doi:10.1145/nnnnnnn.nnnnnnn"` becomes `"arxiv:..."` or `"title:..."`. Without migration, re-ingesting one of those PDFs mints a *second* `Paper` node, and `DUP_CHECK` (which matches on `paper_id`) will not catch it.

`Paper.id` has a uniqueness constraint (`pipeline/graph/schema.py:124-125`), so a relabel that collides with an existing node would raise. Relationships are graph edges attached to the node, not to the id string, so they follow the node safely; only the scalar property changes.

- [ ] **Step 1: Write the script**

Create `scripts/migrate_paper_ids.py`:

```python
"""One-off migration: relabel Paper.id for nodes whose identity was computed from a DOI
that clean_doi now rejects (unfilled LaTeX template placeholders, URL-prefixed values).

Without this, re-ingesting one of those PDFs mints a SECOND Paper node — triage's
DUP_CHECK matches on the computed paper_id, which has changed.

Paper.id carries a uniqueness constraint, so a relabel that would collide with an existing
node is reported and skipped for manual resolution rather than attempted. Relationships are
attached to the node, not the id string, so they survive the relabel untouched.

Dry-run by default. Run: uv run python scripts/migrate_paper_ids.py [--apply]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.graph import research_port as rp

ALL_PAPERS = """
MATCH (p:Paper)
RETURN p.id AS id, p.doi AS doi, p.arxiv_id AS arxiv_id, p.title AS title
ORDER BY p.id
"""

RELABEL = "MATCH (p:Paper {id:$old}) SET p.id = $new, p.doi = $doi"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    args = ap.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]),
    )
    database = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")

    changed = collided = 0
    with driver, driver.session(database=database) as s:
        papers = [dict(r) for r in s.run(ALL_PAPERS)]
        existing = {p["id"] for p in papers}

        for p in papers:
            cleaned = rp.clean_doi(p.get("doi"))
            if cleaned == p.get("doi"):
                continue  # DOI unchanged -> identity unchanged
            try:
                new_id = rp.compute_paper_id(cleaned, p.get("arxiv_id"), p.get("title"))
            except ValueError:
                print(f"  STUCK    {p['id']}  (no doi, arxiv, or title — cannot recompute)")
                continue
            if new_id == p["id"]:
                continue
            if new_id in existing:
                collided += 1
                print(f"  COLLIDE  {p['id']}\n           -> {new_id} (already exists; "
                      f"resolve by hand)")
                continue
            print(f"  RELABEL  {p['id']}\n           -> {new_id}  (doi {p['doi']!r} -> "
                  f"{cleaned!r})")
            if args.apply:
                s.run(RELABEL, old=p["id"], new=new_id, doi=cleaned)
                existing.discard(p["id"])
                existing.add(new_id)
            changed += 1

    verb = "relabelled" if args.apply else "would relabel"
    print(f"\n{verb}: {changed}   collisions needing manual resolution: {collided}")
    return 1 if collided else 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Lint and dry-run**

Run: `uv run ruff check scripts/migrate_paper_ids.py && uv run python scripts/migrate_paper_ids.py`
Expected: no lint errors; roughly 3-4 `RELABEL` lines (the placeholder-DOI and URL-prefixed papers found in the audit), 0 collisions.

If any `COLLIDE` line appears, stop and report it rather than running `--apply` — a collision means two nodes describe the same paper and one must be merged by hand.

- [ ] **Step 3: Commit**

```bash
git add scripts/migrate_paper_ids.py
git commit -m "feat: add collision-safe paper-id migration for cleaned DOIs"
```

---

## Task 7: Expose the new properties to the MCP/server layer

**Files:**
- Modify: `server/queries.py:63-69` (`render_schema`), `:249-259` (`GET_PAPER`)
- Test: `tests/server/` (add to the existing server test module)

**Interfaces:**
- Consumes: the five properties from Task 4.
- Produces: nothing consumed by later tasks.

**Context:** Both lists are hardcoded. `render_schema()` is the property list shown to the query-generating LLM, and `GET_PAPER`'s projection enumerates returned properties explicitly. Without this, `/kg:ask` cannot see venue data even though it is in the graph.

- [ ] **Step 1: Write the failing test**

Create `tests/server/test_venue_visibility.py`:

```python
from server import queries


def test_render_schema_advertises_venue_properties():
    schema = queries.render_schema()
    for prop in ("venue", "journal_name", "publication_types"):
        assert prop in schema, f"{prop} invisible to the query-generating LLM"


def test_get_paper_projects_venue_properties():
    for prop in ("venue", "journal_name", "volume", "pages", "publication_types"):
        assert f".{prop}" in queries.GET_PAPER
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/server/test_venue_visibility.py -v`
Expected: FAIL — neither list mentions `venue`.

- [ ] **Step 3: Update `render_schema`**

Replace the `Key properties:` line for `Paper`:

```python
    lines += ["", "Key properties: Paper{id,title,year,doi,arxiv_id,abstract,tldr,"
              "citation_count,venue,journal_name,volume,pages,publication_types}, "
              "Concept{name,description,tags}, Definition{id,term,statement}, "
              "Result{id,kind,name,statement}, Chunk{id,text,position}, "
              "Notation{id,symbol_latex,meaning}, Proof{id,sketch,technique}, "
              "Book{id,title}, Chapter/Section{id,title}.",
              "Only Topic/Researcher/Idea are in the vocabulary but not yet populated."]
```

- [ ] **Step 4: Update `GET_PAPER`'s projection**

```cypher
RETURN p{.id, .title, .year, .doi, .arxiv_id, .s2_id, .abstract, .tldr,
         .citation_count, .influential_citation_count,
         .venue, .journal_name, .volume, .pages, .publication_types} AS paper,
       collect(DISTINCT a.name) AS authors, sm.json AS summary_json
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/server -v && uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add server/queries.py tests/server/test_venue_visibility.py
git commit -m "feat: expose venue properties to the MCP schema and GET_PAPER projection"
```

---

## Task 8: Attachment filename formatting

**Files:**
- Create: `pipeline/zotero/__init__.py` (empty), `pipeline/zotero/naming.py`
- Test: `tests/test_zotero_naming.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `attachment_filename(title: str | None, authors: list[str], year: int | str | None) -> str` in `pipeline.zotero.naming`. Used by Tasks 9, 12.

**Context:** Confirmed format is `Title - Author(s) - Year.pdf`. One author gives the surname; two give `Surname and Surname`; three or more give `Surname et al.`. The whole filename stays under 200 bytes, leaving headroom under the common 255-byte filesystem limit for the eventual WebDAV/NAS backend. A missing component is omitted along with its separator rather than rendered as `Unknown`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_zotero_naming.py`:

```python
from pipeline.zotero.naming import attachment_filename, surname


def test_surname_takes_the_last_whitespace_token():
    assert surname("Bruno Bouchard") == "Bouchard"
    assert surname("Jean-Pierre Fouque") == "Fouque"
    assert surname("Plato") == "Plato"
    assert surname("  Ada  Lovelace  ") == "Lovelace"


def test_one_author():
    assert attachment_filename("Deep BSDE", ["Bruno Bouchard"], 2019) == \
        "Deep BSDE - Bouchard - 2019.pdf"


def test_two_authors_joined_with_and():
    assert attachment_filename("Deep BSDE", ["Bruno Bouchard", "Ada Lovelace"], 2019) == \
        "Deep BSDE - Bouchard and Lovelace - 2019.pdf"


def test_three_or_more_authors_use_et_al():
    names = ["Bruno Bouchard", "Ada Lovelace", "Alan Turing"]
    assert attachment_filename("Deep BSDE", names, 2019) == \
        "Deep BSDE - Bouchard et al. - 2019.pdf"
    assert attachment_filename("Deep BSDE", names + ["Grace Hopper"], 2019) == \
        "Deep BSDE - Bouchard et al. - 2019.pdf"


def test_path_hostile_characters_are_replaced():
    got = attachment_filename("A/B: A Study", ["Ada Lovelace"], 2020)
    assert "/" not in got and ":" not in got
    assert got == "A-B- A Study - Lovelace - 2020.pdf"


def test_missing_year_omits_the_segment_and_separator():
    assert attachment_filename("Deep BSDE", ["Bruno Bouchard"], None) == \
        "Deep BSDE - Bouchard.pdf"


def test_missing_authors_omits_the_segment():
    assert attachment_filename("Deep BSDE", [], 2019) == "Deep BSDE - 2019.pdf"


def test_missing_title_falls_back_to_untitled():
    assert attachment_filename(None, ["Bruno Bouchard"], 2019) == "Untitled - Bouchard - 2019.pdf"


def test_long_title_truncates_under_200_bytes():
    got = attachment_filename("Lorem ipsum dolor sit amet " * 20, ["Ada Lovelace"], 2020)
    assert len(got.encode("utf-8")) <= 200
    assert got.endswith(" - Lovelace - 2020.pdf"), "author/year must survive truncation"


def test_whitespace_runs_collapse_and_edges_strip():
    assert attachment_filename("  A   Study  ", ["Ada Lovelace"], 2020) == \
        "A Study - Lovelace - 2020.pdf"


def test_trailing_dots_are_stripped_from_the_title():
    assert attachment_filename("A Study...", ["Ada Lovelace"], 2020) == \
        "A Study - Lovelace - 2020.pdf"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_zotero_naming.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pipeline.zotero'`

- [ ] **Step 3: Write the implementation**

Create `pipeline/zotero/__init__.py` as an empty file, then `pipeline/zotero/naming.py`:

```python
"""Attachment filename formatting: "Title - Author(s) - Year.pdf".

Pure — no I/O, no Zotero types. The 200-byte cap leaves headroom under the common
255-byte filesystem limit, which matters once files land on a WebDAV/NAS backend.
"""
from __future__ import annotations

import re

MAX_FILENAME_BYTES = 200
_PATH_HOSTILE = re.compile(r'[/\\:*?"<>|\x00-\x1f]')
_WHITESPACE = re.compile(r"\s+")


def surname(name: str) -> str:
    """Last whitespace-separated token. A single-token name is returned whole."""
    parts = _WHITESPACE.sub(" ", (name or "").strip()).split(" ")
    return parts[-1] if parts and parts[0] else ""


def _sanitize(text: str) -> str:
    cleaned = _PATH_HOSTILE.sub("-", text or "")
    cleaned = _WHITESPACE.sub(" ", cleaned).strip()
    return cleaned.strip(".").strip()


def author_segment(authors: list[str]) -> str:
    """'' | 'Surname' | 'A and B' | 'A et al.' — Zotero's own citation convention."""
    names = [surname(a) for a in (authors or []) if surname(a)]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{names[0]} et al."


def attachment_filename(title: str | None, authors: list[str],
                        year: int | str | None) -> str:
    """Build the attachment filename, omitting absent segments and their separators."""
    safe_title = _sanitize(title or "") or "Untitled"
    segments = [_sanitize(author_segment(authors)), _sanitize(str(year)) if year else ""]
    tail = "".join(f" - {s}" for s in segments if s) + ".pdf"

    budget = MAX_FILENAME_BYTES - len(tail.encode("utf-8"))
    encoded = safe_title.encode("utf-8")
    if len(encoded) > budget:
        # Cut on a byte boundary, then drop any partial trailing UTF-8 sequence.
        safe_title = encoded[:max(budget, 0)].decode("utf-8", errors="ignore").strip()
    return f"{safe_title}{tail}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_naming.py -v`
Expected: PASS (11 tests)

- [ ] **Step 5: Verify lint**

Run: `uv run ruff check pipeline/zotero tests/test_zotero_naming.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/__init__.py pipeline/zotero/naming.py tests/test_zotero_naming.py
git commit -m "feat: Zotero attachment filename formatting"
```

---

## Task 9: Item construction, type mapping, and dedup matching

**Files:**
- Create: `pipeline/zotero/items.py`
- Test: `tests/test_zotero_items.py`

**Interfaces:**
- Consumes: `rp.clean_doi` (Task 1), `rp.normalize_title`, `attachment_filename` (Task 8).
- Produces, in `pipeline.zotero.items`:
  - `paper_item(paper: dict, authors: list[str], collection_key: str) -> dict`
  - `book_item(book: dict, authors: list[str], collection_key: str) -> dict`
  - `attachment_item(parent_key: str, filename: str) -> dict`
  - `match_existing(candidates: list[dict], doi: str | None, arxiv_id: str | None, title: str | None) -> str | None`
  - `split_creator(name: str) -> dict`

  Used by Tasks 11, 12, 14.

**Context — item type mapping table (from the spec):**

| Condition | `itemType` | Fields beyond title/creators/date |
|---|---|---|
| `publication_types` contains `Conference` | `conferencePaper` | `proceedingsTitle` ← venue, `DOI`, `pages` |
| `publication_types` contains `JournalArticle`, or a genuine publisher DOI exists | `journalArticle` | `publicationTitle` ← `journal_name` or `venue`, `volume`, `pages`, `DOI` |
| `arxiv_id` present, no publisher DOI | `preprint` | `repository` = "arXiv", `archiveID` = `arXiv:<id>`, `url` |
| none of the above | `preprint` | title, creators, date only |

`Conference` is tested **before** `JournalArticle` because S2 sometimes returns both for proceedings papers a journal later indexed, and the proceedings is the more specific truth. `tldr` is deliberately never written to Zotero: it is a generated summary, not bibliographic data.

- [ ] **Step 1: Write the failing test**

Create `tests/test_zotero_items.py`:

```python
from pipeline.zotero.items import (
    attachment_item, book_item, match_existing, paper_item, split_creator,
)

JOURNAL_PAPER = {
    "title": "Equilibrium Returns with Transaction Costs",
    "year": 2019,
    "doi": "10.1007/s00780-019-00397-0",
    "arxiv_id": "1707.08464",
    "venue": "Finance and Stochastics",
    "journal_name": "Finance and Stochastics",
    "volume": "23",
    "pages": "707-744",
    "publication_types": ["JournalArticle"],
    "abstract": "We study...",
    "tldr": "A short generated summary.",
}


def test_split_creator_two_part_name():
    assert split_creator("Bruno Bouchard") == {
        "creatorType": "author", "firstName": "Bruno", "lastName": "Bouchard"}


def test_split_creator_three_part_name_splits_on_last_space():
    assert split_creator("Jean Pierre Fouque") == {
        "creatorType": "author", "firstName": "Jean Pierre", "lastName": "Fouque"}


def test_split_creator_single_token_uses_zotero_single_field_mode():
    assert split_creator("Plato") == {"creatorType": "author", "name": "Plato"}


def test_journal_article_mapping():
    item = paper_item(JOURNAL_PAPER, ["Bruno Bouchard"], "COLL1")
    assert item["itemType"] == "journalArticle"
    assert item["publicationTitle"] == "Finance and Stochastics"
    assert item["volume"] == "23"
    assert item["pages"] == "707-744"
    assert item["DOI"] == "10.1007/s00780-019-00397-0"
    assert item["date"] == "2019"
    assert item["collections"] == ["COLL1"]
    assert item["abstractNote"] == "We study..."


def test_tldr_is_never_written_to_zotero():
    item = paper_item(JOURNAL_PAPER, ["Bruno Bouchard"], "COLL1")
    assert "A short generated summary." not in str(item)


def test_conference_wins_over_journal_article():
    paper = dict(JOURNAL_PAPER, publication_types=["JournalArticle", "Conference"])
    item = paper_item(paper, [], "COLL1")
    assert item["itemType"] == "conferencePaper"
    assert item["proceedingsTitle"] == "Finance and Stochastics"
    assert "publicationTitle" not in item


def test_publisher_doi_alone_implies_journal_article():
    paper = dict(JOURNAL_PAPER, publication_types=[])
    assert paper_item(paper, [], "C")["itemType"] == "journalArticle"


def test_journal_name_falls_back_to_venue():
    paper = dict(JOURNAL_PAPER, journal_name=None)
    assert paper_item(paper, [], "C")["publicationTitle"] == "Finance and Stochastics"


def test_arxiv_only_maps_to_preprint():
    paper = {"title": "A Preprint", "year": 2025, "arxiv_id": "2503.13804",
             "doi": None, "publication_types": []}
    item = paper_item(paper, [], "C")
    assert item["itemType"] == "preprint"
    assert item["repository"] == "arXiv"
    assert item["archiveID"] == "arXiv:2503.13804"
    assert item["url"] == "https://arxiv.org/abs/2503.13804"


def test_arxiv_doi_is_not_a_publisher_doi():
    """10.48550/arXiv.* is arXiv's own DOI — the paper is still a preprint."""
    paper = {"title": "T", "year": 2023, "arxiv_id": "2305.16261",
             "doi": "10.48550/arXiv.2305.16261", "publication_types": []}
    assert paper_item(paper, [], "C")["itemType"] == "preprint"


def test_ssrn_doi_is_not_a_publisher_doi():
    paper = {"title": "T", "year": 2020, "arxiv_id": "2005.02633",
             "doi": "10.2139/ssrn.3594076", "publication_types": []}
    assert paper_item(paper, [], "C")["itemType"] == "preprint"


def test_placeholder_doi_never_reaches_the_item():
    paper = {"title": "T", "year": 2017, "arxiv_id": None,
             "doi": "10.1145/NNNNNNN.NNNNNNN", "publication_types": []}
    item = paper_item(paper, [], "C")
    assert item["itemType"] == "preprint"
    assert "DOI" not in item


def test_identifierless_paper_maps_to_bare_preprint():
    paper = {"title": "Lecture Notes", "year": None, "arxiv_id": None, "doi": None,
             "publication_types": []}
    item = paper_item(paper, ["Ada Lovelace"], "C")
    assert item["itemType"] == "preprint"
    assert item["title"] == "Lecture Notes"
    assert "date" not in item
    assert "repository" not in item


def test_book_mapping():
    book = {"title": "Probability with Martingales", "year": 1991,
            "publisher": "Cambridge University Press", "edition": "1st",
            "isbn": "9780521406055"}
    item = book_item(book, ["David Williams"], "BOOKS")
    assert item["itemType"] == "book"
    assert item["publisher"] == "Cambridge University Press"
    assert item["edition"] == "1st"
    assert item["ISBN"] == "9780521406055"
    assert item["collections"] == ["BOOKS"]


def test_attachment_item_shape():
    att = attachment_item("PARENT1", "A Study - Lovelace - 2020.pdf")
    assert att == {
        "itemType": "attachment", "parentItem": "PARENT1", "linkMode": "imported_file",
        "title": "A Study - Lovelace - 2020.pdf",
        "filename": "A Study - Lovelace - 2020.pdf",
        "contentType": "application/pdf",
    }


# --- dedup matching ---------------------------------------------------------------

CANDIDATES = [
    {"key": "K_DOI", "data": {"DOI": "10.1007/s00780-019-00397-0", "title": "Something Else"}},
    {"key": "K_ARXIV", "data": {"archiveID": "arXiv:1707.08464", "title": "Other"}},
    {"key": "K_TITLE", "data": {"title": "Equilibrium Returns with Transaction Costs"}},
]


def test_doi_match_wins():
    assert match_existing(CANDIDATES, "10.1007/s00780-019-00397-0", "1707.08464",
                          "Equilibrium Returns with Transaction Costs") == "K_DOI"


def test_arxiv_match_beats_title():
    assert match_existing(CANDIDATES, None, "1707.08464",
                          "Equilibrium Returns with Transaction Costs") == "K_ARXIV"


def test_title_match_is_the_last_resort():
    assert match_existing(CANDIDATES, None, None,
                          "Equilibrium Returns with Transaction Costs") == "K_TITLE"


def test_title_match_is_case_and_whitespace_insensitive():
    assert match_existing(CANDIDATES, None, None,
                          "  EQUILIBRIUM   returns with Transaction Costs ") == "K_TITLE"


def test_arxiv_match_ignores_version_suffix():
    assert match_existing(CANDIDATES, None, "1707.08464v3", None) == "K_ARXIV"


def test_arxiv_match_also_reads_the_url_field():
    cands = [{"key": "K_URL", "data": {"url": "https://arxiv.org/abs/2503.13804"}}]
    assert match_existing(cands, None, "2503.13804", None) == "K_URL"


def test_placeholder_doi_is_not_used_for_matching():
    cands = [{"key": "K", "data": {"DOI": "10.1145/NNNNNNN.NNNNNNN"}}]
    assert match_existing(cands, "10.1145/NNNNNNN.NNNNNNN", None, None) is None


def test_no_match_returns_none():
    assert match_existing(CANDIDATES, "10.9999/nope", "0000.00000", "Unrelated") is None


def test_empty_candidates_returns_none():
    assert match_existing([], "10.1007/x", "1707.08464", "T") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_zotero_items.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pipeline.zotero.items'`

- [ ] **Step 3: Write the implementation**

Create `pipeline/zotero/items.py`:

```python
"""Zotero item construction and deduplication matching.

Pure — builds dicts and compares them. All HTTP lives in client.py, so every rule here
(item-type mapping, creator splitting, match precedence) is unit-testable without mocks.
"""
from __future__ import annotations

from pipeline.graph.research_port import clean_doi, normalize_title, strip_arxiv_version

# DOI registrants that mint DOIs for preprints, not publications. A paper carrying only
# one of these is still a preprint.
_PREPRINT_DOI_PREFIXES = ("10.48550/", "10.2139/ssrn")


def publisher_doi(doi: str | None) -> str | None:
    """A cleaned DOI, unless it belongs to a preprint registrar (arXiv, SSRN)."""
    d = clean_doi(doi)
    if d is None:
        return None
    return None if d.lower().startswith(_PREPRINT_DOI_PREFIXES) else d


def split_creator(name: str) -> dict:
    """Zotero two-field mode, splitting on the last space. A single-token name uses
    Zotero's single-field mode rather than guessing at a given/family split."""
    parts = (name or "").strip().split()
    if len(parts) < 2:
        return {"creatorType": "author", "name": " ".join(parts)}
    return {"creatorType": "author", "firstName": " ".join(parts[:-1]),
            "lastName": parts[-1]}


def _base(title: str | None, authors: list[str], year, collection_key: str) -> dict:
    item = {
        "title": title or "Untitled",
        "creators": [split_creator(a) for a in (authors or []) if a and a.strip()],
        "collections": [collection_key],
        "tags": [],
        "relations": {},
    }
    if year:
        item["date"] = str(year)
    return item


def paper_item(paper: dict, authors: list[str], collection_key: str) -> dict:
    """Map a Paper node onto a Zotero item, typed from publication_types then DOI shape."""
    item = _base(paper.get("title"), authors, paper.get("year"), collection_key)
    types = [t.lower() for t in (paper.get("publication_types") or [])]
    doi = publisher_doi(paper.get("doi"))
    venue = paper.get("journal_name") or paper.get("venue")
    arxiv = paper.get("arxiv_id")

    if paper.get("abstract"):
        item["abstractNote"] = paper["abstract"]

    # Conference before journal: S2 returns both for proceedings a journal later indexed,
    # and the proceedings is the more specific truth.
    if "conference" in types:
        item["itemType"] = "conferencePaper"
        if venue:
            item["proceedingsTitle"] = venue
        if paper.get("pages"):
            item["pages"] = paper["pages"]
        if doi:
            item["DOI"] = doi
        return item

    if "journalarticle" in types or doi:
        item["itemType"] = "journalArticle"
        if venue:
            item["publicationTitle"] = venue
        if paper.get("volume"):
            item["volume"] = paper["volume"]
        if paper.get("pages"):
            item["pages"] = paper["pages"]
        if doi:
            item["DOI"] = doi
        return item

    item["itemType"] = "preprint"
    if arxiv:
        bare = strip_arxiv_version(arxiv)
        item["repository"] = "arXiv"
        item["archiveID"] = f"arXiv:{bare}"
        item["url"] = f"https://arxiv.org/abs/{bare}"
    return item


def book_item(book: dict, authors: list[str], collection_key: str) -> dict:
    item = _base(book.get("title"), authors, book.get("year"), collection_key)
    item["itemType"] = "book"
    for src, dest in (("publisher", "publisher"), ("edition", "edition"), ("isbn", "ISBN")):
        if book.get(src):
            item[dest] = book[src]
    return item


def attachment_item(parent_key: str, filename: str) -> dict:
    return {
        "itemType": "attachment",
        "parentItem": parent_key,
        "linkMode": "imported_file",
        "title": filename,
        "filename": filename,
        "contentType": "application/pdf",
    }


def match_existing(candidates: list[dict], doi: str | None, arxiv_id: str | None,
                   title: str | None) -> str | None:
    """Return the Zotero item key of an existing item describing this work, or None.

    Precedence: cleaned DOI, then arXiv id (version-insensitive, checking archiveID and
    url), then normalized title. A placeholder DOI is never used for matching.
    """
    target_doi = clean_doi(doi)
    if target_doi:
        want = target_doi.lower()
        for c in candidates:
            cand = clean_doi((c.get("data") or {}).get("DOI"))
            if cand and cand.lower() == want:
                return c["key"]

    if arxiv_id:
        bare = strip_arxiv_version(arxiv_id.strip().lower())
        for c in candidates:
            data = c.get("data") or {}
            haystack = f"{data.get('archiveID') or ''} {data.get('url') or ''}".lower()
            if bare and bare in haystack:
                return c["key"]

    if title:
        want = normalize_title(title)
        for c in candidates:
            cand_title = (c.get("data") or {}).get("title")
            if cand_title and normalize_title(cand_title) == want:
                return c["key"]

    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_items.py -v`
Expected: PASS (24 tests)

- [ ] **Step 5: Verify lint and full suite**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/items.py tests/test_zotero_items.py
git commit -m "feat: Zotero item construction, type mapping, and dedup matching"
```

---

## Task 10: `ZoteroResource` and the collections bootstrap

**Files:**
- Modify: `pipeline/runtime/resources.py` (add `ZoteroResource`)
- Modify: `.env.example` (add `ZOTERO_API_KEY`, `ZOTERO_USER_ID`)
- Create: `pipeline/zotero/client.py` (transport + collections)
- Test: `tests/test_zotero_client.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `ZoteroResource` in `pipeline.runtime.resources` with fields `api_key`, `user_id`, `base_url`, `request_timeout`, and method `get_client() -> ZoteroClient`.
  - `ZoteroClient(api_key, user_id, base_url="https://api.zotero.org", timeout=60.0)` in `pipeline.zotero.client`, with `ensure_collections() -> dict[str, str]` returning `{"papers": <key>, "books": <key>}`.
  - `ZoteroClientError` (raised on 4xx) and `ZoteroTransientError` (raised on exhausted 429/5xx retries).

  Used by Tasks 11, 12, 13, 14.

**Context:** One top-level `Alethograph` collection with `Papers` and `Books` subcollections, created idempotently: list, match by name and parent, create only what is missing. The user's library already contains 45 collections; nothing outside `Alethograph` is created, renamed, or deleted.

- [ ] **Step 1: Write the failing test**

Create `tests/test_zotero_client.py`:

```python
import pytest

from pipeline.zotero.client import ZoteroClient, ZoteroClientError, ZoteroTransientError


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else []
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json


class FakeHTTP:
    """Records requests and replays queued responses, so tests assert on call shape."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def client(http):
    return ZoteroClient(api_key="KEY", user_id="5666366", http=http)


def test_auth_header_is_sent_and_key_is_not_in_the_url():
    http = FakeHTTP([FakeResponse(json_data=[])])
    c = client(http)
    c.list_collections()
    call = http.calls[0]
    assert call["headers"]["Zotero-API-Key"] == "KEY"
    assert "KEY" not in call["url"]


def test_ensure_collections_creates_all_three_when_library_is_empty():
    http = FakeHTTP([
        FakeResponse(json_data=[]),                                        # list
        FakeResponse(json_data={"successful": {"0": {"key": "ALEPH"}}}),   # create Alethograph
        FakeResponse(json_data={"successful": {"0": {"key": "PAP"},
                                               "1": {"key": "BKS"}}}),    # create children
    ])
    c = client(http)
    assert c.ensure_collections() == {"papers": "PAP", "books": "BKS"}


def test_ensure_collections_is_idempotent_when_all_exist():
    existing = [
        {"key": "ALEPH", "data": {"name": "Alethograph", "parentCollection": False}},
        {"key": "PAP", "data": {"name": "Papers", "parentCollection": "ALEPH"}},
        {"key": "BKS", "data": {"name": "Books", "parentCollection": "ALEPH"}},
        {"key": "OTHER", "data": {"name": "Papers", "parentCollection": "SOMEWHERE"}},
    ]
    http = FakeHTTP([FakeResponse(json_data=existing)])
    c = client(http)
    assert c.ensure_collections() == {"papers": "PAP", "books": "BKS"}
    assert len(http.calls) == 1, "nothing should be created when all three exist"


def test_ensure_collections_ignores_same_named_collection_under_another_parent():
    """The library has 45 unrelated collections; a stray 'Papers' elsewhere must not match."""
    existing = [
        {"key": "ALEPH", "data": {"name": "Alethograph", "parentCollection": False}},
        {"key": "STRAY", "data": {"name": "Papers", "parentCollection": "UNRELATED"}},
    ]
    http = FakeHTTP([
        FakeResponse(json_data=existing),
        FakeResponse(json_data={"successful": {"0": {"key": "PAP"}, "1": {"key": "BKS"}}}),
    ])
    c = client(http)
    assert c.ensure_collections() == {"papers": "PAP", "books": "BKS"}


def test_list_collections_paginates_until_short_page():
    page1 = [{"key": f"K{i}", "data": {"name": str(i), "parentCollection": False}}
             for i in range(100)]
    page2 = [{"key": "LAST", "data": {"name": "last", "parentCollection": False}}]
    http = FakeHTTP([FakeResponse(json_data=page1), FakeResponse(json_data=page2)])
    c = client(http)
    assert len(c.list_collections()) == 101
    assert http.calls[1]["params"]["start"] == 100


def test_client_error_raises():
    http = FakeHTTP([FakeResponse(status_code=403, text="Forbidden")])
    with pytest.raises(ZoteroClientError):
        client(http).list_collections()


def test_429_retries_then_raises_transient(monkeypatch):
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: None)
    http = FakeHTTP([FakeResponse(status_code=429, headers={"Retry-After": "1"})
                     for _ in range(4)])
    with pytest.raises(ZoteroTransientError):
        client(http).list_collections()


def test_429_then_success_succeeds(monkeypatch):
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: None)
    http = FakeHTTP([
        FakeResponse(status_code=429, headers={"Retry-After": "1"}),
        FakeResponse(json_data=[]),
    ])
    assert client(http).list_collections() == []


def test_backoff_header_is_honoured(monkeypatch):
    slept = []
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: slept.append(s))
    http = FakeHTTP([
        FakeResponse(json_data=[], headers={"Backoff": "3"}),
        FakeResponse(json_data=[]),
    ])
    c = client(http)
    c.list_collections()
    c.list_collections()
    assert 3 in slept, "a Backoff header must delay the NEXT request"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_zotero_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pipeline.zotero.client'`

- [ ] **Step 3: Write the client transport and collections bootstrap**

Create `pipeline/zotero/client.py`:

```python
"""Zotero Web API v3 transport.

Hand-rolled on `requests`, mirroring how research_port.py handles Semantic Scholar —
pyzotero would be a new dependency for a handful of endpoints. All item-shaping logic
lives in items.py; this module only moves bytes.

The API key is passed in the Zotero-API-Key header and never placed in a URL, so it
cannot leak through request logging.
"""
from __future__ import annotations

import logging
import time

import requests
from requests import RequestException

log = logging.getLogger(__name__)

PAGE_SIZE = 100
BATCH_LIMIT = 50  # Zotero caps item/collection creation at 50 per request.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 4

COLLECTION_ROOT = "Alethograph"
COLLECTION_PAPERS = "Papers"
COLLECTION_BOOKS = "Books"


class ZoteroClientError(RuntimeError):
    """A 4xx that will not resolve by retrying: bad payload, revoked key, missing item."""


class ZoteroTransientError(RuntimeError):
    """Throttling or a 5xx that survived every retry. Callers log and continue."""


class ZoteroClient:
    def __init__(self, api_key: str, user_id: str,
                 base_url: str = "https://api.zotero.org",
                 timeout: float = 60.0, http=None):
        self.api_key = api_key
        self.user_id = user_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._http = http or requests.request
        self._backoff_until = 0.0

    # --- transport ---------------------------------------------------------------

    @property
    def prefix(self) -> str:
        return f"{self.base_url}/users/{self.user_id}"

    def _headers(self, extra: dict | None = None) -> dict:
        headers = {"Zotero-API-Key": self.api_key, "Zotero-API-Version": "3"}
        headers.update(extra or {})
        return headers

    def request(self, method: str, path: str, *, params: dict | None = None,
                json_body=None, data=None, headers: dict | None = None,
                absolute: bool = False):
        """One API call with Backoff/Retry-After handling and bounded retries."""
        url = path if absolute else f"{self.prefix}{path}"
        for attempt in range(_MAX_ATTEMPTS):
            wait = self._backoff_until - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._backoff_until = 0.0

            try:
                resp = self._http(method, url, params=params, json=json_body, data=data,
                                  headers=self._headers(headers), timeout=self.timeout)
            except RequestException as exc:
                log.warning("Zotero %s %s failed: %s", method, path, exc)
                time.sleep(2.0 * (attempt + 1))
                continue

            backoff = resp.headers.get("Backoff")
            if backoff:
                self._backoff_until = time.monotonic() + float(backoff)

            if resp.status_code in _RETRYABLE_STATUS:
                delay = float(resp.headers.get("Retry-After") or 2.0 * (attempt + 1))
                log.warning("Zotero %s %s throttled (HTTP %s), sleeping %ss",
                            method, path, resp.status_code, delay)
                time.sleep(delay)
                continue
            if resp.status_code >= 400:
                raise ZoteroClientError(
                    f"Zotero {method} {path} -> HTTP {resp.status_code}: {resp.text[:300]}")
            return resp

        raise ZoteroTransientError(
            f"Zotero {method} {path} exhausted {_MAX_ATTEMPTS} attempts")

    def _paginate(self, path: str, params: dict | None = None) -> list[dict]:
        out: list[dict] = []
        start = 0
        while True:
            page = self.request("GET", path,
                                params={**(params or {}), "limit": PAGE_SIZE,
                                        "start": start, "format": "json"}).json()
            out.extend(page)
            if len(page) < PAGE_SIZE:
                return out
            start += PAGE_SIZE

    # --- collections -------------------------------------------------------------

    def list_collections(self) -> list[dict]:
        return self._paginate("/collections")

    def create_collections(self, payload: list[dict]) -> list[str]:
        """Create collections, returning their keys in submission order."""
        resp = self.request("POST", "/collections", json_body=payload).json()
        successful = resp.get("successful") or {}
        return [successful[str(i)]["key"] for i in range(len(payload))]

    def ensure_collections(self) -> dict[str, str]:
        """Idempotently ensure Alethograph/{Papers,Books} exist. Returns their keys.

        Matches on name AND parent, so an unrelated 'Papers' collection elsewhere in the
        library is never adopted.
        """
        collections = self.list_collections()

        def find(name: str, parent) -> str | None:
            for c in collections:
                data = c.get("data") or {}
                if data.get("name") == name and (data.get("parentCollection") or False) == parent:
                    return c["key"]
            return None

        root = find(COLLECTION_ROOT, False)
        if root is None:
            root = self.create_collections([{"name": COLLECTION_ROOT}])[0]

        papers = find(COLLECTION_PAPERS, root)
        books = find(COLLECTION_BOOKS, root)

        missing = [(name, dest) for name, dest in
                   ((COLLECTION_PAPERS, "papers"), (COLLECTION_BOOKS, "books"))
                   if (papers if dest == "papers" else books) is None]
        if missing:
            keys = self.create_collections(
                [{"name": name, "parentCollection": root} for name, _ in missing])
            for (_, dest), key in zip(missing, keys):
                if dest == "papers":
                    papers = key
                else:
                    books = key

        return {"papers": papers, "books": books}
```

- [ ] **Step 4: Add `ZoteroResource`**

Append to `pipeline/runtime/resources.py`, after `AnthropicResource`:

```python
class ZoteroResource(ConfigurableResource):
    """Zotero Web API v3 — personal library sync for ingested papers and books."""
    api_key: str = Field(default_factory=lambda: os.environ.get("ZOTERO_API_KEY", ""))
    user_id: str = Field(default_factory=lambda: os.environ.get("ZOTERO_USER_ID", ""))
    base_url: str = "https://api.zotero.org"
    request_timeout: float = 60.0

    def get_client(self):
        from pipeline.zotero.client import ZoteroClient
        return ZoteroClient(api_key=self.api_key, user_id=self.user_id,
                            base_url=self.base_url, timeout=self.request_timeout)
```

- [ ] **Step 5: Document the new environment keys**

Append to `.env.example`:

```bash
# Zotero Web API — personal library sync (see docs/superpowers/specs/
# 2026-08-31-zotero-library-sync-design.md). Create a key with write access at
# https://www.zotero.org/settings/keys
ZOTERO_API_KEY=
ZOTERO_USER_ID=
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_client.py -v`
Expected: PASS (9 tests)

- [ ] **Step 7: Verify the key never appears in a URL**

Run: `uv run pytest tests/test_zotero_client.py::test_auth_header_is_sent_and_key_is_not_in_the_url -v`
Expected: PASS

- [ ] **Step 8: Verify lint and full suite**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add pipeline/zotero/client.py pipeline/runtime/resources.py .env.example tests/test_zotero_client.py
git commit -m "feat: Zotero client transport, retry policy, and collections bootstrap"
```

---

## Task 11: Item creation, candidate search, and three-step file upload

**Files:**
- Modify: `pipeline/zotero/client.py` (add item and file methods)
- Test: `tests/test_zotero_client.py`

**Interfaces:**
- Consumes: `ZoteroClient` (Task 10).
- Produces, as `ZoteroClient` methods:
  - `search_candidates(title: str | None) -> list[dict]`
  - `library_index() -> list[dict]`
  - `create_items(payload: list[dict]) -> list[str]`
  - `add_to_collection(item_key: str, collection_key: str) -> bool`
  - `upload_attachment(item_key: str, filename: str, data: bytes) -> str` returning `"uploaded"` | `"exists"`

  Used by Tasks 12, 14.

**Context — the documented three-step upload:**
1. **Authorize** — `POST /users/<id>/items/<itemKey>/file` with `md5`, `filename`, `filesize`, `mtime` (**milliseconds**), header `If-None-Match: *`. Content type must be `application/x-www-form-urlencoded`.
2. **Upload** — if the response carries upload params, POST `prefix` + file bytes + `suffix` to the returned `url` with the returned `contentType`. If the response is `{"exists": 1}`, Zotero already holds that file and has associated it; stop.
3. **Register** — `POST /users/<id>/items/<itemKey>/file` with `upload=<uploadKey>` and the same conditional header. A `204` confirms.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_zotero_client.py`:

```python
def test_create_items_returns_keys_in_submission_order():
    http = FakeHTTP([FakeResponse(json_data={
        "successful": {"0": {"key": "A"}, "1": {"key": "B"}}, "failed": {}})])
    assert client(http).create_items([{"itemType": "preprint"},
                                      {"itemType": "book"}]) == ["A", "B"]


def test_create_items_raises_when_zotero_reports_a_failure():
    http = FakeHTTP([FakeResponse(json_data={
        "successful": {}, "failed": {"0": {"code": 400, "message": "bad field"}}})])
    with pytest.raises(ZoteroClientError, match="bad field"):
        client(http).create_items([{"itemType": "nonsense"}])


def test_create_items_rejects_batches_over_the_limit():
    with pytest.raises(ValueError, match="50"):
        client(FakeHTTP([])).create_items([{"itemType": "preprint"}] * 51)


def test_add_to_collection_patches_only_the_collections_field():
    http = FakeHTTP([
        FakeResponse(json_data={"data": {"key": "ITEM", "version": 7,
                                         "collections": ["OLD"], "title": "Keep me"}}),
        FakeResponse(status_code=204),
    ])
    assert client(http).add_to_collection("ITEM", "NEW") is True
    patch = http.calls[1]
    assert patch["method"] == "PATCH"
    assert patch["json"] == {"collections": ["OLD", "NEW"]}
    assert "title" not in patch["json"], "must not rewrite the user's own metadata"
    assert patch["headers"]["If-Unmodified-Since-Version"] == "7"


def test_add_to_collection_is_a_noop_when_already_a_member():
    http = FakeHTTP([FakeResponse(json_data={
        "data": {"key": "ITEM", "version": 7, "collections": ["NEW"]}})])
    assert client(http).add_to_collection("ITEM", "NEW") is False
    assert len(http.calls) == 1, "no PATCH when the item is already in the collection"


def test_upload_attachment_short_circuits_when_file_exists():
    http = FakeHTTP([FakeResponse(json_data={"exists": 1})])
    assert client(http).upload_attachment("ITEM", "f.pdf", b"data") == "exists"
    assert len(http.calls) == 1, "exists:1 means no upload and no registration"


def test_upload_attachment_runs_all_three_steps():
    http = FakeHTTP([
        FakeResponse(json_data={"url": "https://s3.example/put", "contentType": "text/plain",
                                "prefix": "PRE", "suffix": "SUF", "uploadKey": "UK"}),
        FakeResponse(status_code=201),
        FakeResponse(status_code=204),
    ])
    assert client(http).upload_attachment("ITEM", "f.pdf", b"data") == "uploaded"
    assert len(http.calls) == 3
    assert http.calls[1]["data"] == b"PREdataSUF"
    assert http.calls[2]["data"]["upload"] == "UK"


def test_upload_authorization_sends_md5_filesize_and_millisecond_mtime():
    import hashlib
    http = FakeHTTP([FakeResponse(json_data={"exists": 1})])
    client(http).upload_attachment("ITEM", "f.pdf", b"data")
    sent = http.calls[0]["data"]
    assert sent["md5"] == hashlib.md5(b"data").hexdigest()
    assert sent["filesize"] == 4
    assert sent["filename"] == "f.pdf"
    assert sent["mtime"] > 10**12, "mtime must be milliseconds, not seconds"
    assert http.calls[0]["headers"]["If-None-Match"] == "*"


def test_search_candidates_returns_empty_without_a_title():
    http = FakeHTTP([])
    assert client(http).search_candidates(None) == []
    assert http.calls == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_zotero_client.py -v -k "create_items or add_to_collection or upload or search"`
Expected: FAIL — the methods do not exist.

- [ ] **Step 3: Write the implementation**

Add to `pipeline/zotero/client.py` (imports: `hashlib`, `time` is already imported):

```python
    # --- items -------------------------------------------------------------------

    def search_candidates(self, title: str | None, limit: int = 25) -> list[dict]:
        """Candidate items for deduplication, found by title. One request.

        Used on the per-ingest path; the backfill uses library_index() instead so it
        does not issue one search per item.
        """
        if not title or not title.strip():
            return []
        return self.request("GET", "/items", params={
            "q": title.strip(), "qmode": "titleCreatorYear", "limit": limit,
            "format": "json", "itemType": "-attachment || note",
        }).json()

    def library_index(self) -> list[dict]:
        """Every non-attachment, non-note item in the library. One paginated sweep,
        reused across a whole backfill run."""
        return self._paginate("/items", {"itemType": "-attachment || note"})

    def create_items(self, payload: list[dict]) -> list[str]:
        """Create items, returning their keys in submission order."""
        if len(payload) > BATCH_LIMIT:
            raise ValueError(f"Zotero accepts at most {BATCH_LIMIT} items per request")
        resp = self.request("POST", "/items", json_body=payload).json()
        failed = resp.get("failed") or {}
        if failed:
            raise ZoteroClientError(f"Zotero rejected {len(failed)} item(s): {failed}")
        successful = resp.get("successful") or {}
        return [successful[str(i)]["key"] for i in range(len(payload))]

    def get_item(self, item_key: str) -> dict:
        return self.request("GET", f"/items/{item_key}").json()

    def add_to_collection(self, item_key: str, collection_key: str) -> bool:
        """Add an existing item to a collection without touching any other field.

        Returns False if it was already a member. PATCH carries only `collections`, so
        the user's own metadata and notes are never rewritten.
        """
        item = self.get_item(item_key)
        data = item.get("data") or {}
        collections = list(data.get("collections") or [])
        if collection_key in collections:
            return False
        self.request("PATCH", f"/items/{item_key}",
                     json_body={"collections": collections + [collection_key]},
                     headers={"If-Unmodified-Since-Version": str(data.get("version", 0))})
        return True

    # --- files -------------------------------------------------------------------

    def upload_attachment(self, item_key: str, filename: str, data: bytes) -> str:
        """Zotero's three-step file upload. Returns "exists" or "uploaded".

        "exists" means Zotero already stores a file with this md5 and has associated it
        with the item — free server-side deduplication.
        """
        auth_headers = {"If-None-Match": "*",
                        "Content-Type": "application/x-www-form-urlencoded"}
        auth = self.request("POST", f"/items/{item_key}/file", headers=auth_headers, data={
            "md5": hashlib.md5(data).hexdigest(),
            "filename": filename,
            "filesize": len(data),
            "mtime": int(time.time() * 1000),  # milliseconds, per the API docs
        }).json()

        if auth.get("exists"):
            return "exists"

        body = auth["prefix"].encode("utf-8") + data + auth["suffix"].encode("utf-8")
        self.request("POST", auth["url"], absolute=True, data=body,
                     headers={"Content-Type": auth["contentType"]})

        self.request("POST", f"/items/{item_key}/file", headers=auth_headers,
                     data={"upload": auth["uploadKey"]})
        return "uploaded"
```

Note `hashlib` must be added to the module imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_client.py -v`
Expected: PASS (18 tests)

- [ ] **Step 5: Verify lint and full suite**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/client.py tests/test_zotero_client.py
git commit -m "feat: Zotero item creation, candidate search, and three-step file upload"
```

---

## Task 12: Push orchestration and `zotero_key` write-back

**Files:**
- Create: `pipeline/zotero/push.py`
- Test: `tests/test_zotero_push.py`

**Interfaces:**
- Consumes: `attachment_filename` (Task 8); `paper_item`, `book_item`, `attachment_item`, `match_existing` (Task 9); `ZoteroClient` and its errors (Tasks 10-11).
- Produces, in `pipeline.zotero.push`:
  - `PUSHED_MARKER = "MATCH (n) WHERE n.id = $id SET n.zotero_key = $key"`
  - `PAPER_FOR_PUSH`, `BOOK_FOR_PUSH` — Cypher returning a node plus its author names
  - `push_one(client, collection_key, record, authors, pdf_bytes, candidates=None) -> dict`

  Used by Tasks 13, 14.

**Context:** `push_one` returns a result dict rather than raising for transient trouble, so the asset can emit metadata and return without failing the run. The Zotero item key is written back to the graph as `zotero_key`, doing triple duty: idempotency guard, repair query (`WHERE p.zotero_key IS NULL`), and a direct node-to-item link.

- [ ] **Step 1: Write the failing test**

Create `tests/test_zotero_push.py`:

```python
import pytest

from pipeline.zotero.client import ZoteroClientError, ZoteroTransientError
from pipeline.zotero.push import push_one

PAPER = {
    "id": "arxiv:2503.13804", "title": "A Preprint", "year": 2025,
    "arxiv_id": "2503.13804", "doi": None, "publication_types": [],
    "kind": "paper",
}


class StubClient:
    def __init__(self, candidates=None, created="NEWKEY", upload="uploaded", raises=None):
        self._candidates = candidates or []
        self._created = created
        self._upload = upload
        self._raises = raises
        self.calls = []

    def search_candidates(self, title, limit=25):
        self.calls.append(("search", title))
        return self._candidates

    def create_items(self, payload):
        if self._raises:
            raise self._raises
        self.calls.append(("create", payload))
        return [self._created, "ATTKEY"][:len(payload)]

    def add_to_collection(self, item_key, collection_key):
        self.calls.append(("add_to_collection", item_key, collection_key))
        return True

    def upload_attachment(self, item_key, filename, data):
        self.calls.append(("upload", item_key, filename, len(data)))
        return self._upload


def test_creates_item_and_uploads_when_no_match():
    c = StubClient()
    out = push_one(c, "COLL", PAPER, ["Ada Lovelace"], b"%PDF-1.4")
    assert out["pushed"] is True
    assert out["outcome"] == "created"
    assert out["zotero_key"] == "NEWKEY"
    assert out["item_type"] == "preprint"
    assert out["filename"] == "A Preprint - Lovelace - 2025.pdf"
    assert any(call[0] == "upload" for call in c.calls)


def test_matched_item_is_added_to_collection_and_never_recreated():
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates)
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["outcome"] == "matched"
    assert out["zotero_key"] == "EXISTING"
    assert ("add_to_collection", "EXISTING", "COLL") in c.calls
    assert not any(call[0] == "create" for call in c.calls), "must not create a duplicate"
    assert not any(call[0] == "upload" for call in c.calls), \
        "must not attach a second PDF to the user's existing item"


def test_supplied_candidates_skip_the_search_request():
    c = StubClient()
    push_one(c, "COLL", PAPER, [], b"%PDF", candidates=[])
    assert not any(call[0] == "search" for call in c.calls)


def test_transient_error_reports_not_pushed_and_does_not_raise():
    c = StubClient(raises=ZoteroTransientError("throttled"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["pushed"] is False
    assert "throttled" in out["reason"]
    assert out["zotero_key"] is None


def test_client_error_propagates():
    c = StubClient(raises=ZoteroClientError("HTTP 400: bad field"))
    with pytest.raises(ZoteroClientError):
        push_one(c, "COLL", PAPER, [], b"%PDF")


def test_book_records_use_the_book_item_shape():
    book = {"id": "isbn:9780521406055", "title": "Probability with Martingales",
            "year": 1991, "publisher": "CUP", "kind": "book"}
    c = StubClient()
    out = push_one(c, "BOOKS", book, ["David Williams"], b"%PDF")
    assert out["item_type"] == "book"
    assert out["filename"] == "Probability with Martingales - Williams - 1991.pdf"


def test_missing_pdf_still_creates_the_item():
    c = StubClient()
    out = push_one(c, "COLL", PAPER, [], None)
    assert out["pushed"] is True
    assert out["attachment"] == "skipped-no-pdf"
    assert not any(call[0] == "upload" for call in c.calls)


def test_exists_upload_is_reported():
    c = StubClient(upload="exists")
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["attachment"] == "exists"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_zotero_push.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pipeline.zotero.push'`

- [ ] **Step 3: Write the implementation**

Create `pipeline/zotero/push.py`:

```python
"""Push orchestration shared by the Dagster assets and the backfill script.

One code path so the asset and the backfill cannot diverge. push_one() returns a result
dict rather than raising on transient trouble: a Zotero outage must not fail an ingest
run whose graph write has already succeeded.
"""
from __future__ import annotations

import logging

from pipeline.zotero.client import ZoteroTransientError
from pipeline.zotero.items import attachment_item, book_item, match_existing, paper_item
from pipeline.zotero.naming import attachment_filename

log = logging.getLogger(__name__)

PAPER_FOR_PUSH = """
MATCH (p:Paper {document_id: $document_id})
OPTIONAL MATCH (a:Author)-[:AUTHORED]->(p)
RETURN p{.id, .title, .year, .doi, .arxiv_id, .venue, .journal_name, .volume, .pages,
         .publication_types, .abstract, .zotero_key} AS node,
       collect(DISTINCT a.name) AS authors
"""

BOOK_FOR_PUSH = """
MATCH (b:Book {document_id: $document_id})
OPTIONAL MATCH (a:Author)-[:AUTHORED]->(b)
RETURN b{.id, .title, .year, .publisher, .edition, .isbn, .zotero_key} AS node,
       collect(DISTINCT a.name) AS authors
"""

PAPERS_NEEDING_PUSH = """
MATCH (p:Paper) WHERE p.zotero_key IS NULL
RETURN p.document_id AS document_id, p.id AS id ORDER BY p.id
"""

BOOKS_NEEDING_PUSH = """
MATCH (b:Book) WHERE b.zotero_key IS NULL
RETURN b.document_id AS document_id, b.id AS id ORDER BY b.id
"""

MARK_PAPER_PUSHED = "MATCH (p:Paper {id:$id}) SET p.zotero_key = $key"
MARK_BOOK_PUSHED = "MATCH (b:Book {id:$id}) SET b.zotero_key = $key"


def push_one(client, collection_key: str, record: dict, authors: list[str],
             pdf_bytes: bytes | None, candidates: list[dict] | None = None) -> dict:
    """File one paper or book into Zotero.

    `record` carries a "kind" of "paper" or "book". `candidates` lets the backfill supply
    a pre-fetched library index instead of paying for one search per item; when None, one
    search request is issued.

    Returns {pushed, outcome, zotero_key, item_type, filename, attachment, reason}.
    Raises only ZoteroClientError (a code defect or revoked key), never on throttling.
    """
    is_book = record.get("kind") == "book"
    result = {"pushed": False, "outcome": None, "zotero_key": None,
              "item_type": None, "filename": None, "attachment": None, "reason": None}

    try:
        pool = client.search_candidates(record.get("title")) if candidates is None \
            else candidates
        existing = match_existing(pool, record.get("doi"), record.get("arxiv_id"),
                                  record.get("title"))

        if existing:
            client.add_to_collection(existing, collection_key)
            result.update(pushed=True, outcome="matched", zotero_key=existing,
                          attachment="skipped-existing-item")
            return result

        item = (book_item(record, authors, collection_key) if is_book
                else paper_item(record, authors, collection_key))
        result["item_type"] = item["itemType"]
        key = client.create_items([item])[0]
        result.update(pushed=True, outcome="created", zotero_key=key)

        filename = attachment_filename(record.get("title"), authors, record.get("year"))
        result["filename"] = filename
        if not pdf_bytes:
            result["attachment"] = "skipped-no-pdf"
            return result

        att_key = client.create_items([attachment_item(key, filename)])[0]
        result["attachment"] = client.upload_attachment(att_key, filename, pdf_bytes)
        return result

    except ZoteroTransientError as exc:
        # Throttling or a 5xx that survived retries. zotero_key stays null, so the repair
        # query picks this record up on the next backfill run.
        log.warning("Zotero push deferred for %s: %s", record.get("id"), exc)
        result["reason"] = str(exc)
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_push.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Verify lint and full suite**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/push.py tests/test_zotero_push.py
git commit -m "feat: Zotero push orchestration with transient-failure tolerance"
```

---

## Task 13: The Dagster assets and pipeline wiring

**Files:**
- Create: `pipeline/assets/zotero_push.py`, `pipeline/assets/book_zotero_push.py`
- Modify: `pipeline/runtime/jobs.py`, `pipeline/definitions.py`
- Create: `tests/integration/test_zotero_integration.py`

**Interfaces:**
- Consumes: `push_one` and the Cypher constants (Task 12), `ZoteroResource` (Task 10).
- Produces: assets `zotero_push` and `book_zotero_push`, appended to the `ingest_document` and `ingest_book` jobs.

**Context:** Both assets are thin wrappers: read the node and its authors from Neo4j, fetch the PDF from MinIO's raw bucket (keyed by content hash, `RAW_BUCKET/{key}.pdf`), delegate to `push_one`, write back `zotero_key`. Papers are partitioned by `documents_partitions_def()`, books by `books_partitions_def()`.

- [ ] **Step 1: Write the paper asset**

Create `pipeline/assets/zotero_push.py`:

```python
"""zotero_push: file this paper into the user's Zotero library under Alethograph/Papers.

Last asset in ingest_document. A Zotero failure never fails the run — the graph write has
already succeeded, and zotero_key staying null means scripts/backfill_zotero.py retries.
"""
from __future__ import annotations

import botocore.exceptions
from dagster import MaterializeResult, asset

from pipeline.runtime.partitions import documents_partitions_def
from pipeline.runtime.storage import RAW_BUCKET
from pipeline.zotero import push as zp


def _fetch_pdf(s3, key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")["Body"].read()
    except botocore.exceptions.ClientError:
        return None


@asset(partitions_def=documents_partitions_def(), deps=["paper_analysis"],
       required_resource_keys={"minio", "neo4j_new", "zotero"})
def zotero_push(context) -> MaterializeResult:
    key = context.partition_key
    new = context.resources.neo4j_new

    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(zp.PAPER_FOR_PUSH, document_id=key).single()
        if row is None:
            return MaterializeResult(metadata={"pushed": False, "reason": "no Paper node"})
        node, authors = dict(row["node"]), row["authors"]
        if node.get("zotero_key"):
            return MaterializeResult(metadata={"pushed": False, "reason": "already in Zotero",
                                               "zotero_key": node["zotero_key"]})

        client = context.resources.zotero.get_client()
        collections = client.ensure_collections()
        pdf = _fetch_pdf(context.resources.minio.get_client(), key)
        out = zp.push_one(client, collections["papers"], {**node, "kind": "paper"},
                          authors, pdf)

        if out["zotero_key"]:
            s.run(zp.MARK_PAPER_PUSHED, id=node["id"], key=out["zotero_key"])

    return MaterializeResult(metadata={
        "pushed": out["pushed"], "outcome": out["outcome"] or "",
        "zotero_key": out["zotero_key"] or "", "item_type": out["item_type"] or "",
        "filename": out["filename"] or "", "attachment": out["attachment"] or "",
        "reason": out["reason"] or "",
    })
```

- [ ] **Step 2: Write the book asset**

Create `pipeline/assets/book_zotero_push.py`:

```python
"""book_zotero_push: file this book into the user's Zotero library under Alethograph/Books.

Books flow through a separate asset chain with no Semantic Scholar lookup, so this mirrors
zotero_push rather than sharing an asset. The orchestration itself is shared via push_one.
"""
from __future__ import annotations

import botocore.exceptions
from dagster import MaterializeResult, asset

from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import RAW_BUCKET
from pipeline.zotero import push as zp


def _fetch_pdf(s3, key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")["Body"].read()
    except botocore.exceptions.ClientError:
        return None


@asset(partitions_def=books_partitions_def(), deps=["book_structure_write"],
       required_resource_keys={"minio", "neo4j_new", "zotero"})
def book_zotero_push(context) -> MaterializeResult:
    key = context.partition_key
    new = context.resources.neo4j_new

    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(zp.BOOK_FOR_PUSH, document_id=key).single()
        if row is None:
            return MaterializeResult(metadata={"pushed": False, "reason": "no Book node"})
        node, authors = dict(row["node"]), row["authors"]
        if node.get("zotero_key"):
            return MaterializeResult(metadata={"pushed": False, "reason": "already in Zotero",
                                               "zotero_key": node["zotero_key"]})

        client = context.resources.zotero.get_client()
        collections = client.ensure_collections()
        pdf = _fetch_pdf(context.resources.minio.get_client(), key)
        out = zp.push_one(client, collections["books"], {**node, "kind": "book"},
                          authors, pdf)

        if out["zotero_key"]:
            s.run(zp.MARK_BOOK_PUSHED, id=node["id"], key=out["zotero_key"])

    return MaterializeResult(metadata={
        "pushed": out["pushed"], "outcome": out["outcome"] or "",
        "zotero_key": out["zotero_key"] or "", "item_type": out["item_type"] or "",
        "filename": out["filename"] or "", "attachment": out["attachment"] or "",
        "reason": out["reason"] or "",
    })
```

- [ ] **Step 3: Wire into the jobs**

In `pipeline/runtime/jobs.py`, add `zotero_push` to the first import block and `book_zotero_push` to the book import block, then extend both selections:

```python
ingest_document = define_asset_job(
    name="ingest_document",
    selection=AssetSelection.assets(
        raw_blob.raw_blob, parsed_document.parsed_document, triage_metadata.triage_metadata,
        chunks.chunks, extracted_graph.extracted_graph, resolved_entities.resolved_entities,
        graph_write.graph_write, paper_analysis.paper_analysis, zotero_push.zotero_push,
    ),
    description="Full per-document build: raw → parse → triage → chunk → extract → resolve "
                "→ write → analyse → Zotero.",
)
```

```python
ingest_book = define_asset_job(
    name="ingest_book",
    selection=AssetSelection.assets(
        book_raw_blob.book_raw_blob, book_parsed.book_parsed, book_metadata.book_metadata,
        book_structure.book_structure, book_chunks.book_chunks,
        book_structure_write.book_structure_write, book_zotero_push.book_zotero_push,
    ),
    description="Book structure build: raw → parse(pages+toc) → metadata → structure → "
                "chunk+embed → write → Zotero. RAG-ready; extraction follows per chapter.",
)
```

- [ ] **Step 4: Register the assets and the resource**

In `pipeline/definitions.py`, add `zotero_push, book_zotero_push` to the `from pipeline.assets import (...)` block, add `zotero_push.zotero_push` and `book_zotero_push.book_zotero_push` to the `assets=[...]` list, add `ZoteroResource` to the resources import, and add to the resources dict:

```python
        "zotero": ZoteroResource(),
```

- [ ] **Step 5: Verify Dagster still loads the definitions**

Run: `uv run python -c "from pipeline.definitions import defs; print(len(defs.get_asset_graph().all_asset_keys), 'assets')"`
Expected: prints a count two higher than before (20 assets).

- [ ] **Step 6: Write the integration test**

Create `tests/integration/test_zotero_integration.py`:

```python
"""End-to-end against the real Zotero library. Creates one item, verifies it, deletes it.

Skipped unless --run-integration. Requires ZOTERO_API_KEY and ZOTERO_USER_ID.
"""
import os

import pytest

from pipeline.zotero.client import ZoteroClient
from pipeline.zotero.items import attachment_item, paper_item

pytestmark = pytest.mark.integration


@pytest.fixture
def client():
    key, user = os.environ.get("ZOTERO_API_KEY"), os.environ.get("ZOTERO_USER_ID")
    if not key or not user:
        pytest.skip("ZOTERO_API_KEY / ZOTERO_USER_ID not set")
    return ZoteroClient(api_key=key, user_id=user)


def test_collections_bootstrap_is_idempotent(client):
    first = client.ensure_collections()
    second = client.ensure_collections()
    assert first == second
    assert first["papers"] and first["books"]


def test_create_attach_and_delete_roundtrip(client):
    collections = client.ensure_collections()
    paper = {"title": "ZZZ Alethograph Integration Test — safe to delete",
             "year": 2026, "arxiv_id": "0000.00001", "doi": None,
             "publication_types": []}
    item = paper_item(paper, ["Test Author"], collections["papers"])
    key = client.create_items([item])[0]
    try:
        fetched = client.get_item(key)
        assert fetched["data"]["itemType"] == "preprint"
        assert collections["papers"] in fetched["data"]["collections"]

        att_key = client.create_items([attachment_item(key, "test.pdf")])[0]
        assert client.upload_attachment(att_key, "test.pdf", b"%PDF-1.4\n%%EOF\n") in (
            "uploaded", "exists")
    finally:
        client.request("DELETE", f"/items/{key}",
                       headers={"If-Unmodified-Since-Version":
                                str(client.get_item(key)["data"]["version"])})
```

- [ ] **Step 7: Run the unit suite, then the integration test**

Run: `uv run pytest tests -q && uv run ruff check .`
Expected: PASS, integration tests skipped.

Run: `uv run pytest tests/integration/test_zotero_integration.py -v --run-integration`
Expected: PASS. Check the real Zotero library afterwards — an `Alethograph` collection with `Papers` and `Books` subcollections should exist, and the test item should be gone.

- [ ] **Step 8: Commit**

```bash
git add pipeline/assets/zotero_push.py pipeline/assets/book_zotero_push.py \
        pipeline/runtime/jobs.py pipeline/definitions.py \
        tests/integration/test_zotero_integration.py
git commit -m "feat: zotero_push and book_zotero_push assets wired into both ingest jobs"
```

---

## Task 14: Backfill the existing corpus into Zotero

**Files:**
- Create: `scripts/backfill_zotero.py`
- Test: manual dry run

**Interfaces:**
- Consumes: `push_one` and the Cypher constants (Task 12), `ZoteroClient` (Tasks 10-11).
- Produces: nothing consumed by later tasks.

**Context:** Roughly 174 items and ~1.28 GB of uploads. Attachment upload dominates the runtime. Resumable for free via `zotero_key`: re-running skips everything already pushed. The script fetches the library index **once** and reuses it across every item, rather than issuing 174 searches.

**Sequencing warning:** `scripts/backfill_venue.py` (Task 5) must have been run *without* `--dry-run` before this script runs for real. Otherwise every existing paper files as a `preprint`, and the `zotero_key` idempotency guard means a second run will not correct it.

- [ ] **Step 1: Write the script**

Create `scripts/backfill_zotero.py`:

```python
"""One-off backfill: push every ingested paper and book into Zotero under Alethograph.

Resumable — papers and books already carrying a zotero_key are skipped, so an interrupted
run can simply be re-run. Fetches the library index once and reuses it for deduplication
across every item rather than issuing one search per item.

PREREQUISITE: run scripts/backfill_venue.py (without --dry-run) first. Without venue data
every existing paper files as a preprint, and the zotero_key guard prevents a re-run from
correcting it.

Run: uv run python scripts/backfill_zotero.py [--apply] [--limit N]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import boto3
import botocore.exceptions
from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.runtime.storage import RAW_BUCKET
from pipeline.zotero import push as zp
from pipeline.zotero.client import ZoteroClient


def fetch_pdf(s3, key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")["Body"].read()
    except botocore.exceptions.ClientError:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write to Zotero (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="process at most N records")
    args = ap.parse_args()

    load_dotenv()
    client = ZoteroClient(api_key=os.environ["ZOTERO_API_KEY"],
                          user_id=os.environ["ZOTERO_USER_ID"])
    s3 = boto3.client("s3", endpoint_url=os.environ["MINIO_ENDPOINT"],
                      aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
                      region_name="us-east-1")
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    database = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")

    collections = client.ensure_collections()
    print(f"collections: Papers={collections['papers']} Books={collections['books']}")
    print("fetching library index for deduplication...")
    index = client.library_index()
    print(f"  {len(index)} existing items indexed")

    created = matched = deferred = no_pdf = 0
    with driver, driver.session(database=database) as s:
        todo = [("paper", dict(r)) for r in s.run(zp.PAPERS_NEEDING_PUSH)]
        todo += [("book", dict(r)) for r in s.run(zp.BOOKS_NEEDING_PUSH)]
        if args.limit:
            todo = todo[:args.limit]
        print(f"{len(todo)} records to push\n")

        for kind, ref in todo:
            query = zp.PAPER_FOR_PUSH if kind == "paper" else zp.BOOK_FOR_PUSH
            row = s.run(query, document_id=ref["document_id"]).single()
            if row is None:
                print(f"  SKIP     {ref['id']}  (node vanished)")
                continue
            node, authors = dict(row["node"]), row["authors"]
            pdf = fetch_pdf(s3, ref["document_id"])
            if pdf is None:
                no_pdf += 1

            if not args.apply:
                print(f"  DRY      {kind:5} {node.get('title')!r}")
                continue

            out = zp.push_one(client, collections["papers" if kind == "paper" else "books"],
                              {**node, "kind": kind}, authors, pdf, candidates=index)
            if out["zotero_key"]:
                s.run(zp.MARK_PAPER_PUSHED if kind == "paper" else zp.MARK_BOOK_PUSHED,
                      id=node["id"], key=out["zotero_key"])

            if out["outcome"] == "created":
                created += 1
                print(f"  CREATED  {out['item_type']:16} {out['filename']}")
            elif out["outcome"] == "matched":
                matched += 1
                print(f"  MATCHED  {out['zotero_key']}  {node.get('title')!r}")
            else:
                deferred += 1
                print(f"  DEFERRED {node.get('title')!r}  ({out['reason']})")

    print(f"\ncreated: {created}   matched existing: {matched}   "
          f"deferred (retry later): {deferred}   missing PDF: {no_pdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Lint and dry-run**

Run: `uv run ruff check scripts/backfill_zotero.py && uv run python scripts/backfill_zotero.py`
Expected: no lint errors; the collections line, an index count, then ~174 `DRY` lines.

- [ ] **Step 3: Smoke-test a single record against the real library**

Run: `uv run python scripts/backfill_zotero.py --apply --limit 1`
Expected: one `CREATED` line. Open Zotero and confirm the item appears under `Alethograph/Papers` with a correctly-named PDF attachment.

- [ ] **Step 4: Commit**

```bash
git add scripts/backfill_zotero.py
git commit -m "feat: add backfill_zotero script for the existing corpus"
```

---

## Sequencing after implementation

These are operational runs, not code changes. Order matters.

- [ ] `uv run python scripts/migrate_paper_ids.py` — dry run. If any `COLLIDE` appears, resolve by hand before continuing.
- [ ] `uv run python scripts/migrate_paper_ids.py --apply`
- [ ] `uv run python scripts/backfill_venue.py --dry-run`
- [ ] `uv run python scripts/backfill_venue.py` — enriches ~124 papers.
- [ ] Spot-check: `MATCH (p:Paper) WHERE p.venue IS NOT NULL RETURN count(p)` should be well above zero, and a known published paper (e.g. the Journal of Finance one, DOI `10.1111/jofi.13188`) should now carry `venue` and `publication_types`.
- [ ] `uv run python scripts/backfill_zotero.py` — dry run.
- [ ] `uv run python scripts/backfill_zotero.py --apply --limit 1` — verify one item in the Zotero UI.
- [ ] `uv run python scripts/backfill_zotero.py --apply` — full run, ~1.28 GB of uploads.

## Self-review notes

**Spec coverage.** Spec §A1→Task 4; §A2→Task 4; §A3→Task 3; §A4→Task 4; §A5→Tasks 1 and 6 (the migration note became its own task because relabelling under a uniqueness constraint carries a different risk profile from the pure function); §A6→Task 2; §A7→Task 5; §A8→Task 7; §A9→Tasks 1-4; §B1→Tasks 8-13; §B2→Task 10; §B3→Task 10; §B4→Tasks 9 and 12; §B5→Task 9; §B6→Task 8; §B7→Task 11; §B8→Tasks 12-13; §B9→Tasks 10-12; §B10→Task 14; §B11→Tasks 8-13.

**Deviation from the spec.** The spec sketched `_PLACEHOLDER_DOI` as a blocklist alone; Task 1 adds a structural `^10\.\d{4,9}/\S+$` shape check, which is stricter and catches malformed DOIs the blocklist would miss. The spec's `/$` alternation is subsumed by requiring a non-empty suffix.
