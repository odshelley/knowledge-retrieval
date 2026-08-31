# Zotero Library Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist publication-venue data on `Paper` nodes at ingest time, then automatically file every ingested paper and book into the user's Zotero library under an `Alethograph` collection with normalized PDF attachments.

**Architecture:** Phase A (Tasks 1-6) extends the existing Semantic Scholar enrichment in `pipeline/graph/research_port.py` — which already *requests* `venue` and discards it — to persist venue, journal, volume, pages, and publication types, while fixing two latent bugs in the same code path. Phase B (Tasks 7-13) adds a `pipeline/zotero/` package whose pure logic (filename formatting, item construction, dedup matching) is separated from its HTTP transport, consumed by two new Dagster assets and one backfill script.

**Tech Stack:** Python 3.12, Dagster 1.9.5, Neo4j 5.x (`neo4j` driver), `requests`, pytest. No new third-party dependencies — `requests` is already declared in `pyproject.toml`.

> **Revision note (2026-08-31).** This plan was reviewed by four independent reviewers and every load-bearing claim re-verified against the live codebase and the live Zotero / Semantic Scholar APIs. The original Task 6 (paper-id migration) has been **dropped** — see "Dropped work" at the end for why. Findings that changed the code are marked **[REV]** inline.

## Global Constraints

- **No new dependencies.** `pyzotero` is explicitly rejected; hand-roll the Zotero calls with `requests`, mirroring how `research_port.py` hand-rolls Semantic Scholar.
- **Python >= 3.12**, `from __future__ import annotations` at the top of every new module (repo-wide convention).
- **Ruff line-length 100**, target `py312`. Run `uv run ruff check .` before every commit.
- **Secrets never logged, and never sent off-host.** `ZOTERO_API_KEY` is read from the environment only; never write it to asset metadata, log output, exception messages, or any committed file, and never attach it to a request aimed at a non-Zotero host.
- **A Zotero failure must never fail an ingest run.** This is absolute. It covers an unconfigured key, an exhausted storage quota, throttling, and outages alike. Only a genuine payload defect (HTTP 400) raises.
- **Never modify the user's own Zotero data.** A matched pre-existing item may only have its `collections` array appended to and, if it has no attachment, gain one. No other field is ever written.
- **Neo4j has no nested-map property type.** S2's `journal` object must be flattened into separate scalar properties.
- **`s.run(rp.WRITE_PAPER, **paper)` splats the dict**, so `paper` dict keys and Cypher `$params` must match exactly or the query raises on a missing parameter.
- **Zotero batch limit is 50** items per create request; pagination `limit` max is 100.
- **Zotero `mtime` is milliseconds**, not seconds.
- **Zotero field-name casing is `DOI` and `ISBN` uppercase**, everything else camelCase. Lowercase variants are silently dropped by the API. (Verified against the live `items/new` templates, schema v42.)
- Test commands run as `uv run pytest ...` from the repo root.
- **Known-failing baseline — read this before trusting any "Expected: PASS".** On a clean checkout today, `uv run pytest tests -q` reports **6 failed, 306 passed, 20 skipped**. All 6 failures are `ModuleNotFoundError: No module named 'mcp'` in `tests/server/test_app.py`, because the optional `server` extra is not installed. They are unrelated to this work. Separately, `uv run ruff check .` reports **12 pre-existing errors, all in `notebooks/smoke_test.ipynb`**.

  Therefore every "verify the full suite" step in this plan uses:

  ```bash
  uv run pytest tests -q --ignore=tests/server/test_app.py
  uv run ruff check pipeline server scripts tests
  ```

  which is genuinely green today (306 passed, 20 skipped, no lint errors). Do **not** chain these with `&&` against the unfiltered suite — the pre-existing failures would stop the chain before ruff ever runs, and an agent following the plan literally would halt at Task 1.

## Verified API facts

These were checked against live APIs, not inferred. Do not "correct" them to match secondary sources.

- `itemType=-attachment || note` **is** valid: the negation binds to the whole OR expression. Verified empirically against a public library; a web search gives the wrong answer here.
- Zotero's `preprint` item type exists in API v3, with fields `repository`, `archiveID`, `url`, `DOI`, `abstractNote`. It has **no** `volume` or `pages`.
- Single-field creators are `{"creatorType": "author", "name": "..."}` with **no `fieldMode` key**. `fieldMode` is an internal Zotero client concept, not Web API v3. Verified against 15 live single-field creators.
- `qmode=titleCreatorYear` is real (and the default); `qmode=bogus` returns 400.
- PATCH merges: properties absent from the body are left untouched. `If-Unmodified-Since-Version` is mandatory. Success returns 204.
- S2's `publicationTypes` values are `"JournalArticle"` and `"Conference"` — **no space**. S2's own OpenAPI example says `"Journal Article"` *with* a space and is stale. It also returns literal `null`, not `[]`, when absent.
- S2's `journal` object is `{name, volume, pages}` with no `issue`.

## File Structure

**Phase A — modified:**
- `pipeline/graph/research_port.py` — S2 client. Gains `clean_doi`, `with_retry`, venue mapping, revised `WRITE_PAPER`.
- `pipeline/assets/triage_metadata.py` — ingest asset. Wires `clean_doi` and the new venue keys.
- `scripts/backfill_citations.py` — drops its private `_with_retry` in favour of the shared one.
- `server/queries.py` — exposes new properties to the MCP layer.
- `tests/test_research_port.py` — **already exists with 8 tests; APPEND, never overwrite.** **[REV]**

**Phase A — created:**
- `scripts/backfill_venue.py` — re-enriches existing papers.

**Phase B — created (`pipeline/zotero/`):**
- `naming.py` — pure: attachment filename formatting. Stdlib only.
- `items.py` — pure: item-type mapping, creator splitting, payload construction, dedup matching.
- `client.py` — HTTP only: collections bootstrap, candidate search, library index, item create, three-step file upload.
- `push.py` — orchestration shared by both assets and the backfill script.

**Phase B — created (elsewhere):**
- `pipeline/assets/zotero_push.py`, `pipeline/assets/book_zotero_push.py`
- `scripts/backfill_zotero.py`
- `tests/test_zotero_naming.py`, `tests/test_zotero_items.py`, `tests/test_zotero_client.py`, `tests/test_zotero_push.py`
- `tests/integration/test_zotero_integration.py`

**Phase B — modified:**
- `pipeline/runtime/resources.py`, `pipeline/runtime/jobs.py`, `pipeline/definitions.py`, `.env.example`

The pure/HTTP split matters: the suite tests pure functions directly and hand-rolls fakes for I/O (`tests/test_raw_blob.py`, `tests/test_resolver.py`); there is no HTTP-mocking library in the dev dependencies. Keeping `naming.py` and `items.py` free of I/O means Tasks 7-8 need no fakes at all.

---

## Task 1: `clean_doi` — reject placeholder and malformed DOIs

**Files:**
- Modify: `pipeline/graph/research_port.py` (add after `normalize_title`, ~line 22)
- Modify: `pipeline/assets/triage_metadata.py:50-60` **[REV]** (the block from `rec = None` through the `compute_paper_id` assignment)
- Test: `tests/test_research_port.py` — **APPEND to the existing file. It already contains 8 tests. Do not use Write.** **[REV]**

**Interfaces:**
- Consumes: nothing.
- Produces: `clean_doi(doi: str | None) -> str | None` in `pipeline.graph.research_port`. Used by Tasks 4, 8, 11.

**Context:** A live audit of the 150-paper corpus found 3 DOIs that are unfilled LaTeX template placeholders: `10.1145/NNNNNNN.NNNNNNN`, `10.1017/S09624929XXXXXXXX`, and `http://dx.doi.org/10.1145/0000000.0000000`. These break S2 lookup and pollute `compute_paper_id`, which derives node identity as `"doi:" + doi.strip().lower()`.

**[REV] Regex correction.** The originally-drafted pattern `r"N{4,}|X{4,}|0{6,}"` with `re.IGNORECASE` was tested against plausible real DOIs and **produced false positives**: it rejected `10.1002/nla.2000000` (six zeros inside ordinary digits) and `10.1088/1361-6420/abcxxxx` (lowercase x). The corrected rule is case-**sensitive** for `N`/`X`, and treats an all-zeros suffix as a placeholder only when the *entire* suffix is zeros and dots.

**[REV] Accepted consequence.** `clean_doi` changes what `compute_paper_id` returns for those 3-4 papers. Their existing graph nodes keep their old ids (there is no migration — see "Dropped work"). If one of those PDFs is ever deliberately re-ingested, a second `Paper` node is created and `DUP_CHECK` (`triage_metadata.py:20`, which matches on the computed id) will not catch it. This is accepted, not a bug: the daily schedule (`pipeline/runtime/schedules.py:12-18`) only registers *new* content-hash partitions, so re-ingesting an existing document is a deliberate manual act.

- [ ] **Step 1: Append the failing tests**

Append to `tests/test_research_port.py` (do not overwrite; `import pytest` and `from pipeline.graph import research_port as rp` may already be present at the top — reuse them):

```python
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


def test_clean_doi_does_not_false_positive_on_realistic_dois():
    """Regression: an earlier case-insensitive `0{6,}` / `X{4,}` rule rejected both of
    these. Six zeros among digits and a lowercase-x suffix are both legitimate."""
    assert rp.clean_doi("10.1002/nla.2000000") == "10.1002/nla.2000000"
    assert rp.clean_doi("10.1088/1361-6420/abcxxxx") == "10.1088/1361-6420/abcxxxx"


def test_clean_doi_rejects_structurally_invalid():
    assert rp.clean_doi(None) is None
    assert rp.clean_doi("") is None
    assert rp.clean_doi("   ") is None
    assert rp.clean_doi("not-a-doi") is None
    assert rp.clean_doi("10.1145/") is None          # empty suffix
    assert rp.clean_doi("10.1/short-prefix") is None  # registrant must be 4-9 digits
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_research_port.py -v -k clean_doi`
Expected: FAIL with `AttributeError: module 'pipeline.graph.research_port' has no attribute 'clean_doi'`

- [ ] **Step 3: Write the implementation**

In `pipeline/graph/research_port.py`, directly after `normalize_title`:

```python
# A DOI is "10.<4-9 digit registrant>/<suffix>". Anything else is not a DOI.
_DOI_SHAPE = re.compile(r"^10\.\d{4,9}/\S+$")
# Unfilled LaTeX template DOIs seen in the corpus. Case-SENSITIVE on purpose: a
# case-insensitive rule rejected the legitimate "10.1088/1361-6420/abcxxxx".
_PLACEHOLDER_RUN = re.compile(r"N{4,}|X{4,}")
# All-zeros suffix, e.g. "0000000.0000000". Requiring the WHOLE suffix keeps
# "10.1002/nla.2000000" (six zeros among real digits) valid.
_ALL_ZEROS_SUFFIX = re.compile(r"^[0.]+$")
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
    if not _DOI_SHAPE.match(d):
        return None
    suffix = d.split("/", 1)[1]
    if _PLACEHOLDER_RUN.search(suffix) or _ALL_ZEROS_SUFFIX.match(suffix):
        return None
    return d
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS — the 4 new tests plus the 8 pre-existing ones.

- [ ] **Step 5: Wire into `triage_metadata.py`**

**[REV]** Replace lines **50-60** — the whole block from `rec = None` down to and including the `paper_id = rp.compute_paper_id(...)` assignment. Replacing a narrower range leaves a dangling `rec = None` and deletes the `paper_id` assignment.

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

A placeholder DOI is now passed to neither `lookup_by_doi` (it would 404 and burn a request) nor `compute_paper_id` (which would mint a junk identity).

- [ ] **Step 6: Verify the full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
Expected: PASS, no new failures.

- [ ] **Step 7: Commit**

```bash
git add pipeline/graph/research_port.py pipeline/assets/triage_metadata.py tests/test_research_port.py
git commit -m "fix: reject placeholder and malformed DOIs before lookup and identity"
```

---

## Task 2: Retry throttled S2 lookups, keep 404 definitive

**Files:**
- Modify: `pipeline/graph/research_port.py` (add `with_retry` and `_get_paper`; rewrite both lookups)
- Modify: `scripts/backfill_citations.py:58-66` (delete private copy, import shared)
- Test: `tests/test_research_port.py` — **APPEND** **[REV]**

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `with_retry(fn, *args, attempts: int = 3, base_sleep: float = 5.0)` in `pipeline.graph.research_port`.

**Context:** `lookup_by_arxiv` and `lookup_by_doi` currently fold a 404, a 429, and a timeout into an identical `None` (`research_port.py:50-63`), so on S2's unauthenticated tier a throttled call is indistinguishable from a missing paper. S2's documented limit is *1000 requests/second shared among all unauthenticated users* — i.e. no per-caller budget, which is why throttling is unpredictable. A second request issued within ~1s was observed returning 429.

A retry helper written for exactly this already exists in `scripts/backfill_citations.py:58-66` but was never promoted into the pipeline.

**[REV] Scope of `with_retry` after this task.** Once the lookups retry internally, `with_retry` must **not** be wrapped around them — doing so multiplies the retries (3 inner × 3 outer, with cumulative sleeps). It remains in use only for `rp.references()`, which has no internal retry. Task 5 and `backfill_citations.py` are written accordingly.

- [ ] **Step 1: Append the failing tests**

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

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_research_port.py -v -k "retry or 429"`
Expected: FAIL — `with_retry` does not exist; `lookup_by_arxiv` does not retry.

(The quotes around the `-k` expression are required. Unquoted, the shell splits it and pytest reports `file or directory not found: or`, collecting 0 tests.)

- [ ] **Step 3: Write the implementation**

Add `import logging` and `import time` to the module imports, then:

```python
log = logging.getLogger(__name__)

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def with_retry(fn, *args, attempts: int = 3, base_sleep: float = 5.0):
    """Retry a call that signals failure by returning a falsy value.

    Used for rp.references(), which has no internal retry. Do NOT wrap lookup_by_arxiv /
    lookup_by_doi in this — they retry internally, and nesting multiplies the sleeps.
    """
    out = None
    for i in range(attempts):
        out = fn(*args)
        if out:
            return out
        if i < attempts - 1:
            time.sleep(base_sleep * (i + 1))
    return out


def _get_paper(path: str) -> dict | None:
    """One S2 GET with retry on throttling/5xx. A 404 is definitive and returns at once.

    Path prefixes are "arXiv:<id>" and "DOI:<doi>". S2 documents these uppercase as
    "ARXIV:"; prefix matching is case-insensitive in practice, verified live.
    """
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS. The 3 pre-existing `@patch("...research_port.requests.get")` tests still pass through the new `_get_paper` path.

- [ ] **Step 5: Delete the duplicate in `backfill_citations.py`**

Remove the `_with_retry` definition at lines 58-66 and replace every call site with `rp.with_retry(`. The module already does `from pipeline.graph import research_port as rp`. Leave the `rp.references()` call sites wrapped; **unwrap any `lookup_by_arxiv` / `lookup_by_doi` call sites**, which now retry internally.

Run: `grep -n "_with_retry" scripts/backfill_citations.py`
Expected: no output.

- [ ] **Step 6: Verify the full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
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
- Test: `tests/test_research_port.py` — **APPEND**

**Interfaces:**
- Consumes: nothing.
- Produces: revised `WRITE_PAPER`. Task 4 extends the same `SET` clause; Tasks 5 and 12 reuse the query verbatim.

**Context:** `WRITE_PAPER` uses an unconditional `SET`, so re-ingesting a document whose S2 lookup returned nothing overwrites stored metadata with nulls. The `coalesce` idiom is already used one line below for `a.s2_author_id`.

**[REV] Test must normalize whitespace.** A flat substring assertion fails for `influential_citation_count`, because that clause exceeds 100 characters on one line (measured: 103) and must wrap, defeating the match. Normalizing whitespace makes the assertion robust to wrapping without fighting the linter.

- [ ] **Step 1: Append the failing tests**

Add `import re` to the test module imports if absent.

```python
ENRICHMENT_PROPS = [
    "title", "year", "arxiv_id", "doi", "s2_id",
    "abstract", "tldr", "citation_count", "influential_citation_count",
]


def _flat(cypher: str) -> str:
    """Collapse whitespace so assertions survive line-wrapping in the query text."""
    return re.sub(r"\s+", " ", cypher)


@pytest.mark.parametrize("prop", ENRICHMENT_PROPS)
def test_write_paper_never_nulls_out_enrichment_fields(prop):
    """A failed S2 lookup passes None for every enrichment field. An unconditional SET
    would erase good stored values; each must be guarded by coalesce."""
    assert f"p.{prop} = coalesce(${prop}, p.{prop})" in _flat(rp.WRITE_PAPER)


def test_write_paper_sets_document_id_unconditionally():
    """document_id comes from the partition key, never from S2, and is never null."""
    assert "p.document_id = $document_id" in _flat(rp.WRITE_PAPER)
    assert "coalesce($document_id" not in _flat(rp.WRITE_PAPER)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_research_port.py -v -k write_paper`
Expected: FAIL — the current query uses `SET p.title=$title, ...` with no `coalesce`.

- [ ] **Step 3: Write the implementation**

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

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS

- [ ] **Step 5: Verify the full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
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
- Test: `tests/test_research_port.py` — **APPEND**

**Interfaces:**
- Consumes: `WRITE_PAPER` from Task 3.
- Produces: five new `Paper` properties — `venue`, `journal_name`, `volume`, `pages` (strings) and `publication_types` (list of string). Consumed by Tasks 5, 6, 8, 11.

**Context:** `FIELDS` already requests `venue` and `_paper_json_to_record` already maps it, but `triage_metadata.py` builds the write payload without it. `publicationTypes` and `journal` are not requested at all. `publicationVenue` is deliberately **not** requested: it duplicates `venue`/`journal` and adds a second nested structure to flatten for no gain.

**[REV] `publication_types` passes `None`, not `[]`, to Cypher.** The original draft used `CASE WHEN size($publication_types) > 0`. Passing `None` for the empty case lets a plain `coalesce` do the job, which is uniform with every other field and sidesteps any question about `size()` semantics on a parameter. The Python-side record keeps `[]` for ergonomics; the asset converts at the boundary.

- [ ] **Step 1: Append the failing tests**

```python
S2_JOURNAL_RESPONSE = {
    "paperId": "6b0388ae597bbe27aab7e81cb653a720b4f0760d",
    "title": "A Multi-agent Targeted Trading Equilibrium with Transaction Costs",
    "abstract": "We study...",
    "year": 2023,
    "venue": "SIAM Journal on Financial Mathematics",
    "publicationTypes": ["JournalArticle"],
    "journal": {"name": "SIAM J. Financial Math.", "volume": "15", "pages": "161-193"},
    "externalIds": {"DOI": "10.1137/22M1542982", "ArXiv": "2306.08519"},
    "citationCount": 42,
    "influentialCitationCount": 5,
    "tldr": {"text": "A short summary."},
    "authors": [{"name": "Bruno Bouchard", "authorId": "1"}],
}


def test_record_maps_venue_and_flattens_journal():
    rec = rp._paper_json_to_record(S2_JOURNAL_RESPONSE)
    assert rec["venue"] == "SIAM Journal on Financial Mathematics"
    assert rec["journal_name"] == "SIAM J. Financial Math."
    assert rec["volume"] == "15"
    assert rec["pages"] == "161-193"
    assert rec["publication_types"] == ["JournalArticle"]


def test_record_survives_missing_journal_and_null_publication_types():
    """S2 returns literal null (not []) for publicationTypes when absent — verified live."""
    minimal = {"paperId": "x", "title": "T", "journal": None, "publicationTypes": None}
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


@pytest.mark.parametrize("prop", ["venue", "journal_name", "volume", "pages",
                                  "publication_types"])
def test_write_paper_coalesces_venue_fields(prop):
    assert f"p.{prop} = coalesce(${prop}, p.{prop})" in _flat(rp.WRITE_PAPER)
```

- [ ] **Step 2: Run tests to verify they fail**

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
        # S2 returns literal null here, not [], when the field is absent.
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
    p.publication_types = coalesce($publication_types, p.publication_types),
```

- [ ] **Step 6: Extend the `paper` dict in `triage_metadata.py`**

Note `or None` on the last line: an empty list must arrive as null so `coalesce` preserves any stored value.

```python
        "venue": rec.get("venue"),
        "journal_name": rec.get("journal_name"),
        "volume": rec.get("volume"),
        "pages": rec.get("pages"),
        "publication_types": rec.get("publication_types") or None,
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_research_port.py -v`
Expected: PASS

- [ ] **Step 8: Verify the parameter set matches the query**

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

- [ ] **Step 9: Verify the full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
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
- Test: manual dry run (a one-off operational script, matching `scripts/backfill_citations.py`, which has no unit tests)

**Interfaces:**
- Consumes: `rp.clean_doi` (Task 1), `rp.lookup_by_arxiv` / `rp.lookup_by_doi` (Task 2), `rp.WRITE_PAPER` (Tasks 3-4).
- Produces: nothing consumed by later tasks. Must be **run** before Task 13.

**Context:** 124 of 150 papers carry an arXiv ID or DOI and can be enriched. The remaining 26 have no identifier and stay unenriched by design.

**[REV] Three corrections from review:**
1. **Do not write `doi`.** The original draft passed a freshly-fetched DOI into `WRITE_PAPER` without recomputing `Paper.id`. Since `compute_paper_id` prefers DOI over arXiv, that would leave nodes with `id="arxiv:…"` but `doi="10.1111/…"`, so a later re-ingest computes a *different* id, `DUP_CHECK` misses, and a duplicate `Paper` is created — which then breaks `PAPER_FOR_PUSH`'s `.single()` with two rows. Venue is what this script is for; identity is not its business.
2. **Do not wrap the lookups in `with_retry`** — they retry internally as of Task 2.
3. **Only query existing authors when actually needed** — one saved Neo4j round-trip per paper.

- [ ] **Step 1: Write the script**

```python
"""One-off backfill: re-fetch Semantic Scholar metadata for papers ingested before venue
data was persisted, and write venue/journal/volume/pages/publication_types onto the node.

Deliberately does NOT write `doi`: compute_paper_id prefers DOI over arXiv id, so adding a
DOI to a node whose id was derived from its arXiv id decouples the two and makes a later
re-ingest mint a duplicate Paper. Venue is this script's only job.

Papers with neither an arXiv id nor a usable DOI cannot be enriched (S2 lookup is
identifier-only) and are reported as 'unidentifiable' rather than silently skipped.

Writes through research_port.WRITE_PAPER so coalesce semantics match the ingest asset
exactly — a paper whose lookup fails keeps whatever it already had.

Idempotent; free S2 API, 1 req/paper, retried internally on 429, paced at ~1 req/s to
stay under S2's unauthenticated limit (the same 1.1s pacing scripts/backfill_citations.py
uses; without it, 124 back-to-back requests throttle hard and each 429 costs 15s of
in-lookup backoff).

Run: uv run python scripts/backfill_venue.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
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


def enrich(rec: dict, p: dict, authors: list[dict]) -> dict:
    """Build WRITE_PAPER params. Every enrichment field may be None; coalesce protects the
    stored value, so a failed lookup is a no-op rather than data loss.

    `doi` and `arxiv_id` deliberately pass through UNCHANGED from the stored node — see the
    module docstring on why this script must not alter identity inputs.
    """
    return {
        "id": p["id"],
        "document_id": p["document_id"],
        "title": rec.get("title"),
        "year": rec.get("year"),
        "arxiv_id": p.get("arxiv_id"),
        "doi": p.get("doi"),
        "s2_id": rec.get("s2_id"),
        "abstract": rec.get("abstract"),
        "tldr": rec.get("tldr"),
        "citation_count": rec.get("citation_count"),
        "influential_citation_count": rec.get("influential_citation_count"),
        "venue": rec.get("venue"),
        "journal_name": rec.get("journal_name"),
        "volume": rec.get("volume"),
        "pages": rec.get("pages"),
        "publication_types": rec.get("publication_types") or None,
        "authors": authors,
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

            # ~1 req/s. S2's unauthenticated tier shares a global budget and throttles
            # unpredictably; do NOT wrap these in with_retry — they retry internally as
            # of Task 2, and nesting multiplies both the requests and the backoff.
            time.sleep(1.1)
            rec = rp.lookup_by_arxiv(arxiv) if arxiv else None
            if rec is None and doi:
                time.sleep(1.1)
                rec = rp.lookup_by_doi(doi)

            if not rec:
                no_record += 1
                print(f"  MISS  {p['id']}  (S2 returned nothing)")
                continue

            print(f"  OK    {p['id']}  venue={rec.get('venue')!r} "
                  f"types={rec.get('publication_types')}")
            if not args.dry_run:
                # S2 author names win when present; otherwise keep what the node already
                # has, so WRITE_PAPER's MERGE is a no-op rather than dropping authorship.
                authors = rec.get("authors")
                if not authors:
                    names = s.run(EXISTING_AUTHORS, id=p["id"]).single()["names"]
                    authors = [{"name": n, "s2_author_id": None} for n in names]
                s.run(rp.WRITE_PAPER, **enrich(rec, p, authors))
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
Expected: no lint errors; a per-paper report ending in a summary. Roughly 150 papers considered, ~26 reported `SKIP`.

Do **not** run without `--dry-run` yet — that happens in the sequencing step.

- [ ] **Step 3: Commit**

```bash
git add scripts/backfill_venue.py
git commit -m "feat: add backfill_venue script for pre-venue-enrichment papers"
```

---

## Task 6: Expose the new properties to the MCP/server layer

**Files:**
- Modify: `server/queries.py:63-69` (`render_schema`), `:249-259` (`GET_PAPER`)
- Test: `tests/server/test_venue_visibility.py` (create; `tests/server/__init__.py` already exists)

**Interfaces:**
- Consumes: the five properties from Task 4.
- Produces: nothing consumed by later tasks.

**Context:** Both lists are hardcoded. `render_schema()` is the property list shown to the query-generating LLM; `GET_PAPER`'s projection enumerates returned properties explicitly. Without this, `/kg:ask` cannot see venue data even though it is in the graph. This task does not serve the Zotero push — it is independently useful and cheap, and is grouped here because it is the last consumer of Task 4's schema change.

- [ ] **Step 1: Write the failing test**

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

Run: `uv run pytest tests/server -v && uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add server/queries.py tests/server/test_venue_visibility.py
git commit -m "feat: expose venue properties to the MCP schema and GET_PAPER projection"
```

---

## Task 7: Attachment filename formatting

**Files:**
- Create: `pipeline/zotero/__init__.py` (empty, matching every other pipeline subpackage), `pipeline/zotero/naming.py`
- Test: `tests/test_zotero_naming.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `attachment_filename(title, authors, year) -> str`, `surname(name) -> str`, `author_segment(authors) -> str` in `pipeline.zotero.naming`. Used by Tasks 8, 11.

**Context:** Confirmed format is `Title - Author(s) - Year.pdf`. One author gives the surname; two give `Surname and Surname`; three or more give `Surname et al.`. The filename stays under 200 bytes, leaving headroom under the 255-byte filesystem limit for the eventual WebDAV/NAS backend. A missing component is omitted with its separator rather than rendered as `Unknown`.

**[REV] The `et al.` trap.** `_sanitize` ends with `.strip(".")`, which is required by `test_trailing_dots_are_stripped_from_the_title` and **fatal** if applied to the joined author segment: it silently turns `Bouchard et al.` into `Bouchard et al`, failing this task's own test. The fix is to sanitize each surname before joining, never the joined result. This was caught by executing the plan's tests against the plan's code; both forms look correct on inspection.

**Note:** the implementation below was executed against every assertion in this task and all 11 pass, including the byte math — a maximally long title lands at exactly 200 bytes, so there is **zero margin**. Any change to the tail format or to `MAX_FILENAME_BYTES` flips that test; re-run it if either moves.

- [ ] **Step 1: Write the failing test**

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
    """'' | 'Surname' | 'A and B' | 'A et al.' — Zotero's own citation convention.

    Sanitizes each surname BEFORE joining. Sanitizing the joined result instead would
    strip the period off "et al." (_sanitize ends in .strip(".")), which is load-bearing
    for titles but wrong here.
    """
    names = [s for s in (_sanitize(surname(a)) for a in (authors or [])) if s]
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
    # author_segment sanitizes its own parts — re-sanitizing here would eat "et al."
    segments = [author_segment(authors), _sanitize(str(year)) if year else ""]
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

- [ ] **Step 5: Lint**

Run: `uv run ruff check pipeline/zotero tests/test_zotero_naming.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/__init__.py pipeline/zotero/naming.py tests/test_zotero_naming.py
git commit -m "feat: Zotero attachment filename formatting"
```

---

## Task 8: Item construction, type mapping, and dedup matching

**Files:**
- Create: `pipeline/zotero/items.py`
- Test: `tests/test_zotero_items.py`

**Interfaces:**
- Consumes: `rp.clean_doi` (Task 1), `rp.normalize_title`, `rp.strip_arxiv_version`.
- Produces, in `pipeline.zotero.items`:
  - `paper_item(paper, authors, collection_key) -> dict`
  - `book_item(book, authors, collection_key) -> dict`
  - `attachment_item(parent_key, filename) -> dict`
  - `match_existing(candidates, doi, arxiv_id, title, isbn=None) -> str | None`
  - `split_creator(name) -> dict`
  - `publisher_doi(doi) -> str | None`

  Used by Tasks 10, 11, 13.

**Item type mapping:**

| Condition | `itemType` | Fields beyond title/creators/date |
|---|---|---|
| `publication_types` contains `Conference` | `conferencePaper` | `proceedingsTitle`, `DOI`, `pages` |
| contains `JournalArticle`, or a publisher DOI exists | `journalArticle` | `publicationTitle`, `journalAbbreviation`, `volume`, `pages`, `DOI` |
| `arxiv_id` present, no publisher DOI | `preprint` | `repository`="arXiv", `archiveID`, `url` |
| none of the above | `preprint` | title, creators, date only |

`Conference` is tested **before** `JournalArticle`: S2 returns both for proceedings a journal later indexed, and the proceedings is the more specific truth.

**[REV] Three corrections from review:**

1. **`venue` beats `journal_name`, not the other way round.** S2's `journal.name` is often an abbreviation and is sometimes outright wrong. Verified live:
   - `10.1137/22M1542982` — `venue`="SIAM Journal on Financial Mathematics", `journal.name`="SIAM J. Financial Math."
   - `10.1287/moor.2021.1176` — `venue`="Mathematics of Operations Research", `journal.name`=**"ArXiv"**, `journal.volume`=**"abs/1911.03539"**

   The original draft's `journal_name or venue` would file the second paper as `publicationTitle="ArXiv"`, `volume="abs/1911.03539"` — wrong data in a real library. Corrected: `publicationTitle` takes `venue`, `journalAbbreviation` takes `journal_name` when it differs, and the whole `journal` object is discarded when `journal.name` is "ArXiv", since its `volume`/`pages` are then arXiv identifiers rather than bibliographic data.

2. **arXiv matching needs digit boundaries.** Plain substring matching was tested and produces false positives: `"1707.08464" in "https://arxiv.org/abs/11707.084640"` is `True`. A false match silently files a paper against someone else's item and it never gets its own entry.

3. **Books match on ISBN first.** Books are the items most likely already in a daily user's library, and their titles vary most (subtitle present/absent, edition suffix).

`tldr` is deliberately never written to Zotero: it is a generated summary, not bibliographic data. `split_creator`'s single-field branch takes **no `fieldMode` key** — verified against 15 live single-field creators.

- [ ] **Step 1: Write the failing test**

```python
from pipeline.zotero.items import (
    attachment_item, book_item, match_existing, paper_item, publisher_doi, split_creator,
)

JOURNAL_PAPER = {
    "title": "A Multi-agent Targeted Trading Equilibrium with Transaction Costs",
    "year": 2023,
    "doi": "10.1137/22M1542982",
    "arxiv_id": "2306.08519",
    "venue": "SIAM Journal on Financial Mathematics",
    "journal_name": "SIAM J. Financial Math.",
    "volume": "15",
    "pages": "161-193",
    "publication_types": ["JournalArticle"],
    "abstract": "We study...",
    "tldr": "A short generated summary.",
}

# Real S2 record: journal.name is "ArXiv" for a paper published in MOR.
MISLEADING_JOURNAL_PAPER = {
    "title": "Bridging Bayesian and Minimax Mean Square Error Estimation",
    "year": 2021,
    "doi": "10.1287/moor.2021.1176",
    "arxiv_id": "1911.03539",
    "venue": "Mathematics of Operations Research",
    "journal_name": "ArXiv",
    "volume": "abs/1911.03539",
    "pages": None,
    "publication_types": ["JournalArticle"],
}


def test_split_creator_two_part_name():
    assert split_creator("Bruno Bouchard") == {
        "creatorType": "author", "firstName": "Bruno", "lastName": "Bouchard"}


def test_split_creator_three_part_name_splits_on_last_space():
    assert split_creator("Jean Pierre Fouque") == {
        "creatorType": "author", "firstName": "Jean Pierre", "lastName": "Fouque"}


def test_split_creator_single_token_has_no_fieldmode():
    """fieldMode is an internal Zotero client concept, not Web API v3 — verified against
    15 live single-field creators, none of which carry it."""
    got = split_creator("Plato")
    assert got == {"creatorType": "author", "name": "Plato"}
    assert "fieldMode" not in got


def test_journal_article_uses_venue_not_the_abbreviation():
    item = paper_item(JOURNAL_PAPER, ["Bruno Bouchard"], "COLL1")
    assert item["itemType"] == "journalArticle"
    assert item["publicationTitle"] == "SIAM Journal on Financial Mathematics"
    assert item["journalAbbreviation"] == "SIAM J. Financial Math."
    assert item["volume"] == "15"
    assert item["pages"] == "161-193"
    assert item["DOI"] == "10.1137/22M1542982"
    assert item["date"] == "2023"
    assert item["collections"] == ["COLL1"]
    assert item["abstractNote"] == "We study..."


def test_arxiv_masquerading_as_a_journal_is_discarded():
    """S2 sometimes reports journal.name="ArXiv" with an "abs/..." volume for a paper
    genuinely published elsewhere. That must never reach the user's library."""
    item = paper_item(MISLEADING_JOURNAL_PAPER, [], "COLL1")
    assert item["publicationTitle"] == "Mathematics of Operations Research"
    assert "journalAbbreviation" not in item
    assert "volume" not in item, "abs/1911.03539 is not a volume"


def test_journal_abbreviation_omitted_when_identical_to_venue():
    paper = dict(JOURNAL_PAPER, journal_name="SIAM Journal on Financial Mathematics")
    assert "journalAbbreviation" not in paper_item(paper, [], "C")


def test_tldr_is_never_written_to_zotero():
    item = paper_item(JOURNAL_PAPER, ["Bruno Bouchard"], "COLL1")
    assert "A short generated summary." not in str(item)


def test_conference_wins_over_journal_article():
    paper = dict(JOURNAL_PAPER, publication_types=["JournalArticle", "Conference"])
    item = paper_item(paper, [], "COLL1")
    assert item["itemType"] == "conferencePaper"
    assert item["proceedingsTitle"] == "SIAM Journal on Financial Mathematics"
    assert "publicationTitle" not in item


def test_publisher_doi_alone_implies_journal_article():
    paper = dict(JOURNAL_PAPER, publication_types=[])
    assert paper_item(paper, [], "C")["itemType"] == "journalArticle"


def test_venue_falls_back_to_journal_name_when_venue_absent():
    paper = dict(JOURNAL_PAPER, venue=None)
    assert paper_item(paper, [], "C")["publicationTitle"] == "SIAM J. Financial Math."


def test_arxiv_only_maps_to_preprint():
    paper = {"title": "A Preprint", "year": 2025, "arxiv_id": "2503.13804",
             "doi": None, "publication_types": []}
    item = paper_item(paper, [], "C")
    assert item["itemType"] == "preprint"
    assert item["repository"] == "arXiv"
    assert item["archiveID"] == "arXiv:2503.13804"
    assert item["url"] == "https://arxiv.org/abs/2503.13804"


def test_arxiv_doi_is_not_a_publisher_doi():
    paper = {"title": "T", "year": 2023, "arxiv_id": "2305.16261",
             "doi": "10.48550/arXiv.2305.16261", "publication_types": []}
    assert paper_item(paper, [], "C")["itemType"] == "preprint"
    assert publisher_doi("10.48550/arXiv.2305.16261") is None


def test_ssrn_doi_is_not_a_publisher_doi():
    assert publisher_doi("10.2139/ssrn.3594076") is None


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


def test_attachment_item_includes_required_tags_and_relations():
    """The API docs list tags and relations as required on item creation."""
    att = attachment_item("PARENT1", "A Study - Lovelace - 2020.pdf")
    assert att == {
        "itemType": "attachment", "parentItem": "PARENT1", "linkMode": "imported_file",
        "title": "A Study - Lovelace - 2020.pdf",
        "filename": "A Study - Lovelace - 2020.pdf",
        "contentType": "application/pdf",
        "tags": [], "relations": {},
    }
    assert "collections" not in att, "child items cannot be collection members"


# --- dedup matching ---------------------------------------------------------------

CANDIDATES = [
    {"key": "K_DOI", "data": {"DOI": "10.1137/22M1542982", "title": "Something Else"}},
    {"key": "K_ARXIV", "data": {"archiveID": "arXiv:2306.08519", "title": "Other"}},
    {"key": "K_TITLE", "data": {"title": "A Multi-agent Targeted Trading Equilibrium"}},
]


def test_doi_match_wins():
    assert match_existing(CANDIDATES, "10.1137/22M1542982", "2306.08519",
                          "A Multi-agent Targeted Trading Equilibrium") == "K_DOI"


def test_arxiv_match_beats_title():
    assert match_existing(CANDIDATES, None, "2306.08519",
                          "A Multi-agent Targeted Trading Equilibrium") == "K_ARXIV"


def test_title_match_is_the_last_resort():
    assert match_existing(CANDIDATES, None, None,
                          "A Multi-agent Targeted Trading Equilibrium") == "K_TITLE"


def test_title_match_is_case_and_whitespace_insensitive():
    assert match_existing(CANDIDATES, None, None,
                          "  A MULTI-AGENT   targeted Trading Equilibrium ") == "K_TITLE"


def test_arxiv_match_ignores_version_suffix():
    assert match_existing(CANDIDATES, None, "2306.08519v3", None) == "K_ARXIV"


def test_arxiv_match_also_reads_the_url_field():
    cands = [{"key": "K_URL", "data": {"url": "https://arxiv.org/abs/2503.13804"}}]
    assert match_existing(cands, None, "2503.13804", None) == "K_URL"


def test_arxiv_match_requires_digit_boundaries():
    """Regression: plain substring matching made 1707.08464 match 11707.084640, filing a
    paper against an unrelated item."""
    cands = [{"key": "K", "data": {"url": "https://arxiv.org/abs/11707.084640"}}]
    assert match_existing(cands, None, "1707.08464", None) is None
    cands2 = [{"key": "K", "data": {"url": "https://example.com/x2503.138040"}}]
    assert match_existing(cands2, None, "2503.13804", None) is None


def test_isbn_match_wins_for_books():
    cands = [{"key": "K_ISBN", "data": {"ISBN": "978-0-521-40605-5", "title": "Other"}},
             {"key": "K_TITLE", "data": {"title": "Probability with Martingales"}}]
    assert match_existing(cands, None, None, "Probability with Martingales",
                          isbn="9780521406055") == "K_ISBN"


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

```python
"""Zotero item construction and deduplication matching.

Pure — builds dicts and compares them. All HTTP lives in client.py, so every rule here
(item-type mapping, creator splitting, match precedence) is unit-testable without fakes.

Field-name casing is load-bearing: Zotero silently drops unknown keys, so `DOI` and `ISBN`
must be uppercase. Verified against the live items/new templates, schema v42.
"""
from __future__ import annotations

import re

from pipeline.graph.research_port import clean_doi, normalize_title, strip_arxiv_version

# DOI registrants that mint DOIs for preprints, not publications. A paper carrying only
# one of these is still a preprint.
_PREPRINT_DOI_PREFIXES = ("10.48550/", "10.2139/ssrn")

# S2 sometimes reports journal.name == "ArXiv" with an "abs/..." volume for a paper that
# venue correctly identifies as published elsewhere. The whole journal object is junk then.
_JUNK_JOURNAL_NAMES = {"arxiv", "arxiv.org", "corr"}

_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]")


def publisher_doi(doi: str | None) -> str | None:
    """A cleaned DOI, unless it belongs to a preprint registrar (arXiv, SSRN)."""
    d = clean_doi(doi)
    if d is None:
        return None
    return None if d.lower().startswith(_PREPRINT_DOI_PREFIXES) else d


def split_creator(name: str) -> dict:
    """Zotero two-field mode, splitting on the last space. A single-token name uses
    Zotero's single-field mode — which takes `name` alone, with NO `fieldMode` key
    (that is an internal Zotero client concept, not Web API v3)."""
    parts = (name or "").strip().split()
    if len(parts) < 2:
        return {"creatorType": "author", "name": " ".join(parts)}
    return {"creatorType": "author", "firstName": " ".join(parts[:-1]),
            "lastName": parts[-1]}


def _journal_fields(paper: dict) -> tuple[str | None, str | None, str | None, str | None]:
    """(publication title, abbreviation, volume, pages), with junk journal data dropped.

    `venue` is the authoritative display name; `journal.name` is often an abbreviation and
    is occasionally "ArXiv" for a paper published elsewhere, in which case its volume is an
    arXiv identifier rather than a volume number and the whole object must be discarded.
    """
    venue = paper.get("venue")
    journal_name = paper.get("journal_name")
    volume, pages = paper.get("volume"), paper.get("pages")

    if journal_name and journal_name.strip().lower() in _JUNK_JOURNAL_NAMES:
        journal_name, volume, pages = None, None, None

    title = venue or journal_name
    abbrev = journal_name if (journal_name and journal_name != title) else None
    return title, abbrev, volume, pages


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
    pub_title, abbrev, volume, pages = _journal_fields(paper)
    arxiv = paper.get("arxiv_id")

    if paper.get("abstract"):
        item["abstractNote"] = paper["abstract"]

    # "conference" before "journalarticle": S2 returns both for proceedings a journal later
    # indexed, and the proceedings is the more specific truth. Casing has no space —
    # S2's own OpenAPI example says "Journal Article" and is stale; live values do not.
    if "conference" in types:
        item["itemType"] = "conferencePaper"
        if pub_title:
            item["proceedingsTitle"] = pub_title
        if pages:
            item["pages"] = pages
        if doi:
            item["DOI"] = doi
        return item

    if "journalarticle" in types or doi:
        item["itemType"] = "journalArticle"
        if pub_title:
            item["publicationTitle"] = pub_title
        if abbrev:
            item["journalAbbreviation"] = abbrev
        if volume:
            item["volume"] = volume
        if pages:
            item["pages"] = pages
        if doi:
            item["DOI"] = doi
        return item

    # preprint has no publicationTitle/volume/pages field — do not set them here.
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
    """A child attachment. `collections` is omitted: child items cannot be members."""
    return {
        "itemType": "attachment",
        "parentItem": parent_key,
        "linkMode": "imported_file",
        "title": filename,
        "filename": filename,
        "contentType": "application/pdf",
        "tags": [],
        "relations": {},
    }


def _norm_isbn(value: str | None) -> str | None:
    return _NON_ALNUM.sub("", value).upper() if value else None


def match_existing(candidates: list[dict], doi: str | None, arxiv_id: str | None,
                   title: str | None, isbn: str | None = None) -> str | None:
    """Return the Zotero item key of an existing item describing this work, or None.

    Precedence: ISBN (books), cleaned DOI, arXiv id, then normalized title. A placeholder
    DOI is never used for matching.
    """
    want_isbn = _norm_isbn(isbn)
    if want_isbn:
        for c in candidates:
            if _norm_isbn((c.get("data") or {}).get("ISBN")) == want_isbn:
                return c["key"]

    target_doi = clean_doi(doi)
    if target_doi:
        want = target_doi.lower()
        for c in candidates:
            cand = clean_doi((c.get("data") or {}).get("DOI"))
            if cand and cand.lower() == want:
                return c["key"]

    if arxiv_id:
        bare = strip_arxiv_version(arxiv_id.strip().lower())
        if bare:
            # Digit boundaries: plain substring matching made "1707.08464" match
            # "11707.084640", filing a paper against an unrelated item.
            pattern = re.compile(rf"(?<!\d){re.escape(bare)}(?!\d)")
            for c in candidates:
                data = c.get("data") or {}
                haystack = f"{data.get('archiveID') or ''} {data.get('url') or ''}".lower()
                if pattern.search(haystack):
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
Expected: PASS

- [ ] **Step 5: Verify lint and full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/items.py tests/test_zotero_items.py
git commit -m "feat: Zotero item construction, type mapping, and dedup matching"
```

---

## Task 9: `ZoteroResource` and the collections bootstrap

**Files:**
- Modify: `pipeline/runtime/resources.py` (add `ZoteroResource` after `AnthropicResource`)
- Modify: `.env.example`
- Create: `pipeline/zotero/client.py`
- Test: `tests/test_zotero_client.py`

**Interfaces:**
- Produces:
  - `ZoteroResource` in `pipeline.runtime.resources` with `api_key`, `user_id`, `base_url`, `request_timeout`, `configured` (property), and `get_client() -> ZoteroClient`.
  - `ZoteroClient(api_key, user_id, base_url=..., timeout=60.0, http=None)` with `list_collections()`, `create_collections(payload)`, `ensure_collections() -> {"papers": key, "books": key}`.
  - `ZoteroClientError` (4xx that will not resolve) and `ZoteroTransientError` (retries exhausted).

  Used by Tasks 10-13.

**Context:** One top-level `Alethograph` collection with `Papers` and `Books` subcollections, created idempotently. The user's library already contains 45 collections; nothing outside `Alethograph` is created, renamed, or deleted, and matching is on name **and** parent so a stray `Papers` elsewhere is never adopted. `parentCollection` is literally `false` for a top-level collection — verified live.

**[REV] Two corrections:** 409 (library locked) is genuinely transient and belongs in the retryable set. The Backoff test must use a range, not an exact float.

- [ ] **Step 1: Write the failing test**

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
    assert call["headers"]["Zotero-API-Version"] == "3"
    assert "KEY" not in call["url"]


def test_ensure_collections_creates_all_three_when_library_is_empty():
    http = FakeHTTP([
        FakeResponse(json_data=[]),                                        # list
        FakeResponse(json_data={"successful": {"0": {"key": "ALEPH"}}}),   # create root
        FakeResponse(json_data={"successful": {"0": {"key": "PAP"},
                                               "1": {"key": "BKS"}}}),    # create children
    ])
    assert client(http).ensure_collections() == {"papers": "PAP", "books": "BKS"}


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


def test_ensure_collections_creates_only_the_missing_child():
    existing = [
        {"key": "ALEPH", "data": {"name": "Alethograph", "parentCollection": False}},
        {"key": "PAP", "data": {"name": "Papers", "parentCollection": "ALEPH"}},
    ]
    http = FakeHTTP([
        FakeResponse(json_data=existing),
        FakeResponse(json_data={"successful": {"0": {"key": "BKS"}}}),
    ])
    c = client(http)
    assert c.ensure_collections() == {"papers": "PAP", "books": "BKS"}
    assert http.calls[1]["json"] == [{"name": "Books", "parentCollection": "ALEPH"}]


def test_ensure_collections_ignores_same_named_collection_under_another_parent():
    existing = [
        {"key": "ALEPH", "data": {"name": "Alethograph", "parentCollection": False}},
        {"key": "STRAY", "data": {"name": "Papers", "parentCollection": "UNRELATED"}},
    ]
    http = FakeHTTP([
        FakeResponse(json_data=existing),
        FakeResponse(json_data={"successful": {"0": {"key": "PAP"}, "1": {"key": "BKS"}}}),
    ])
    assert client(http).ensure_collections() == {"papers": "PAP", "books": "BKS"}


def test_list_collections_paginates_until_short_page():
    page1 = [{"key": f"K{i}", "data": {"name": str(i), "parentCollection": False}}
             for i in range(100)]
    page2 = [{"key": "LAST", "data": {"name": "last", "parentCollection": False}}]
    http = FakeHTTP([FakeResponse(json_data=page1), FakeResponse(json_data=page2)])
    assert len(client(http).list_collections()) == 101
    assert http.calls[1]["params"]["start"] == 100


def test_client_error_raises():
    http = FakeHTTP([FakeResponse(status_code=403, text="Forbidden")])
    with pytest.raises(ZoteroClientError):
        client(http).list_collections()


@pytest.mark.parametrize("status", [429, 409, 500, 503])
def test_retryable_statuses_retry_then_raise_transient(monkeypatch, status):
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: None)
    http = FakeHTTP([FakeResponse(status_code=status, headers={"Retry-After": "1"})
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


def test_backoff_header_delays_the_next_request(monkeypatch):
    slept = []
    monkeypatch.setattr("pipeline.zotero.client.time.sleep", lambda s: slept.append(s))
    http = FakeHTTP([
        FakeResponse(json_data=[], headers={"Backoff": "3"}),
        FakeResponse(json_data=[]),
    ])
    c = client(http)
    c.list_collections()
    c.list_collections()
    # Elapsed monotonic time makes the wait fractionally under 3 — assert a range, never
    # exact equality.
    assert any(2 < s <= 3 for s in slept)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_zotero_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pipeline.zotero.client'`

- [ ] **Step 3: Write the client**

```python
"""Zotero Web API v3 transport.

Hand-rolled on `requests`, mirroring how research_port.py handles Semantic Scholar —
pyzotero would be a new dependency for a handful of endpoints. All item-shaping logic
lives in items.py; this module only moves bytes.

The API key goes in the Zotero-API-Key header, never a URL, and is withheld entirely from
absolute-URL requests, which target Zotero's third-party storage host rather than the API.
"""
from __future__ import annotations

import hashlib
import logging
import time

import requests
from requests import RequestException

log = logging.getLogger(__name__)

PAGE_SIZE = 100        # Zotero's documented maximum; larger values are silently clamped.
BATCH_LIMIT = 50       # Item/collection creation cap per request.
# 409 = target library locked, genuinely transient.
_RETRYABLE_STATUS = frozenset({409, 429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 4

COLLECTION_ROOT = "Alethograph"
COLLECTION_PAPERS = "Papers"
COLLECTION_BOOKS = "Books"

# Excludes attachments and notes. The leading "-" negates the whole OR expression, not
# just the first term — verified empirically; a web search gives the wrong answer here.
NON_FILE_ITEMS = "-attachment || note"


class ZoteroClientError(RuntimeError):
    """A 4xx that will not resolve by retrying: bad payload, revoked key, missing item."""


class ZoteroTransientError(RuntimeError):
    """Throttling, a lock, or a 5xx that survived every retry. Callers log and continue."""


class ZoteroQuotaError(ZoteroClientError):
    """HTTP 413 — the upload would exceed the library owner's storage quota.

    Subclasses ZoteroClientError so it is never silently retried, but callers catch it
    specifically: the item was created, only its file could not be stored.
    """


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
        """One API call with Backoff/Retry-After handling and bounded retries.

        `absolute=True` targets a non-Zotero host (the storage backend during upload) and
        therefore sends NO credentials — the request is authorized by the signature Zotero
        embeds in the multipart prefix, and leaking the key to a third party is forbidden.
        """
        url = path if absolute else f"{self.prefix}{path}"
        for attempt in range(_MAX_ATTEMPTS):
            wait = self._backoff_until - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._backoff_until = 0.0

            sent = dict(headers or {}) if absolute else self._headers(headers)
            try:
                resp = self._http(method, url, params=params, json=json_body, data=data,
                                  headers=sent, timeout=self.timeout)
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
            if resp.status_code == 413:
                raise ZoteroQuotaError(
                    f"Zotero {method} {path} -> 413: storage quota exceeded")
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

        Matches on name AND parent, so an unrelated 'Papers' elsewhere in the user's 45
        collections is never adopted. A top-level collection's parentCollection is the
        literal boolean false, not null — verified live.
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

        found = {"papers": find(COLLECTION_PAPERS, root),
                 "books": find(COLLECTION_BOOKS, root)}
        missing = [(dest, name) for dest, name in
                   (("papers", COLLECTION_PAPERS), ("books", COLLECTION_BOOKS))
                   if found[dest] is None]
        if missing:
            keys = self.create_collections(
                [{"name": name, "parentCollection": root} for _, name in missing])
            for (dest, _), key in zip(missing, keys):
                found[dest] = key
        return found
```

- [ ] **Step 4: Add `ZoteroResource`**

```python
class ZoteroResource(ConfigurableResource):
    """Zotero Web API v3 — personal library sync for ingested papers and books.

    Uses os.environ.get with a default (like OpenAILLMResource) rather than os.environ[...]
    (like minio_from_env), so an unconfigured install still loads the Dagster definitions.
    Assets must check `configured` before building a client — see zotero_push.
    """
    api_key: str = Field(default_factory=lambda: os.environ.get("ZOTERO_API_KEY", ""))
    user_id: str = Field(default_factory=lambda: os.environ.get("ZOTERO_USER_ID", ""))
    base_url: str = "https://api.zotero.org"
    request_timeout: float = 60.0

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.user_id)

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
# https://www.zotero.org/settings/keys . If unset, the Zotero push assets no-op.
ZOTERO_API_KEY=
ZOTERO_USER_ID=
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_client.py -v`
Expected: PASS

- [ ] **Step 7: Confirm definitions still load without Zotero configured**

Run: `uv run python -c "from pipeline.runtime.resources import ZoteroResource; r = ZoteroResource(api_key='', user_id=''); print('configured:', r.configured)"`
Expected: `configured: False` with no exception.

- [ ] **Step 8: Verify lint and full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add pipeline/zotero/client.py pipeline/runtime/resources.py .env.example tests/test_zotero_client.py
git commit -m "feat: Zotero client transport, retry policy, and collections bootstrap"
```

---

## Task 10: Item creation, candidate search, and three-step file upload

**Files:**
- Modify: `pipeline/zotero/client.py`
- Test: `tests/test_zotero_client.py` (append)

**Interfaces:**
- Produces, as `ZoteroClient` methods:
  - `search_candidates(title, limit=25) -> list[dict]`
  - `library_index() -> list[dict]`
  - `create_items(payload) -> list[str]`
  - `get_item(item_key) -> dict`
  - `has_attachment(item_key) -> bool`
  - `add_to_collection(item_key, collection_key) -> bool`
  - `upload_attachment(item_key, filename, data) -> str` returning `"uploaded"` | `"exists"`

  Used by Tasks 11-13.

**The verified three-step upload:**
1. **Authorize** — `POST /users/<id>/items/<key>/file`, `Content-Type: application/x-www-form-urlencoded`, `If-None-Match: *`, body `md5`/`filename`/`filesize`/`mtime`. mtime is **milliseconds** ("Note that `mtime` must be provided in milliseconds, not seconds").
2. **Upload** — if upload params returned, POST `prefix` + bytes + `suffix` to `url` with the returned `contentType` → 201. If `{"exists": 1}`, Zotero already holds that file and has associated it; stop.
3. **Register** — `POST .../file` with `upload=<uploadKey>` and the same conditional header → 204.

**[REV] `has_attachment` is new**, needed so Task 11 can attach a PDF to a matched item that has none.

- [ ] **Step 1: Write the failing tests**

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


def test_has_attachment_detects_a_child_attachment():
    http = FakeHTTP([FakeResponse(json_data=[{"key": "ATT", "data": {
        "itemType": "attachment", "linkMode": "imported_file"}}])])
    assert client(http).has_attachment("ITEM") is True


def test_has_attachment_false_for_a_metadata_only_item():
    http = FakeHTTP([FakeResponse(json_data=[])])
    assert client(http).has_attachment("ITEM") is False


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
    assert len(http.calls) == 1


def test_upload_attachment_short_circuits_when_file_exists():
    http = FakeHTTP([FakeResponse(json_data={"exists": 1})])
    assert client(http).upload_attachment("ITEM", "f.pdf", b"data") == "exists"
    assert len(http.calls) == 1


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


def test_upload_never_sends_the_api_key_to_the_storage_host():
    """Step 2 targets Zotero's S3 backend, not the API. The key must not leave our host."""
    http = FakeHTTP([
        FakeResponse(json_data={"url": "https://s3.example/put", "contentType": "text/plain",
                                "prefix": "PRE", "suffix": "SUF", "uploadKey": "UK"}),
        FakeResponse(status_code=201),
        FakeResponse(status_code=204),
    ])
    client(http).upload_attachment("ITEM", "f.pdf", b"data")
    assert "Zotero-API-Key" not in http.calls[1]["headers"]
    assert "Zotero-API-Key" in http.calls[0]["headers"], "the API call still authenticates"


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


def test_quota_exceeded_raises_the_specific_error():
    from pipeline.zotero.client import ZoteroQuotaError
    http = FakeHTTP([FakeResponse(status_code=413, text="quota")])
    with pytest.raises(ZoteroQuotaError):
        client(http).upload_attachment("ITEM", "f.pdf", b"data")


def test_search_candidates_returns_empty_without_a_title():
    http = FakeHTTP([])
    assert client(http).search_candidates(None) == []
    assert http.calls == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_zotero_client.py -v -k "create_items or attachment or upload or search or quota"`
Expected: FAIL — the methods do not exist.

- [ ] **Step 3: Write the implementation**

Append to `ZoteroClient`:

```python
    # --- items -------------------------------------------------------------------

    def search_candidates(self, title: str | None, limit: int = 25) -> list[dict]:
        """Candidate items for deduplication, found by title. One request.

        Used on the per-ingest path; the backfill uses library_index() so it does not
        issue one search per item. qmode=titleCreatorYear is the documented default.
        """
        if not title or not title.strip():
            return []
        return self.request("GET", "/items", params={
            "q": title.strip(), "qmode": "titleCreatorYear", "limit": limit,
            "format": "json", "itemType": NON_FILE_ITEMS,
        }).json()

    def library_index(self) -> list[dict]:
        """Every non-attachment, non-note item in the library. One paginated sweep,
        reused across a whole backfill run."""
        return self._paginate("/items", {"itemType": NON_FILE_ITEMS})

    def create_items(self, payload: list[dict]) -> list[str]:
        """Create items, returning their keys in submission order."""
        if len(payload) > BATCH_LIMIT:
            raise ValueError(f"Zotero accepts at most {BATCH_LIMIT} items per request")
        resp = self.request("POST", "/items", json_body=payload).json()
        failed = resp.get("failed") or {}
        if failed:
            raise ZoteroClientError(f"Zotero rejected {len(failed)} item(s): {failed}")
        successful = resp.get("successful") or {}
        # Zotero also has an `unchanged` bucket. Fresh creates never land there, but
        # indexing `successful` blindly would KeyError if one ever did.
        unchanged = resp.get("unchanged") or {}
        missing = [i for i in range(len(payload)) if str(i) not in successful]
        if missing:
            raise ZoteroClientError(
                f"Zotero returned no key for item(s) {missing} "
                f"(unchanged={list(unchanged)})")
        return [successful[str(i)]["key"] for i in range(len(payload))]

    def get_item(self, item_key: str) -> dict:
        return self.request("GET", f"/items/{item_key}").json()

    def has_attachment(self, item_key: str) -> bool:
        """True if the item already has a child attachment.

        Lets the push attach a PDF to a metadata-only item the user saved themselves,
        while never adding a second file to one that already has one.
        """
        children = self.request("GET", f"/items/{item_key}/children",
                                params={"format": "json"}).json()
        return any((c.get("data") or {}).get("itemType") == "attachment"
                   for c in children)

    def add_to_collection(self, item_key: str, collection_key: str) -> bool:
        """Add an existing item to a collection without touching any other field.

        Returns False if already a member. PATCH carries only `collections`; Zotero leaves
        properties absent from the body untouched, so the user's metadata is never rewritten.
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

        Raises ZoteroQuotaError on 413 so callers can report a quota problem distinctly
        from a code defect. "exists" means Zotero already stores a file with this md5 and
        has associated it with the item — free server-side deduplication.
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
        # absolute=True withholds the API key: this host is Zotero's storage backend.
        self.request("POST", auth["url"], absolute=True, data=body,
                     headers={"Content-Type": auth["contentType"]})

        self.request("POST", f"/items/{item_key}/file", headers=auth_headers,
                     data={"upload": auth["uploadKey"]})
        return "uploaded"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_client.py -v`
Expected: PASS

- [ ] **Step 5: Verify lint and full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/client.py tests/test_zotero_client.py
git commit -m "feat: Zotero item creation, candidate search, and three-step file upload"
```

---

## Task 11: Push orchestration and completion markers

**Files:**
- Create: `pipeline/zotero/push.py`
- Test: `tests/test_zotero_push.py`

**Interfaces:**
- Consumes: `attachment_filename` (Task 7); `paper_item`, `book_item`, `attachment_item`, `match_existing` (Task 8); `ZoteroClient` and its errors (Tasks 9-10).
- Produces, in `pipeline.zotero.push`:
  - `PAPER_FOR_PUSH`, `BOOK_FOR_PUSH`, `PAPERS_NEEDING_PUSH`, `BOOKS_NEEDING_PUSH`, `MARK_PAPER_PUSHED`, `MARK_BOOK_PUSHED`
  - `push_one(client, collection_key, record, authors, pdf_bytes, candidates=None) -> dict`

  Used by Tasks 12-13.

**[REV] Three corrections, all of which changed behaviour:**

1. **A matched item that has no attachment now gets one.** The original draft skipped the PDF for *every* matched item, then marked the record permanently done. A metadata-only Zotero entry — exactly what a "save to Zotero" browser click produces — would be filed under `Alethograph/Papers` and left un-openable forever, while reporting success. That defeats the project's stated goal. The rule is now the spec's actual rule: attach unless the item already has an attachment.
2. **`zotero_key` is only written once the attachment step has resolved.** It was previously set before the upload, so an upload failure recorded permanent success and the repair query never revisited the record. `push_one` now returns `complete`, and the assets write the marker only when it is true.
3. **`ZoteroQuotaError` is caught, not raised.** A full storage quota returns 413 on the file-authorization step. Treating it as fatal would fail every ingest run once the quota fills — against the plan's own governing constraint — and after the item was already created. It is now reported as `attachment="quota-exceeded"`, non-fatal, with the record left for a later retry.

- [ ] **Step 1: Write the failing test**

```python
import pytest

from pipeline.zotero.client import ZoteroClientError, ZoteroQuotaError, ZoteroTransientError
from pipeline.zotero.push import push_one

PAPER = {
    "id": "arxiv:2503.13804", "title": "A Preprint", "year": 2025,
    "arxiv_id": "2503.13804", "doi": None, "publication_types": [],
    "kind": "paper",
}


class StubClient:
    def __init__(self, candidates=None, created="NEWKEY", upload="uploaded",
                 raises=None, upload_raises=None, has_att=False):
        self._candidates = candidates or []
        self._created = created
        self._upload = upload
        self._raises = raises
        self._upload_raises = upload_raises
        self._has_att = has_att
        self.calls = []

    def search_candidates(self, title, limit=25):
        self.calls.append(("search", title))
        return self._candidates

    def create_items(self, payload):
        if self._raises:
            raise self._raises
        self.calls.append(("create", payload[0].get("itemType")))
        return [self._created if payload[0]["itemType"] != "attachment" else "ATTKEY"]

    def add_to_collection(self, item_key, collection_key):
        self.calls.append(("add_to_collection", item_key, collection_key))
        return True

    def has_attachment(self, item_key):
        self.calls.append(("has_attachment", item_key))
        return self._has_att

    def upload_attachment(self, item_key, filename, data):
        if self._upload_raises:
            raise self._upload_raises
        self.calls.append(("upload", item_key, filename, len(data)))
        return self._upload


def test_creates_item_and_uploads_when_no_match():
    c = StubClient()
    out = push_one(c, "COLL", PAPER, ["Ada Lovelace"], b"%PDF-1.4")
    assert out["pushed"] is True and out["complete"] is True
    assert out["outcome"] == "created"
    assert out["zotero_key"] == "NEWKEY"
    assert out["item_type"] == "preprint"
    assert out["filename"] == "A Preprint - Lovelace - 2025.pdf"
    assert any(call[0] == "upload" for call in c.calls)


def test_matched_item_without_an_attachment_gets_the_pdf():
    """A metadata-only item the user saved from a browser must become openable."""
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att=False)
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["outcome"] == "matched"
    assert out["zotero_key"] == "EXISTING"
    assert out["attachment"] == "uploaded"
    assert ("add_to_collection", "EXISTING", "COLL") in c.calls
    assert any(call[0] == "upload" for call in c.calls)


def test_matched_item_with_an_attachment_is_left_alone():
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att=True)
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["attachment"] == "skipped-has-attachment"
    assert not any(call[0] == "upload" for call in c.calls)


def test_matched_item_is_never_recreated():
    candidates = [{"key": "EXISTING", "data": {"archiveID": "arXiv:2503.13804"}}]
    c = StubClient(candidates=candidates, has_att=True)
    push_one(c, "COLL", PAPER, [], b"%PDF")
    assert not any(call[0] == "create" for call in c.calls)


def test_supplied_candidates_skip_the_search_request():
    c = StubClient()
    push_one(c, "COLL", PAPER, [], b"%PDF", candidates=[])
    assert not any(call[0] == "search" for call in c.calls)


def test_transient_error_reports_incomplete_and_does_not_raise():
    c = StubClient(raises=ZoteroTransientError("throttled"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["pushed"] is False and out["complete"] is False
    assert "throttled" in out["reason"]
    assert out["zotero_key"] is None


def test_upload_failure_leaves_the_record_incomplete():
    """zotero_key must not be committed when the PDF never landed, or the repair query
    will never revisit it and the item stays un-openable forever."""
    c = StubClient(upload_raises=ZoteroTransientError("throttled"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["zotero_key"] == "NEWKEY", "the item was created; report its key"
    assert out["complete"] is False, "but do not mark the record done"


def test_quota_exceeded_is_reported_not_raised():
    c = StubClient(upload_raises=ZoteroQuotaError("413"))
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["attachment"] == "quota-exceeded"
    assert out["complete"] is False


def test_client_error_propagates():
    c = StubClient(raises=ZoteroClientError("HTTP 400: bad field"))
    with pytest.raises(ZoteroClientError):
        push_one(c, "COLL", PAPER, [], b"%PDF")


def test_book_records_use_the_book_item_shape():
    book = {"id": "isbn:9780521406055", "title": "Probability with Martingales",
            "year": 1991, "publisher": "CUP", "isbn": "9780521406055", "kind": "book"}
    c = StubClient()
    out = push_one(c, "BOOKS", book, ["David Williams"], b"%PDF")
    assert out["item_type"] == "book"
    assert out["filename"] == "Probability with Martingales - Williams - 1991.pdf"


def test_missing_pdf_still_creates_the_item_and_is_complete():
    c = StubClient()
    out = push_one(c, "COLL", PAPER, [], None)
    assert out["pushed"] is True and out["complete"] is True
    assert out["attachment"] == "skipped-no-pdf"


def test_exists_upload_is_reported_and_complete():
    c = StubClient(upload="exists")
    out = push_one(c, "COLL", PAPER, [], b"%PDF")
    assert out["attachment"] == "exists" and out["complete"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_zotero_push.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pipeline.zotero.push'`

- [ ] **Step 3: Write the implementation**

```python
"""Push orchestration shared by the Dagster assets and the backfill script.

One code path so the asset and the backfill cannot diverge in how an item is built,
matched, or attached. push_one() returns a result dict rather than raising on transient
trouble or a full quota: a Zotero problem must never fail an ingest run whose graph write
has already succeeded.

`complete` is the contract with callers — write zotero_key ONLY when it is true. Anything
else leaves the record visible to the repair query so a later run finishes the job.
"""
from __future__ import annotations

import logging

from pipeline.zotero.client import ZoteroQuotaError, ZoteroTransientError
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


def _attach(client, item_key: str, filename: str, pdf_bytes: bytes | None) -> str:
    """Upload the PDF, returning a status string. Never raises for quota or throttling."""
    if not pdf_bytes:
        return "skipped-no-pdf"
    try:
        return client.upload_attachment(item_key, filename, pdf_bytes)
    except ZoteroQuotaError:
        log.warning("Zotero storage quota exceeded uploading %s", filename)
        return "quota-exceeded"


def push_one(client, collection_key: str, record: dict, authors: list[str],
             pdf_bytes: bytes | None, candidates: list[dict] | None = None) -> dict:
    """File one paper or book into Zotero.

    `record` carries a "kind" of "paper" or "book". `candidates` lets the backfill supply a
    pre-fetched library index instead of one search per item; when None, one search is issued.

    Returns {pushed, complete, outcome, zotero_key, item_type, filename, attachment, reason}.
    Raises only ZoteroClientError (a payload defect or revoked key) — never on throttling,
    a locked library, or an exhausted storage quota.
    """
    is_book = record.get("kind") == "book"
    result = {"pushed": False, "complete": False, "outcome": None, "zotero_key": None,
              "item_type": None, "filename": None, "attachment": None, "reason": None}
    filename = attachment_filename(record.get("title"), authors, record.get("year"))
    result["filename"] = filename

    try:
        pool = client.search_candidates(record.get("title")) if candidates is None \
            else candidates
        existing = match_existing(pool, record.get("doi"), record.get("arxiv_id"),
                                  record.get("title"),
                                  isbn=record.get("isbn") if is_book else None)

        if existing:
            client.add_to_collection(existing, collection_key)
            result.update(pushed=True, outcome="matched", zotero_key=existing)
            # Attach only when the user's item has no file — a metadata-only entry saved
            # from a browser is exactly the case this project exists to make openable.
            if client.has_attachment(existing):
                result.update(attachment="skipped-has-attachment", complete=True)
            else:
                status = _attach(client, existing, filename, pdf_bytes)
                result.update(attachment=status,
                              complete=status != "quota-exceeded")
            return result

        item = (book_item(record, authors, collection_key) if is_book
                else paper_item(record, authors, collection_key))
        result["item_type"] = item["itemType"]
        key = client.create_items([item])[0]
        result.update(pushed=True, outcome="created", zotero_key=key)

        if not pdf_bytes:
            result.update(attachment="skipped-no-pdf", complete=True)
            return result

        att_key = client.create_items([attachment_item(key, filename)])[0]
        status = _attach(client, att_key, filename, pdf_bytes)
        result.update(attachment=status, complete=status != "quota-exceeded")
        return result

    except ZoteroTransientError as exc:
        # Throttling, a locked library, or a 5xx that survived retries. `complete` stays
        # False, so the caller withholds zotero_key and the repair query revisits this.
        log.warning("Zotero push incomplete for %s: %s", record.get("id"), exc)
        result["reason"] = str(exc)
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_zotero_push.py -v`
Expected: PASS

- [ ] **Step 5: Verify lint and full suite**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/zotero/push.py tests/test_zotero_push.py
git commit -m "feat: Zotero push orchestration with attachment repair and quota tolerance"
```

---

## Task 12: The Dagster assets and pipeline wiring

**Files:**
- Create: `pipeline/assets/zotero_push.py`, `pipeline/assets/book_zotero_push.py`
- Modify: `pipeline/runtime/jobs.py`, `pipeline/definitions.py`
- Create: `tests/integration/test_zotero_integration.py`

**Interfaces:**
- Consumes: `push_one` and the Cypher constants (Task 11), `ZoteroResource` (Task 9).
- Produces: assets `zotero_push` and `book_zotero_push`.

**Context:** Both assets are thin wrappers: read node + authors from Neo4j, fetch the PDF from `RAW_BUCKET/{key}.pdf`, delegate to `push_one`, write back `zotero_key` **only when `complete`**. `deps=[...]` as a string list is the codebase idiom (`triage_metadata.py:34`, `paper_analysis.py:27`).

`book_structure_write` is the correct book dependency: `book_metadata.py:2` states "DECIDES identity only — Book/Author nodes are written by book_structure_write", so that is the first point where the Book node and its authors exist. `book_chapters_sensor` keys off `book_structure_write`'s own materialization (`sensors.py:34`), so adding this asset to the job does not delay chapter extraction.

**[REV] Both assets must no-op when Zotero is unconfigured.** An empty API key yields a 403 → `ZoteroClientError` → a red asset at the end of every `ingest_document` run for anyone who has not set up Zotero, directly violating the plan's governing constraint.

- [ ] **Step 1: Write the paper asset**

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
from pipeline.zotero.client import ZoteroClientError, ZoteroTransientError


def fetch_pdf(s3, key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")["Body"].read()
    except botocore.exceptions.ClientError:
        return None


def _result(out: dict) -> MaterializeResult:
    return MaterializeResult(metadata={
        "pushed": out["pushed"], "complete": out["complete"],
        "outcome": out["outcome"] or "", "zotero_key": out["zotero_key"] or "",
        "item_type": out["item_type"] or "", "filename": out["filename"] or "",
        "attachment": out["attachment"] or "", "reason": out["reason"] or "",
    })


@asset(partitions_def=documents_partitions_def(), deps=["paper_analysis"],
       required_resource_keys={"minio", "neo4j_new", "zotero"})
def zotero_push(context) -> MaterializeResult:
    key = context.partition_key
    zotero = context.resources.zotero
    if not zotero.configured:
        return MaterializeResult(metadata={"pushed": False,
                                           "reason": "Zotero not configured "
                                                     "(ZOTERO_API_KEY / ZOTERO_USER_ID)"})

    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(zp.PAPER_FOR_PUSH, document_id=key).single()
        if row is None:
            return MaterializeResult(metadata={"pushed": False, "reason": "no Paper node"})
        node, authors = dict(row["node"]), row["authors"]
        if node.get("zotero_key"):
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": "already in Zotero",
                                               "zotero_key": node["zotero_key"]})

        # ensure_collections() is OUTSIDE push_one's error handling and can raise on a
        # revoked key or sustained throttling. Guard it, or a Zotero outage fails the
        # whole ingest run — which this asset exists specifically not to do.
        client = zotero.get_client()
        try:
            collections = client.ensure_collections()
        except (ZoteroClientError, ZoteroTransientError) as exc:
            context.log.warning(f"Zotero unavailable, skipping push: {exc}")
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": f"Zotero unavailable: {exc}"})

        pdf = fetch_pdf(context.resources.minio.get_client(), key)
        out = zp.push_one(client, collections["papers"], {**node, "kind": "paper"},
                          authors, pdf)

        # Only mark done when the attachment also landed — otherwise the repair query
        # must be able to find this record again.
        if out["complete"] and out["zotero_key"]:
            s.run(zp.MARK_PAPER_PUSHED, id=node["id"], key=out["zotero_key"])

    return _result(out)
```

- [ ] **Step 2: Write the book asset**

```python
"""book_zotero_push: file this book into the user's Zotero library under Alethograph/Books.

Books flow through a separate asset chain with no Semantic Scholar lookup, so this mirrors
zotero_push rather than sharing an asset. The orchestration itself is shared via push_one.
"""
from __future__ import annotations

from dagster import MaterializeResult, asset

from pipeline.assets.zotero_push import _result, fetch_pdf
from pipeline.runtime.partitions import books_partitions_def
from pipeline.zotero import push as zp
from pipeline.zotero.client import ZoteroClientError, ZoteroTransientError


@asset(partitions_def=books_partitions_def(), deps=["book_structure_write"],
       required_resource_keys={"minio", "neo4j_new", "zotero"})
def book_zotero_push(context) -> MaterializeResult:
    key = context.partition_key
    zotero = context.resources.zotero
    if not zotero.configured:
        return MaterializeResult(metadata={"pushed": False,
                                           "reason": "Zotero not configured "
                                                     "(ZOTERO_API_KEY / ZOTERO_USER_ID)"})

    new = context.resources.neo4j_new
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        row = s.run(zp.BOOK_FOR_PUSH, document_id=key).single()
        if row is None:
            return MaterializeResult(metadata={"pushed": False, "reason": "no Book node"})
        node, authors = dict(row["node"]), row["authors"]
        if node.get("zotero_key"):
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": "already in Zotero",
                                               "zotero_key": node["zotero_key"]})

        client = zotero.get_client()
        try:
            collections = client.ensure_collections()
        except (ZoteroClientError, ZoteroTransientError) as exc:
            context.log.warning(f"Zotero unavailable, skipping push: {exc}")
            return MaterializeResult(metadata={"pushed": False,
                                               "reason": f"Zotero unavailable: {exc}"})

        pdf = fetch_pdf(context.resources.minio.get_client(), key)
        out = zp.push_one(client, collections["books"], {**node, "kind": "book"},
                          authors, pdf)

        if out["complete"] and out["zotero_key"]:
            s.run(zp.MARK_BOOK_PUSHED, id=node["id"], key=out["zotero_key"])

    return _result(out)
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

In `pipeline/definitions.py`: add `zotero_push, book_zotero_push` to the `from pipeline.assets import (...)` block; add `zotero_push.zotero_push` and `book_zotero_push.book_zotero_push` to `assets=[...]`; add `ZoteroResource` to the resources import; and add to the resources dict:

```python
        "zotero": ZoteroResource(),
```

- [ ] **Step 5: Verify Dagster still loads, with and without Zotero configured**

The method is `get_all_asset_keys()`, not `all_asset_keys`, and `load_dotenv()` is required first because `definitions.py` calls `new_neo4j_from_env()` at import time (which raises `KeyError: 'NEO4J_NEW_URI'` without it):

```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from pipeline.definitions import defs
print(len(defs.get_asset_graph().get_all_asset_keys()), 'assets')"
```
Expected: `20 assets` (measured at 18 before this task).

Run: `uv run pytest tests/test_definitions.py -v`
Expected: PASS

- [ ] **Step 6: Write the integration test**

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
    key = client.create_items([paper_item(paper, ["Test Author"], collections["papers"])])[0]
    try:
        fetched = client.get_item(key)
        assert fetched["data"]["itemType"] == "preprint"
        assert collections["papers"] in fetched["data"]["collections"]
        assert client.has_attachment(key) is False

        att_key = client.create_items([attachment_item(key, "test.pdf")])[0]
        assert client.upload_attachment(att_key, "test.pdf", b"%PDF-1.4\n%%EOF\n") in (
            "uploaded", "exists")
        assert client.has_attachment(key) is True
    finally:
        client.request("DELETE", f"/items/{key}",
                       headers={"If-Unmodified-Since-Version":
                                str(client.get_item(key)["data"]["version"])})
```

- [ ] **Step 7: Run the unit suite, then the integration test**

Run: `uv run pytest tests -q --ignore=tests/server/test_app.py; uv run ruff check pipeline server scripts tests`
Expected: PASS, integration tests skipped.

Run: `uv run pytest tests/integration/test_zotero_integration.py -v --run-integration`
Expected: PASS. Afterwards, check the real Zotero library: an `Alethograph` collection with `Papers` and `Books` subcollections should exist, and the test item should be gone.

- [ ] **Step 8: Commit**

```bash
git add pipeline/assets/zotero_push.py pipeline/assets/book_zotero_push.py \
        pipeline/runtime/jobs.py pipeline/definitions.py \
        tests/integration/test_zotero_integration.py
git commit -m "feat: zotero_push and book_zotero_push assets wired into both ingest jobs"
```

---

## Task 13: Backfill the existing corpus into Zotero

**Files:**
- Create: `scripts/backfill_zotero.py`
- Test: manual dry run

**Interfaces:**
- Consumes: `push_one` and the Cypher constants (Task 11), `ZoteroClient` (Tasks 9-10), `minio_from_env` (`pipeline/runtime/resources.py:87`).

**Context:** ~174 items and ~1.28 GB of uploads; attachment upload dominates. Resumable for free via `zotero_key`. The library index is fetched **once** and reused, rather than 174 searches.

**[REV] Three corrections:**
1. **The sequencing hazard is enforced in code, not prose.** Running this before `backfill_venue.py` files the entire corpus as preprints, and the `zotero_key` guard prevents a re-run from correcting it. The script now counts enrichable-but-unenriched papers and refuses above a threshold unless `--skip-venue-check` is passed.
2. **Client errors are caught per record**, so one malformed item cannot abort a long upload run — the spec promised a failure list, and the original draft had no `try`.
3. **Reuses `minio_from_env()`** instead of hand-building a boto3 client.

- [ ] **Step 1: Write the script**

```python
"""One-off backfill: push every ingested paper and book into Zotero under Alethograph.

Resumable — records already carrying a zotero_key are skipped, so an interrupted run can
simply be re-run. Fetches the library index once and reuses it for deduplication across
every record rather than issuing one search per item.

PREREQUISITE: run scripts/backfill_venue.py (without --dry-run) first. Without venue data
every existing paper files as a preprint, and the zotero_key guard prevents a re-run from
correcting it. This script checks for that and refuses rather than trusting the operator.

Run: uv run python scripts/backfill_zotero.py [--apply] [--limit N] [--skip-venue-check]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import botocore.exceptions
from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipeline.runtime.resources import minio_from_env
from pipeline.runtime.storage import RAW_BUCKET
from pipeline.zotero import push as zp
from pipeline.zotero.client import ZoteroClient, ZoteroClientError

# Papers that COULD be enriched but have not been. A non-trivial count means
# backfill_venue.py has not run, and pushing now would file them all as preprints.
UNENRICHED = """
MATCH (p:Paper)
WHERE p.venue IS NULL AND p.journal_name IS NULL
  AND (p.arxiv_id IS NOT NULL OR p.doi IS NOT NULL)
RETURN count(p) AS n
"""
UNENRICHED_THRESHOLD = 10


def fetch_pdf(s3, key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=RAW_BUCKET, Key=f"{key}.pdf")["Body"].read()
    except botocore.exceptions.ClientError:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write to Zotero (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="process at most N records")
    ap.add_argument("--skip-venue-check", action="store_true",
                    help="proceed even if venue enrichment looks incomplete")
    args = ap.parse_args()

    load_dotenv()
    client = ZoteroClient(api_key=os.environ["ZOTERO_API_KEY"],
                          user_id=os.environ["ZOTERO_USER_ID"])
    s3 = minio_from_env().get_client()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    database = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")

    created = matched = deferred = failed = no_pdf = 0
    with driver, driver.session(database=database) as s:
        unenriched = s.run(UNENRICHED).single()["n"]
        if unenriched > UNENRICHED_THRESHOLD and not args.skip_venue_check:
            print(f"REFUSING: {unenriched} papers have an identifier but no venue data.\n"
                  f"Run `uv run python scripts/backfill_venue.py` first, or pass\n"
                  f"--skip-venue-check to proceed anyway. Pushing now would file these as\n"
                  f"preprints permanently — the zotero_key guard stops a re-run fixing it.")
            return 1

        collections = client.ensure_collections()
        print(f"collections: Papers={collections['papers']} Books={collections['books']}")
        print("fetching library index for deduplication...")
        index = client.library_index()
        print(f"  {len(index)} existing items indexed")

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

            # Fetch AFTER the dry-run guard: pulling every PDF just to print DRY lines
            # would download the whole ~1.28 GB corpus out of MinIO for nothing.
            if not args.apply:
                print(f"  DRY      {kind:5} {node.get('title')!r}")
                continue

            pdf = fetch_pdf(s3, ref["document_id"])
            if pdf is None:
                no_pdf += 1

            try:
                out = zp.push_one(
                    client, collections["papers" if kind == "paper" else "books"],
                    {**node, "kind": kind}, authors, pdf, candidates=index)
            except ZoteroClientError as exc:
                # One malformed record must not abort a 174-item upload run.
                failed += 1
                print(f"  FAILED   {node.get('title')!r}  ({exc})")
                continue

            if out["complete"] and out["zotero_key"]:
                s.run(zp.MARK_PAPER_PUSHED if kind == "paper" else zp.MARK_BOOK_PUSHED,
                      id=node["id"], key=out["zotero_key"])

            if not out["complete"]:
                deferred += 1
                print(f"  DEFERRED {node.get('title')!r}  "
                      f"({out['reason'] or out['attachment']})")
            elif out["outcome"] == "created":
                created += 1
                print(f"  CREATED  {out['item_type']:16} {out['filename']}")
            else:
                matched += 1
                print(f"  MATCHED  {out['zotero_key']}  {node.get('title')!r} "
                      f"[{out['attachment']}]")

    print(f"\ncreated: {created}   matched existing: {matched}   "
          f"deferred (retry later): {deferred}   failed: {failed}   missing PDF: {no_pdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Lint and dry-run**

Run: `uv run ruff check scripts/backfill_zotero.py && uv run python scripts/backfill_zotero.py`
Expected: no lint errors. Before Task 5's backfill has been run for real, this should print `REFUSING:` and exit 1 — that is the guard working. After it has run, expect the collections line, an index count, then ~174 `DRY` lines.

- [ ] **Step 3: Smoke-test a single record against the real library**

Run: `uv run python scripts/backfill_zotero.py --apply --limit 1`
Expected: one `CREATED` line. Open Zotero and confirm the item appears under `Alethograph/Papers` with a correctly-named PDF attachment that opens.

- [ ] **Step 4: Commit**

```bash
git add scripts/backfill_zotero.py
git commit -m "feat: add backfill_zotero script with a venue-enrichment ordering guard"
```

---

## Sequencing after implementation

Operational runs, not code changes. Order matters, and Task 13's guard enforces it.

- [ ] `uv run python scripts/backfill_venue.py --dry-run`
- [ ] `uv run python scripts/backfill_venue.py` — enriches ~124 papers.
- [ ] Spot-check: `MATCH (p:Paper) WHERE p.venue IS NOT NULL RETURN count(p)` should be well above zero, and the Journal of Finance paper (DOI `10.1111/jofi.13188`) should now carry `venue` and `publication_types`.
- [ ] `uv run python scripts/backfill_zotero.py` — dry run; must not print `REFUSING:`.
- [ ] `uv run python scripts/backfill_zotero.py --apply --limit 1` — verify one item opens in the Zotero UI.
- [ ] `uv run python scripts/backfill_zotero.py --apply` — full run, ~1.28 GB of uploads. Re-run once afterwards to pick up anything reported `DEFERRED`.

## Dropped work

**The paper-id migration (originally Task 6) has been removed.** Its rationale — "relationships are attached to the node, not the id string, so only the scalar property changes" — was verified false. `Paper.id` is a derivation input for four other identifiers:

- `graph_write.py:23-28` — `Definition.id` and `Result.id` are `f"{paper_id}:def:{hash}"` / `f"{paper_id}:{kind}:{hash}"`
- `paper_analysis.py:12` — `Summary.id` **is** the paper id
- `graph_write.py:143` — `Document.paper_id` is stored as a property
- the MinIO `triage/{doc}.json` payload, read back at `graph_write.py:277` and `paper_analysis.py:33`

Relabelling would leave every derived id stale, so re-materializing `graph_write` alone would silently write nothing (its `MATCH (p:Paper {id:$paper_id})` matching no node), and a full re-ingest would mint duplicate `Definition`/`Result`/`Summary` nodes — making the very re-ingest scenario the migration claimed to fix strictly worse. The payoff was 3-4 papers with junk template DOIs, and the failure it guarded against is not automatically reachable, since `schedules.py:12-18` only registers *new* content-hash partitions.

Fixing those ids properly is its own project and would have to rewrite the triage JSON, `Document.paper_id`, the derived ids, and the Postgres `pending_citations.citing_paper_id` rows together.

## Known limitations

Accepted for this iteration; each is a small follow-up if it becomes a problem.

- **No update path.** Once `zotero_key` is set, nothing rewrites the Zotero item. A paper that is a preprint today and published next year keeps its `preprint` entry. Adding this means an update branch in `push_one` (PATCH bibliographic fields on items we created, identifiable by `zotero_key`) plus a `--refresh` mode on both backfills.
- **Venue is fetched once.** `backfill_venue.py`'s `NEEDS_VENUE` selects only `venue IS NULL`, so preprint-to-published transitions are never picked up. A `--refresh-preprints` mode selecting papers with no `publication_types` would pair with the above.
- **Deletion is not tracked.** Deleting an item in Zotero leaves `zotero_key` set, so it is never re-pushed. Wiping a document from the graph orphans its Zotero item and storage.
- **Manual reorganization is respected, deliberately.** `zotero_key` short-circuits before any collection write, so moving an item out of `Alethograph` in Zotero sticks. This is the intended behaviour, not an accident.
- **The per-ingest and backfill dedup pools differ.** The asset searches by title (25 results); the backfill matches against the whole library index. The backfill's net is strictly wider, so the two can reach different conclusions for the same record. Post-backfill the library is ~2000 items — roughly 20 paginated requests — so switching the asset to `library_index()` is cheap if duplicates ever appear.
- **`ensure_collections()` runs once per partition**, costing one extra `GET /collections` per ingest. Harmless; cacheable on the resource if it ever matters.

## Self-review notes

**Spec coverage.** Spec §A1→Task 4; §A2→Task 4; §A3→Task 3; §A4→Task 4; §A5→Task 1 (migration dropped, see above); §A6→Task 2; §A7→Task 5; §A8→Task 6; §A9→Tasks 1-4; §B1→Tasks 7-12; §B2→Task 9; §B3→Task 9; §B4→Tasks 8 and 11; §B5→Task 8; §B6→Task 7; §B7→Task 10; §B8→Tasks 11-12; §B9→Tasks 9-12; §B10→Task 13; §B11→Tasks 7-12.

**Deviations from the spec, all deliberate:**
- The spec sketched `_PLACEHOLDER_DOI` as a blocklist; Task 1 adds a structural shape check and makes the run-detection case-sensitive, after testing showed the drafted pattern rejected legitimate DOIs.
- The spec said "do not attach a second PDF if the item already has one"; Task 11 implements exactly that, including attaching to a matched item that has none — which the first draft of this plan got wrong.
- The spec's §A5 migration note is not implemented — see "Dropped work".
- The spec's §B9 classed all 4xx as fatal; Task 11 exempts 413 (quota) and Task 9 treats 409 (locked) as transient.
