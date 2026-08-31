# Zotero Library Sync — Design

**Date:** 2026-08-31
**Status:** Draft — awaiting Osian's review
**Scope decision:** Two phases in one project. Phase A enriches the graph with publication-venue data at ingest time; Phase B pushes papers and books into Zotero, consuming what Phase A stores. A must land and backfill before B backfills, or the existing 150-paper corpus lands in Zotero typed as preprints regardless of where it was actually published.

## Motivation

The pipeline has ingested roughly 150 papers and 20 books (~1.28 GB of PDFs). They are reachable through the graph and the alethograph website, but there is no ordinary way to *browse* them or open a PDF the way a reference manager does. Osian already uses Zotero daily, so the goal is that every ingested document appears there automatically, filed sensibly, with no manual step ever.

Two things blocked that:

1. **No venue data anywhere in the graph.** Zotero distinguishes `journalArticle`, `conferencePaper`, and `preprint`, and fills different fields for each. A live audit of the graph (2026-08-31) found no venue, journal, or publication-type property on any `Paper` node — only `title`, `year`, `doi`, `arxiv_id`, `s2_id`, `abstract`, `tldr`, and citation counts. Mapping everything to `preprint` would be lossy for the papers that were genuinely published.
2. **Storage.** Zotero's free tier is 300 MB, below the corpus size. Resolved 2026-08-31: Osian purchased a paid storage tier. A self-hosted WebDAV backend on a Synology NAS is planned for later (deferred pending a house move; scoped in a separate memo) and is a Zotero-side settings change that this design is deliberately unaffected by.

### Corpus audit (live Cypher, 2026-08-31)

| Property | Papers with it | of 150 |
|---|---|---|
| `title` | 150 | 100% |
| `year` | 145 | 97% |
| `arxiv_id` | 108 | 72% |
| `s2_id` | 65 | 43% |
| `doi` | 28 | 19% |
| `abstract` | 20 | 13% |

Two findings shape the design:

- **26 papers carry no identifier at all** (no DOI, no arXiv ID). They can only ever be deduplicated by normalized title, and they cannot be enriched from Semantic Scholar, whose lookups are identifier-only.
- **The DOI field is not clean.** Of 28 DOIs, 5 are arXiv's own (`10.48550/arXiv.*`), 2 are SSRN preprint DOIs (`10.2139/ssrn.*`), and 3 are unfilled LaTeX template placeholders that were extracted verbatim: `10.1145/NNNNNNN.NNNNNNN`, `10.1017/S09624929XXXXXXXX`, and `http://dx.doi.org/10.1145/0000000.0000000`. Roughly 18 papers have a genuine publisher DOI. Placeholder DOIs must be treated as absent, not looked up.

### Why Semantic Scholar, and not Google Scholar or Crossref

Google Scholar has no official API, blocks automated access, and scraping it would breach its terms. It is not a viable source.

Crossref was the obvious second candidate, but it turned out to be unnecessary: **the pipeline already requests `venue` from Semantic Scholar and then discards it.** `pipeline/graph/research_port.py:12` includes `venue` in the `FIELDS` query string, and `_paper_json_to_record` maps it at line 40, but `triage_metadata.py:62-70` builds the Neo4j write payload without that key, so it never reaches the graph. `venue` occurs exactly three times in the repository and never in a write path.

S2 also exposes `publicationTypes` (which distinguishes `JournalArticle` from `Conference` directly, precisely the signal the Zotero mapping needs) and a `journal` object carrying name, volume, and pages. Both are simply absent from the requested field list. S2 lookups work by arXiv ID as well as DOI, so enrichment reaches all 108 arXiv papers rather than only the 18 with real DOIs.

The one field S2 lacks is `issue`. Crossref would supply it, at the cost of a genuinely new integration for a field that does not affect browsing or item typing. Out of scope: YAGNI.

---

## Phase A — Venue enrichment

### A1. New `Paper` properties

Neo4j has no nested-map property type, so S2's `journal` object is flattened rather than stored whole.

| Property | Type | Source |
|---|---|---|
| `venue` | string | S2 `venue` (flat display string, e.g. "Journal of Finance") |
| `journal_name` | string | S2 `journal.name` |
| `volume` | string | S2 `journal.volume` |
| `pages` | string | S2 `journal.pages` |
| `publication_types` | list of string | S2 `publicationTypes`, stored as a Neo4j array |

`publication_types` is a list in the S2 response and is stored as a Neo4j array rather than a joined string, so Zotero mapping can test membership directly. Neo4j supports homogeneous string arrays as scalar properties; no schema change is required in `pipeline/graph/schema.py`, which enumerates node and relationship types but not scalar properties.

### A2. `research_port.py` changes

Extend the field list at line 12:

```python
FIELDS = ("paperId,title,abstract,year,venue,externalIds,citationCount,"
          "influentialCitationCount,tldr,authors,publicationTypes,journal")
```

`publicationVenue` is deliberately not requested. It returns a nested venue object that duplicates what `venue` and `journal` already give, and adds a second nested structure to flatten for no gain.

Extend `_paper_json_to_record` (lines 36-47), guarding the nested `journal` object the same way the existing code guards `tldr`:

```python
    journal = j.get("journal") or {}
    ...
        "venue": j.get("venue"),
        "journal_name": journal.get("name"),
        "volume": journal.get("volume"),
        "pages": journal.get("pages"),
        "publication_types": j.get("publicationTypes") or [],
```

### A3. `WRITE_PAPER` — null-overwrite fix

The existing `SET` clause is unconditional, so re-ingesting a document whose S2 lookup returns nothing (a 429, a timeout, or a genuine absence) **overwrites good stored metadata with nulls**. This is a live bug independent of venue work, and it becomes considerably worse once more fields depend on a flaky external call.

Fix using the `coalesce` idiom already present in the same query for `a.s2_author_id` (line 101). Applied to the new fields and to the existing enrichment-derived ones (`abstract`, `tldr`, `citation_count`, `influential_citation_count`), which have the same failure mode:

```cypher
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
    p.venue = coalesce($venue, p.venue),
    p.journal_name = coalesce($journal_name, p.journal_name),
    p.volume = coalesce($volume, p.volume),
    p.pages = coalesce($pages, p.pages),
    p.publication_types = CASE WHEN size($publication_types) > 0
                               THEN $publication_types ELSE p.publication_types END,
    p.document_id = $document_id
```

`document_id` stays an unconditional `SET`: it is derived from the partition key, never from S2, and is never null.

`publication_types` needs the `CASE` rather than `coalesce` because the record mapper emits `[]` (not `None`) when S2 omits the field, and `coalesce` would treat the empty list as a present value.

### A4. `triage_metadata.py` changes

The write executes as `s.run(rp.WRITE_PAPER, **paper)` (line 81), so the dict keys must match the Cypher parameters exactly or the query errors on a missing parameter. Add the five new keys to the `paper` dict at lines 62-70:

```python
        "venue": rec.get("venue"),
        "journal_name": rec.get("journal_name"),
        "volume": rec.get("volume"),
        "pages": rec.get("pages"),
        "publication_types": rec.get("publication_types") or [],
```

### A5. Placeholder-DOI rejection

Add to `research_port.py` a predicate that treats template placeholders as absent, applied in `triage_metadata.py` before the DOI is used for lookup or for `compute_paper_id`:

```python
_PLACEHOLDER_DOI = re.compile(r"(NNNNNNN|X{4,}|0{6,}|/$)", re.IGNORECASE)

def clean_doi(doi: str | None) -> str | None:
    """Strip URL prefixes; return None for unfilled LaTeX template DOIs."""
    if not doi:
        return None
    d = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi.strip(), flags=re.IGNORECASE)
    return None if not d or _PLACEHOLDER_DOI.search(d) else d
```

This also normalizes `http://dx.doi.org/…` prefixes, which the current code stores verbatim, breaking both S2 lookup and `compute_paper_id`'s `"doi:" + doi.lower()` identity.

**Migration note:** `compute_paper_id` derives node identity from the DOI, so cleaning a DOI changes the computed id for the three placeholder-DOI papers and any URL-prefixed one. The backfill (A7) must match those nodes by `document_id`, which is stable, and must not create duplicates. Nodes whose id changes are relabelled in place by the backfill rather than re-created; the backfill logs every id change.

### A6. Retry and failure distinction

`lookup_by_arxiv` and `lookup_by_doi` currently fold a 404, a 429, and a timeout into an identical `None` (lines 50-63), so on S2's unauthenticated tier a throttled call is indistinguishable from a genuinely missing paper. A retry helper written for exactly this problem already exists in `scripts/backfill_citations.py:58-66` but was never promoted into the pipeline path.

Move `_with_retry` into `research_port.py` as a shared public helper, have `backfill_citations.py` import it rather than define its own, and apply it to both lookups. Retry on 429 and 5xx with exponential backoff (3 attempts, 5s base); do not retry a 404. On exhaustion, return `None` as today but log at warning level with the distinguishing status code, so a rate-limited run is visible in the Dagster logs instead of silently producing metadata-poor nodes.

### A7. Backfill

`scripts/backfill_venue.py`, modelled on the existing `scripts/backfill_citations.py`:

1. Query papers missing venue data and holding a usable identifier: `MATCH (p:Paper) WHERE p.venue IS NULL AND (p.arxiv_id IS NOT NULL OR p.doi IS NOT NULL) RETURN p.id, p.document_id, p.arxiv_id, p.doi`.
2. Re-run the S2 lookup per paper through the retried helpers, arXiv ID first then DOI, matching triage's precedence.
3. Write via the same `WRITE_PAPER` query so the `coalesce` semantics are identical and the script cannot diverge from the asset.
4. Apply `clean_doi` and relabel any node whose id changes, logging each change.
5. Report a summary: enriched, no-S2-record, rate-limited, unidentifiable.

Expected reach: roughly 124 of 150 papers have an identifier; the 26 without one stay unenriched by design and will map to `preprint` in Phase B.

### A8. Server-layer visibility

New properties are invisible to the MCP and query layer until two hardcoded lists are updated:

- `server/queries.py:63` — `render_schema()` enumerates the property list shown to the query-generating LLM.
- `server/queries.py:255-256` — `GET_PAPER`'s projection enumerates returned properties explicitly.

Both take the five new properties. Without this, `/kg:ask` cannot see venue data even though it is in the graph.

### A9. Tests (Phase A)

Extend `tests/test_triage.py` (or create it if absent; the suite has no triage test today):

- `_paper_json_to_record` maps a full S2 response including nested `journal` into flat keys.
- `_paper_json_to_record` survives S2 responses with `journal: null`, absent `publicationTypes`, and absent `venue`.
- `clean_doi` returns `None` for each of the three real placeholder DOIs found in the corpus, strips a `http://dx.doi.org/` prefix, and passes a genuine DOI through unchanged.
- **Null-overwrite regression:** write a paper with full metadata, then write the same paper id with an all-null enrichment record, and assert the stored `venue`/`abstract`/`citation_count` survive. This is the test that pins A3.
- `_with_retry` retries a 429 and does not retry a 404.

---

## Phase B — Zotero push

### B1. Shape

A new Dagster asset, `zotero_push`, partitioned identically to the rest of the document pipeline (by document id) and appended to the `ingest_document` job after `paper_analysis`. A parallel `book_zotero_push` appends to the book path, since books flow through an entirely separate asset chain with no S2 lookup.

Both delegate to one shared client module, `pipeline/zotero/client.py`, so item construction, deduplication, and upload logic exist once. `requests` is already a project dependency; `pyzotero` is not, and is not worth adding for the handful of endpoints in use, consistent with `research_port.py` hand-rolling its S2 calls.

### B2. Credentials

A `ZoteroResource` in `pipeline/runtime/resources.py`, following the existing `ConfigurableResource` pattern:

```python
class ZoteroResource(ConfigurableResource):
    """Zotero Web API v3 — personal library sync for ingested papers and books."""
    api_key: str = Field(default_factory=lambda: os.environ.get("ZOTERO_API_KEY", ""))
    user_id: str = Field(default_factory=lambda: os.environ.get("ZOTERO_USER_ID", ""))
    base_url: str = "https://api.zotero.org"
    request_timeout: float = 60.0
```

New `.env` keys `ZOTERO_API_KEY` and `ZOTERO_USER_ID`. The key needs write access to the personal library. It is read from the environment only and must never be logged, echoed into asset metadata, or committed.

### B3. Collection structure

One top-level `Alethograph` collection with `Papers` and `Books` subcollections, created idempotently on first run: list collections, match by name and parent, create only what is missing (`POST /users/<id>/collections`). Collection keys are resolved once per run and cached in memory.

Osian's library already contains 45 collections. Nothing outside `Alethograph` is created, renamed, or deleted. Existing items found by deduplication are *added to* the Alethograph collection, never moved out of the collections they already belong to, because Zotero collection membership is a set rather than a location.

### B4. Deduplication

Confirmed with Osian 2026-08-31: **when an item already exists, add it to the Alethograph collection rather than creating a second copy.**

Match precedence, first hit wins:

1. `DOI` field equals the paper's cleaned DOI (skipped when the DOI is absent or a placeholder)
2. `archiveID` or `url` contains the arXiv ID
3. Normalized title exact match, using the same `normalize_title` the graph uses for identity, so the two systems agree on what "same title" means

On a hit: `PATCH` the existing item's `collections` array to include the Alethograph subcollection key, and leave every other field untouched. Do not overwrite the user's own metadata or notes, and do not attach a second PDF if the item already has one.

On a miss: create the item (B5) and upload the PDF (B7).

The 26 identifier-less papers fall through to title matching only. This is accepted: a title collision between an ingested paper and a manually added one is exactly the case where merging is correct anyway.

### B5. Item type mapping

Driven by Phase A's `publication_types`, falling back to identifier shape:

| Condition | Zotero `itemType` | Fields populated beyond title/creators/date |
|---|---|---|
| `publication_types` contains `Conference` | `conferencePaper` | `proceedingsTitle` ← `venue`, `DOI`, `pages` |
| `publication_types` contains `JournalArticle`, or a genuine publisher DOI exists | `journalArticle` | `publicationTitle` ← `journal_name` or `venue`, `volume`, `pages`, `DOI` |
| `arxiv_id` present, no publisher DOI | `preprint` | `repository` = "arXiv", `archiveID` = `arXiv:<id>`, `url` |
| none of the above | `preprint` | title, creators, date only |
| Book node | `book` | `publisher`, `edition` (both already on the Book node) |

`Conference` is tested before `JournalArticle` because S2 sometimes returns both for papers published in proceedings that a journal later indexed, and the proceedings is the more specific truth.

Abstract goes to `abstractNote` where present. `tldr` is deliberately not written to any Zotero field: it is a generated summary, not bibliographic data, and putting it in `extra` would clutter every item.

Creators come from the graph's `(:Author)-[:AUTHORED]->(:Paper)` edges, split into Zotero's structured `firstName`/`lastName` on the last whitespace, with a single-token name stored as `name` in Zotero's single-field mode rather than guessed at.

### B6. Attachment filename

Confirmed with Osian: `Title - Author(s) - Year.pdf`.

- **Authors:** one author gives the surname; two give `Surname and Surname`; three or more give `Surname et al.`
- **Sanitization:** replace `/` and `:` and other path-hostile characters with `-`, collapse runs of whitespace, strip leading and trailing dots
- **Length:** truncate the title component so the whole filename stays under 200 bytes, leaving headroom under the common 255-byte filesystem limit for the eventual WebDAV/NAS backend
- **Missing parts:** omit the segment and its separator rather than emitting `Unknown`, so a year-less paper becomes `Title - Author.pdf`

This is the attachment's `filename` and `title`. It does not affect the parent item's own title field, which stays the real title.

### B7. PDF upload

PDFs come from MinIO's raw bucket, keyed by the document's content hash, which is the pipeline's canonical store. MinIO is bound to `localhost:9000` and is not reachable from Zotero's servers, so a genuine by-reference attachment is impossible; the file is uploaded and Zotero stores its own copy. This is what the paid storage tier is for.

Upload follows Zotero's documented three-step protocol:

1. **Authorize** — `POST /users/<id>/items/<itemKey>/file` with `md5`, `filename`, `filesize`, `mtime` (milliseconds, not seconds), and header `If-None-Match: *` for a new attachment.
2. **Upload** — if the response carries upload parameters, `POST` the concatenation of `prefix` + file bytes + `suffix` to the returned URL with the returned `contentType`. If the response is `{"exists": 1}`, Zotero already holds a file with that hash and has associated it with the item; skip to done.
3. **Register** — `POST /users/<id>/items/<itemKey>/file` with `upload=<uploadKey>` and the same conditional header. A `204` confirms.

The attachment is an `imported_file` child item with `parentItem` set to the created item's key. `exists: 1` gives free server-side deduplication when the same PDF reaches Zotero twice.

### B8. Idempotency and repair

On success the asset writes the Zotero item key back to the graph: `SET p.zotero_key = $key`. This does triple duty:

- **Idempotency** — a re-materialized partition sees `zotero_key` already set and skips the push
- **Repair query** — `MATCH (p:Paper) WHERE p.zotero_key IS NULL` enumerates everything that has not landed, which is exactly what the backfill script consumes
- **Diagnosis** — a populated key links a graph node to its Zotero item directly

`zotero_key` is added to the `server/queries.py` lists alongside the Phase A properties.

### B9. Error handling

Confirmed policy: **a Zotero failure must not fail the ingest run.** The graph write is the pipeline's actual job and has already succeeded by the time this asset runs; losing a whole run because Zotero returned a 503 would be backwards.

- **Transient** (network error, timeout, 429, 5xx): retry with the shared `_with_retry` backoff. On exhaustion, emit `MaterializeResult(metadata={"pushed": False, "reason": ...})` and return without raising. `zotero_key` stays null, so the repair query picks it up on the next backfill run.
- **Client error** (400, 403, 404, malformed payload): raise. These indicate a code defect or a revoked API key, not a blip, and should be visible immediately rather than accumulating silently.
- **Rate limits:** Zotero returns `Backoff` and `Retry-After` headers. Honour both; do not use a fixed sleep.

Every path emits asset metadata recording whether the push happened, which item type was chosen, and whether the item was created or matched to an existing one.

### B10. Backfill

`scripts/backfill_zotero.py`, run once after Phase A's backfill completes:

1. Enumerate `MATCH (p:Paper) WHERE p.zotero_key IS NULL` and the `Book` equivalent
2. Push each through the same client module the asset uses, so behaviour cannot diverge
3. Batch item creation up to Zotero's documented limit of 50 per request; attachments upload one at a time, since each is a three-step protocol
4. Report created / matched-existing / failed counts, and list failures with reasons

Scale: roughly 174 items and ~1.28 GB of uploads. Attachment upload dominates the runtime. The script must be resumable, which `zotero_key` gives for free: re-running skips everything already pushed.

### B11. Tests (Phase B)

Unit tests against a mocked Zotero API, following the suite's existing mocking style:

- Item construction produces the right `itemType` for each row of B5's table, including the `Conference`-before-`JournalArticle` precedence
- Filename formatting: one, two, and three-plus authors; a title containing `/` and `:`; a missing year; an over-long title truncating below 200 bytes
- Deduplication precedence: DOI hit beats arXiv hit beats title hit; a placeholder DOI is not used for matching
- A dedup hit issues a `PATCH` adding the collection, and never a `POST` creating an item
- `exists: 1` short-circuits the upload without a second POST
- A 429 during push emits `pushed: False` and does not raise; a 400 raises
- Collection bootstrap is idempotent: a second run creates nothing

One integration test behind the existing `--run-integration` gate, pushing a single fixture PDF into the real library and cleaning up after itself.

---

## Sequencing

1. **A1-A6** — enrichment plus the null-overwrite and retry fixes, with tests
2. **A7** — run `backfill_venue.py` over the existing 150 papers
3. **A8** — server-layer property lists
4. **B1-B9** — the Zotero client, resource, and both assets, with tests
5. **B10** — run `backfill_zotero.py` over the full corpus

Step 2 must complete before step 5. Running them out of order files the entire existing corpus in Zotero as preprints, and the `zotero_key` idempotency guard means a second run will not correct it without an explicit re-push.

## Out of scope

- **Crossref integration** for the `issue` field (§Why Semantic Scholar)
- **WebDAV storage backend** — a later Zotero-side settings change once the NAS exists; nothing in this design depends on which storage backend Zotero uses
- **Topic-based collection hierarchy** — `Topic` nodes exist in the schema vocabulary but hold zero data (verified by live Cypher), so there is nothing to organize by yet. Revisit if topic extraction is ever populated.
- **Syncing Zotero changes back into the graph.** This is a one-way push. Edits made in Zotero stay in Zotero.
- **Retiring `~/alethograph-explorer`** and its vault-write hook, tracked separately.

## Open questions

None blocking. Two items to confirm during implementation:

- Whether S2's `publicationTypes` is populated densely enough across the corpus to carry the mapping, or whether the DOI fallback ends up doing most of the work. Measurable after A7's backfill, and it changes nothing structurally either way.
- Whether Zotero's 50-item batch creation interacts badly with per-item attachment upload at backfill scale. If it does, fall back to one-at-a-time creation, which costs runtime but nothing else.
