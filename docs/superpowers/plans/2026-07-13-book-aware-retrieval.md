# Book-Aware Retrieval Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Book content (Williams: 118 chunks, 188 results, 57 definitions, 261 notations) flows through the typed MCP retrieval tools with (book, chapter, section) citations, instead of being reachable only via hand-written `run_cypher`.

**Architecture:** Papers and books share the `(Chunk)-[:BELONGS_TO]->(Document)` hop; the retrieval queries currently hard-require the document's owner to be `(:Paper)`, which silently excludes every book chunk and book-stated statement. Each query gains a source union (`src:Paper OR src:Book`), keeps the existing `paper_id`/`paper_title` output field names for client compatibility, and adds `source_type` / `chapter` / `section` fields (null for papers). No new tools; no server API changes.

**Tech Stack:** Neo4j 5 Cypher (read-only), FastMCP server in `server/`, pytest.

## Global Constraints

- Line length 100, Black formatting, Ruff clean: `uv run ruff check server/ tests/`
- All queries stay read-only; never touch `check_read_only` or the guard regexes
- **Do not rename existing output fields** — `paper_id`, `paper_title`, `year`, `position` are parsed by `scripts/run_eval.py`, the ask skill, and MCP clients. Book rows populate them with the book's id/title; new fields are additive (`source_type`, `chapter`, `section`)
- The `$paper_id` filter parameter keeps its name and now also matches a book id (documented in docstrings, not renamed)
- Unit tests assert query text and shaping (house style in `tests/server/test_queries.py`); live behavior is verified by `tests/server/test_integration_server.py` (skips without `--run-integration` + `KG_NEO4J_*` env)
- Run tests with: `uv run --extra dev --extra server pytest <file> -q`
- Graph shape reference (from `pipeline/graph/schema.py` PATTERNS):
  `(Book)-[:HAS_DOCUMENT]->(Document)`, `(Chunk)-[:BELONGS_TO]->(Document)`,
  `(Chunk)-[:PART_OF]->(Section)`, `(Book)-[:HAS_CHAPTER]->(Chapter)-[:HAS_SECTION]->(Section)`,
  `(Section)-[:STATES]->(Definition|Result)`, `(Book)-[:COVERS]->(Concept)`,
  `(Result)-[:HAS_PROOF]->(Proof)`, `(Notation)-[:INTRODUCED_IN]->(Section)`

---

### Task 1: Book-aware chunk search (FULLTEXT_SEARCH + VECTOR_SEARCH)

**Files:**
- Modify: `server/queries.py:148-167` (the two search query constants)
- Test: `tests/server/test_queries.py` (append)
- Test: `tests/server/test_integration_server.py` (append)

**Interfaces:**
- Consumes: nothing new
- Produces: chunk rows now carry `source_type: 'paper'|'book'`, `chapter: str|None`, `section: str|None` in addition to the existing `chunk_id`, `text`, `position`, `score`, `paper_id`, `paper_title`, `year`. Tasks 2 and 6 rely on book ids appearing in `paper_id`.

- [ ] **Step 1: Write the failing unit test**

Append to `tests/server/test_queries.py`:

```python
def test_chunk_search_queries_include_book_sources():
    """Both search paths must accept Paper OR Book document owners and emit
    section/chapter citation fields (null for papers)."""
    for query in (q.FULLTEXT_SEARCH, q.VECTOR_SEARCH):
        assert "src:Paper OR src:Book" in query
        assert "PART_OF" in query          # section hop for book chunks
        assert "AS source_type" in query
        assert "AS chapter" in query
        assert "AS section" in query
        # compat: legacy field names survive
        assert "AS paper_id" in query and "AS paper_title" in query
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev --extra server pytest tests/server/test_queries.py::test_chunk_search_queries_include_book_sources -q`
Expected: FAIL with `AssertionError` (queries still say `(p:Paper)`)

- [ ] **Step 3: Replace the two query constants**

In `server/queries.py`, replace `FULLTEXT_SEARCH` and `VECTOR_SEARCH` entirely:

```python
FULLTEXT_SEARCH = """
CALL db.index.fulltext.queryNodes('chunk_text', $q) YIELD node, score
MATCH (node)-[:BELONGS_TO]->(:Document)<-[:HAS_DOCUMENT]-(src)
WHERE (src:Paper OR src:Book) AND ($paper_id IS NULL OR src.id = $paper_id)
OPTIONAL MATCH (node)-[:PART_OF]->(sec:Section)<-[:HAS_SECTION]-(chp:Chapter)
RETURN node.id AS chunk_id, node.text AS text, node.position AS position, score,
       src.id AS paper_id, src.title AS paper_title, src.year AS year,
       CASE WHEN src:Book THEN 'book' ELSE 'paper' END AS source_type,
       chp.title AS chapter, sec.title AS section
ORDER BY score DESC
LIMIT $top_k
"""

VECTOR_SEARCH = """
CALL db.index.vector.queryNodes('chunk_embedding', $k, $embedding)
YIELD node, score
MATCH (node)-[:BELONGS_TO]->(:Document)<-[:HAS_DOCUMENT]-(src)
WHERE (src:Paper OR src:Book) AND ($paper_id IS NULL OR src.id = $paper_id)
OPTIONAL MATCH (node)-[:PART_OF]->(sec:Section)<-[:HAS_SECTION]-(chp:Chapter)
RETURN node.id AS chunk_id, node.text AS text, node.position AS position, score,
       src.id AS paper_id, src.title AS paper_title, src.year AS year,
       CASE WHEN src:Book THEN 'book' ELSE 'paper' END AS source_type,
       chp.title AS chapter, sec.title AS section
ORDER BY score DESC
LIMIT $top_k
"""
```

Also update the module docstring provenance note at `server/queries.py:3-4` to:

```python
"""Cypher constants + pure validation/shaping for the kg MCP tools.

All queries are read-only. Provenance note: paper chunks carry `position` (int order
within the paper) — cite as (paper, chunk position). Book chunks additionally carry
`chapter`/`section` titles and `source_type='book'` — cite as (book, chapter, section).
"""
```

- [ ] **Step 4: Run unit test to verify it passes**

Run: `uv run --extra dev --extra server pytest tests/server/test_queries.py -q`
Expected: PASS (all tests, not just the new one — nothing else asserts on these constants)

- [ ] **Step 5: Add the live integration test (skips locally without env)**

Append to `tests/server/test_integration_server.py`:

```python
def test_search_chunks_reaches_book_content(mcp, graph):
    """Williams (v2-ingested) must be findable by hybrid search with a section citation.
    'upcrossing' is Williams-specific vocabulary absent from the paper corpus."""
    from server.retrieve import search_chunks_core
    out = search_chunks_core(graph, "upcrossing lemma martingale convergence", top_k=8)
    book_hits = [c for c in out["chunks"] if c.get("source_type") == "book"]
    assert book_hits, f"no book chunks in hits: {[c['paper_title'] for c in out['chunks']]}"
    assert book_hits[0]["section"] is not None
    assert book_hits[0]["chapter"] is not None
```

- [ ] **Step 6: Run integration test (live graph required; otherwise verify it skips)**

Run: `set -a && source .env && set +a && uv run --extra dev --extra server pytest tests/server/test_integration_server.py::test_search_chunks_reaches_book_content --run-integration -q`
Expected: PASS against Aura (or SKIP if env vars absent)

- [ ] **Step 7: Commit**

```bash
git add server/queries.py tests/server/test_queries.py tests/server/test_integration_server.py
git commit -m "feat(server): chunk search reaches book content with chapter/section citations"
```

---

### Task 2: EXPAND_LOCAL handles book sources

**Files:**
- Modify: `server/queries.py:169-185` (EXPAND_LOCAL)
- Test: `tests/server/test_queries.py` (append)

**Interfaces:**
- Consumes: `paper_id` values from Task 1 hits (may now be book ids)
- Produces: expand rows keep the shape `{paper_id, concepts, definitions, results, cites, cited_by}`; for book ids, `concepts` come via `COVERS`, `definitions`/`results` via chapter→section paths, `cites`/`cited_by` are empty lists.

- [ ] **Step 1: Write the failing unit test**

Append to `tests/server/test_queries.py`:

```python
def test_expand_local_includes_book_paths():
    assert "src:Paper OR src:Book" in q.EXPAND_LOCAL
    assert "DISCUSSES|COVERS" in q.EXPAND_LOCAL
    assert "HAS_CHAPTER" in q.EXPAND_LOCAL      # book statement path
    # null-struct hygiene: combined collects must filter empty OPTIONAL rows
    assert "WHERE x.id IS NOT NULL" in q.EXPAND_LOCAL
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev --extra server pytest tests/server/test_queries.py::test_expand_local_includes_book_paths -q`
Expected: FAIL

- [ ] **Step 3: Replace EXPAND_LOCAL**

```python
EXPAND_LOCAL = """
UNWIND $paper_ids AS pid
MATCH (src) WHERE (src:Paper OR src:Book) AND src.id = pid
OPTIONAL MATCH (src)-[:DISCUSSES|COVERS]->(c:Concept)
WITH src, collect(DISTINCT c.name)[..10] AS concepts
OPTIONAL MATCH (src)-[:STATES]->(pd:Definition)
OPTIONAL MATCH (src)-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(bd:Definition)
WITH src, concepts,
     [x IN collect(DISTINCT {id: pd.id, term: pd.term}) +
           collect(DISTINCT {id: bd.id, term: bd.term})
      WHERE x.id IS NOT NULL][..10] AS definitions
OPTIONAL MATCH (src)-[:STATES]->(pr:Result)
OPTIONAL MATCH (src)-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(br:Result)
WITH src, concepts, definitions,
     [x IN collect(DISTINCT {id: pr.id, kind: pr.kind, name: pr.name}) +
           collect(DISTINCT {id: br.id, kind: br.kind, name: br.name})
      WHERE x.id IS NOT NULL][..10] AS results
OPTIONAL MATCH (src)-[:CITES]->(o:Paper)
WITH src, concepts, definitions, results,
     [x IN collect(DISTINCT {id: o.id, title: o.title}) WHERE x.id IS NOT NULL][..5] AS cites
OPTIONAL MATCH (i:Paper)-[:CITES]->(src)
RETURN src.id AS paper_id, concepts, definitions, results, cites,
       [x IN collect(DISTINCT {id: i.id, title: i.title}) WHERE x.id IS NOT NULL][..5] AS cited_by
"""
```

- [ ] **Step 4: Run the full server unit suite**

Run: `uv run --extra dev --extra server pytest tests/server/ -q`
Expected: PASS (integration tests skip)

- [ ] **Step 5: Commit**

```bash
git add server/queries.py tests/server/test_queries.py
git commit -m "feat(server): expand=local resolves book concepts and statements"
```

---

### Task 3: GET_CONCEPT surfaces book definitions and book sources

**Files:**
- Modify: `server/queries.py:239-262` (GET_CONCEPT)
- Test: `tests/server/test_queries.py` (append)
- Test: `tests/server/test_integration_server.py` (append)

**Interfaces:**
- Consumes: nothing new
- Produces: each `definitions[]` entry gains `source_type`, `chapter`, `section` (book-stated definitions get `paper_id`/`paper_title` = book id/title); the `papers[]` list now also contains books covering the concept, marked `source_type: 'book'`.

- [ ] **Step 1: Write the failing unit test**

```python
def test_get_concept_includes_book_stated_definitions_and_covers():
    assert "Section)-[:STATES]->" in q.GET_CONCEPT or ":STATES]-(bs:Section)" in q.GET_CONCEPT
    assert "COVERS" in q.GET_CONCEPT
    assert "coalesce(dp.id, bk.id)" in q.GET_CONCEPT
    assert "AS source_type" in q.GET_CONCEPT
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev --extra server pytest tests/server/test_queries.py::test_get_concept_includes_book_stated_definitions_and_covers -q`
Expected: FAIL

- [ ] **Step 3: Replace GET_CONCEPT**

```python
GET_CONCEPT = """
MATCH (c:Concept)
WHERE toLower(c.name) = toLower($name)
OPTIONAL MATCH (d:Definition)-[:DEFINES]->(c)
OPTIONAL MATCH (dp:Paper)-[:STATES]->(d)
OPTIONAL MATCH (bs:Section)-[:STATES]->(d)
OPTIONAL MATCH (bk:Book)-[:HAS_CHAPTER]->(bch:Chapter)-[:HAS_SECTION]->(bs)
WITH c, [x IN collect(DISTINCT {
           id: d.id, term: d.term, statement: d.statement,
           paper_id: coalesce(dp.id, bk.id), paper_title: coalesce(dp.title, bk.title),
           source_type: CASE WHEN dp IS NOT NULL THEN 'paper'
                             WHEN bk IS NOT NULL THEN 'book' END,
           chapter: bch.title, section: bs.title})
         WHERE x.id IS NOT NULL][..10] AS definitions
OPTIONAL MATCH (p:Paper)-[:DISCUSSES]->(c)
WITH c, definitions,
     [x IN collect(DISTINCT {id: p.id, title: p.title, year: p.year,
                             source_type: 'paper'}) WHERE x.id IS NOT NULL][..15] AS papers
OPTIONAL MATCH (bkc:Book)-[:COVERS]->(c)
WITH c, definitions,
     papers + [x IN collect(DISTINCT {id: bkc.id, title: bkc.title,
                                      source_type: 'book'}) WHERE x.id IS NOT NULL] AS papers
OPTIONAL MATCH (p2:Paper)-[:DISCUSSES]->(c)
OPTIONAL MATCH (p2)-[:DISCUSSES]->(other:Concept)
WHERE other.name <> c.name
WITH c, definitions, papers, other.name AS oname, count(DISTINCT p2) AS shared
ORDER BY shared DESC
WITH c, definitions, papers, collect(oname)[..10] AS related_concepts
OPTIONAL MATCH (ch:Chunk)-[:MENTIONS]->(c)
WITH c, definitions, papers, related_concepts, collect(DISTINCT ch) AS chs
WITH c, definitions, papers, related_concepts,
     [x IN chs | {chunk_id: x.id, position: x.position,
                  text: left(x.text, 600)}][..5] AS supporting_chunks
RETURN c.name AS name, c.tags AS tags, c.description AS description,
       definitions, papers, related_concepts, supporting_chunks
"""
```

- [ ] **Step 4: Run unit tests, then add and run the integration test**

Run: `uv run --extra dev --extra server pytest tests/server/ -q` → PASS

Append to `tests/server/test_integration_server.py`:

```python
def test_get_concept_returns_book_definition(mcp):
    """'supermartingale' is defined in Williams; its definition entry must cite the book."""
    out = _call(mcp, "get_concept", {"name": "supermartingale"})
    defs = out[1]["definitions"] if isinstance(out, tuple) else out["definitions"]
    book_defs = [d for d in defs if d.get("source_type") == "book"]
    assert book_defs, f"no book-sourced definitions: {defs}"
    assert book_defs[0]["section"] is not None
```

Run: `set -a && source .env && set +a && uv run --extra dev --extra server pytest tests/server/test_integration_server.py::test_get_concept_returns_book_definition --run-integration -q`
Expected: PASS (adjust the concept name to one Williams actually defines if 'supermartingale' resolves differently — check with `run_cypher`: `MATCH (bs:Section)-[:STATES]->(d:Definition)-[:DEFINES]->(c:Concept) RETURN c.name LIMIT 10`)

- [ ] **Step 5: Commit**

```bash
git add server/queries.py tests/server/test_queries.py tests/server/test_integration_server.py
git commit -m "feat(server): get_concept surfaces book-stated definitions and COVERS sources"
```

---

### Task 4: GET_RESULTS + dependency chain accept book sources

The dependency-chain fix matters most: its non-optional `MATCH (p:Paper)-[:STATES]->(node)` **silently drops book results**, and after the v2 migration the cross-chapter DEPENDS_ON edges are exactly book results.

**Files:**
- Modify: `server/queries.py:273-284` (GET_RESULTS) and `server/queries.py:287-302` (`dependency_chain_cypher`)
- Test: `tests/server/test_queries.py` (append)
- Test: `tests/server/test_integration_server.py` (append)

**Interfaces:**
- Consumes: nothing new
- Produces: result rows gain `source_type`, `chapter`, `section`; `$paper_id` matches paper OR book ids; dependency-chain nodes stated only by book sections are no longer dropped.

- [ ] **Step 1: Write the failing unit tests**

```python
def test_get_results_accepts_book_sources():
    assert "OPTIONAL MATCH (sp:Paper)-[:STATES]->" in q.GET_RESULTS
    assert "Section)-[:STATES]->" in q.GET_RESULTS or "(sec:Section)-[:STATES]" in q.GET_RESULTS
    assert "coalesce(sp.id, bk.id)" in q.GET_RESULTS
    assert "bk.id = $paper_id" in q.GET_RESULTS


def test_dependency_chain_keeps_book_results():
    cy = dependency_chain_cypher(3)
    assert "MATCH (p:Paper)-[:STATES]->" not in cy      # the silent-drop pattern is gone
    assert "OPTIONAL MATCH (p:Paper)-[:STATES]->" in cy
    assert "coalesce(p.id, bk.id)" in cy
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --extra dev --extra server pytest tests/server/test_queries.py -q -k "book_sources or keeps_book"`
Expected: 2 FAIL

- [ ] **Step 3: Replace GET_RESULTS and dependency_chain_cypher**

```python
GET_RESULTS = """
MATCH (r:Result)
WHERE ($concept IS NULL OR EXISTS {
        MATCH (r)-[:USES]->(c:Concept) WHERE toLower(c.name) = toLower($concept) })
  AND ($kind IS NULL OR r.kind = $kind)
OPTIONAL MATCH (sp:Paper)-[:STATES]->(r)
OPTIONAL MATCH (sec:Section)-[:STATES]->(r)
OPTIONAL MATCH (bk:Book)-[:HAS_CHAPTER]->(chp:Chapter)-[:HAS_SECTION]->(sec)
WITH r, sp, sec, chp, bk
WHERE (sp IS NOT NULL OR bk IS NOT NULL)
  AND ($paper_id IS NULL OR sp.id = $paper_id OR bk.id = $paper_id)
RETURN r.id AS id, r.kind AS kind, r.name AS name, r.statement AS statement,
       coalesce(sp.id, bk.id) AS paper_id, coalesce(sp.title, bk.title) AS paper_title,
       CASE WHEN sp IS NOT NULL THEN 'paper' ELSE 'book' END AS source_type,
       chp.title AS chapter, sec.title AS section
LIMIT 25
"""
```

```python
def dependency_chain_cypher(depth: int) -> str:
    """Variable-length hops can't be parameterized; interpolate a CLAMPED int only."""
    d = validate_depth(depth)
    return f"""
MATCH (r:Result {{id: $result_id}})
OPTIONAL MATCH (r)-[:DEPENDS_ON*1..{d}]->(dep:Result)
WITH r, collect(DISTINCT dep) AS deps
UNWIND ([r] + deps) AS node
OPTIONAL MATCH (p:Paper)-[:STATES]->(node)
OPTIONAL MATCH (sec:Section)-[:STATES]->(node)
OPTIONAL MATCH (bk:Book)-[:HAS_CHAPTER]->(chp:Chapter)-[:HAS_SECTION]->(sec)
OPTIONAL MATCH (node)-[:USES]->(c:Concept)
OPTIONAL MATCH (node)-[:DEPENDS_ON]->(d2:Result)
RETURN node.id AS id, node.kind AS kind, node.name AS name, node.statement AS statement,
       coalesce(p.id, bk.id) AS paper_id, coalesce(p.title, bk.title) AS paper_title,
       CASE WHEN p IS NOT NULL THEN 'paper' WHEN bk IS NOT NULL THEN 'book' END AS source_type,
       chp.title AS chapter, sec.title AS section,
       collect(DISTINCT c.name) AS uses_concepts,
       collect(DISTINCT d2.id) AS depends_on
"""
```

- [ ] **Step 4: Run unit suite, add integration test, run it**

Run: `uv run --extra dev --extra server pytest tests/server/ -q` → PASS

Append to `tests/server/test_integration_server.py`:

```python
def test_dependency_chain_traverses_book_results(graph):
    """Post-v2, cross-chapter DEPENDS_ON edges live on Williams results. Pick one live
    and confirm the chain query returns book-sourced nodes instead of dropping them."""
    seed = graph.read(
        "MATCH (:Section)-[:STATES]->(r:Result)-[:DEPENDS_ON]->(:Result) "
        "RETURN r.id AS id LIMIT 1")
    if not seed:
        import pytest
        pytest.skip("no book results with dependencies in this graph")
    from server import queries as q
    rows = graph.read(q.dependency_chain_cypher(3), result_id=seed[0]["id"])
    assert rows and any(r["source_type"] == "book" for r in rows)
```

Run: `set -a && source .env && set +a && uv run --extra dev --extra server pytest tests/server/test_integration_server.py::test_dependency_chain_traverses_book_results --run-integration -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server/queries.py tests/server/test_queries.py tests/server/test_integration_server.py
git commit -m "feat(server): results and dependency chains cite book sections; fix silent drop of book results"
```

---

### Task 5: Overview counts, tool docstrings, ask-skill note

**Files:**
- Modify: `server/queries.py:317-324` (OVERVIEW_COUNTS)
- Modify: `server/tools.py` (docstrings of `search_chunks`, `get_results`, `get_corpus_overview`)
- Modify: `/Users/osianshelley/Projects/kg/skills/ask/SKILL.md` (Corpus notes section)
- Test: `tests/server/test_queries.py` (append)

**Interfaces:**
- Consumes: nothing new
- Produces: `get_corpus_overview` counts gain `books` and `notations` keys.

- [ ] **Step 1: Write the failing unit test**

```python
def test_overview_counts_books_and_notations():
    assert "MATCH (b:Book)" in q.OVERVIEW_COUNTS
    assert "MATCH (n:Notation)" in q.OVERVIEW_COUNTS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev --extra server pytest tests/server/test_queries.py::test_overview_counts_books_and_notations -q`
Expected: FAIL

- [ ] **Step 3: Replace OVERVIEW_COUNTS and update docstrings**

```python
OVERVIEW_COUNTS = """
CALL { MATCH (p:Paper) RETURN count(p) AS papers }
CALL { MATCH (b:Book) RETURN count(b) AS books }
CALL { MATCH (c:Chunk) RETURN count(c) AS chunks }
CALL { MATCH (co:Concept) RETURN count(co) AS concepts }
CALL { MATCH (d:Definition) RETURN count(d) AS definitions }
CALL { MATCH (r:Result) RETURN count(r) AS results }
CALL { MATCH (n:Notation) RETURN count(n) AS notations }
RETURN papers, books, chunks, concepts, definitions, results, notations
"""
```

In `server/tools.py`, update these three docstrings verbatim:

```python
    @mcp.tool()
    def search_chunks(query: str, top_k: int = 8, expand: str = "local",
                      paper_id: str | None = None) -> dict:
        """Hybrid (vector + keyword) search over paper AND book chunks; expand='local' adds
        each source's concepts, definitions, results, and CITES neighbours; expand='concepts'
        pivots to the top concepts across hits. Cite paper hits as (paper_title, chunk
        position); book hits carry source_type='book' — cite as (book, chapter, section).
        paper_id also accepts a book id to scope the search."""
```

```python
    @mcp.tool()
    def get_results(concept: str | None = None, paper_id: str | None = None,
                    kind: str | None = None) -> dict:
        """Theorems/lemmas/propositions/corollaries that USE a concept and/or are STATED
        by a paper or a book section (book rows carry chapter/section + source_type='book').
        paper_id accepts a paper id or a book id. Provide at least one of concept/paper_id."""
```

```python
    @mcp.tool()
    def get_corpus_overview() -> dict:
        """Corpus shape: node counts (papers, books, chunks, concepts, definitions, results,
        notations), most-discussed concepts, most recent papers. Call this FIRST to judge
        whether the corpus can support a question."""
```

In `/Users/osianshelley/Projects/kg/skills/ask/SKILL.md`, replace the first Corpus-notes bullet:

```markdown
- Paper chunks cite as (paper, chunk position). Book-backed content is first-class:
  book chunks, definitions, and results carry `source_type: "book"` plus chapter and
  section titles — cite as (book, chapter, section). Notation questions (what does
  a symbol mean?) are answerable via run_cypher on Notation nodes.
```

- [ ] **Step 4: Run the full suite**

Run: `uv run --extra dev --extra server pytest -q`
Expected: all PASS, integration skips

- [ ] **Step 5: Commit**

```bash
git add server/queries.py server/tools.py tests/server/test_queries.py
git commit -m "feat(server): overview counts books/notations; book-aware tool docstrings"
```

(The SKILL.md edit lives in `~/Projects/kg`, a separate repo — commit it there:
`git -C ~/Projects/kg add skills/ask/SKILL.md && git -C ~/Projects/kg commit -m "docs(ask): book content is first-class in kg tools"`)

---

### Task 6: Live verification, PR, deploy

**Files:**
- No new code. PR + deploy + smoke.

**Interfaces:**
- Consumes: everything above
- Produces: the deployed server at `https://kg-graph.fly.dev` serves book-aware retrieval.

- [ ] **Step 1: Full local suite + lint**

Run: `uv run --extra dev --extra server pytest -q && uv run ruff check server/ tests/ && uv run black --check server/`
Expected: all PASS / clean

- [ ] **Step 2: Full integration suite against Aura**

Run: `set -a && source .env && set +a && uv run --extra dev --extra server pytest tests/server/test_integration_server.py --run-integration -q`
Expected: all PASS (including the three new book tests)

- [ ] **Step 3: Open the PR**

```bash
git push -u origin HEAD
gh pr create --title "Book-aware retrieval: search, concepts, results, and chains cite (book, chapter, section)" \
  --body "Typed tools previously hard-required Paper document owners, silently excluding all book content (118 Williams chunks, 188 results, 57 definitions). Source-union queries keep legacy field names (book rows populate paper_id/paper_title) and add source_type/chapter/section. Fixes the dependency-chain silent drop of book results. Unit tests assert query shapes; three live integration tests verify against Aura."
```

- [ ] **Step 4: After merge — deploy and smoke (requires user go-ahead for the deploy)**

```bash
git checkout main && git pull --ff-only
fly deploy --config docker/fly.toml --dockerfile docker/Dockerfile.server
# smoke (token from the kg MCP client config):
uv run --extra server python scripts/smoke_server.py https://kg-graph.fly.dev <token>
```
Expected: healthz 200, all 11 tools ok

- [ ] **Step 5: End-to-end sanity via a fresh Claude session**

Ask `/kg:ask "state the martingale convergence theorem as given in Williams and name the chapter it appears in"` — the answer must quote a Williams result and cite (book, chapter, section) without resorting to run_cypher for the text itself.

---

## Non-goals (explicitly out of scope)

- `search_papers` / `get_paper` book variants (books are discoverable via search_chunks + run_cypher; add a `get_book` only if usage shows the need)
- A typed `get_notation` tool (run_cypher covers symbol lookups; the skill note points there)
- `PAPER_VECTOR_AGG` (search_papers ranking) stays paper-only by design
- Re-ranking or retrieval-quality tuning (top_k, expand behavior unchanged)
