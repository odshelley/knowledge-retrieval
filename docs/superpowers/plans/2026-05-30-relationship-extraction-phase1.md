# Relationship Extraction — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the three within-paper semantic edges — `Definition–DEFINES→Concept`, `Result–USES→Concept`, `Result–DEPENDS_ON→Result` — to the bespoke document-graph builder, so the Neo4j graph carries the connective tissue its schema already declares.

**Architecture:** Extraction (Pydantic models passed to the OpenAI/Claude `.parse()` helpers) gains three link fields that the LLM populates *by name*. Those names flow through the MinIO artifacts unchanged. `graph_write` — the single authority on node identity — translates the names into canonical-concept names and content-hash result-ids, then writes the edges with idempotent `MERGE`. Unresolvable references are skipped and counted, never invented.

**Tech Stack:** Python 3.12, Pydantic v2 (structured-output models), Dagster assets, Neo4j (Cypher `MERGE`/`UNWIND`), pytest (`uv run --extra dev pytest`).

**Design source:** `docs/superpowers/specs/2026-05-30-relationship-extraction-design.md`. (Note: the spec §4.2 describes a dict `EXTRACTION_SCHEMA`; the code has since moved to Pydantic models — this plan implements the same design against the current Pydantic code. Phase 2, concept↔concept hybrid edges, is committed but OUT OF SCOPE here.)

---

## File structure

| File | Responsibility | Change |
|---|---|---|
| `pipeline/extraction.py` | extraction target models + dedup-merge | add link fields to `Definition`/`Result`; faithfulness clause in `SYSTEM_PROMPT`; union link lists in `merge_results` |
| `pipeline/extraction_anthropic.py` | Claude extraction path | **no change** — shares the models + `SYSTEM_PROMPT`; new fields flow through `.parse()` |
| `pipeline/assets/extracted_graph.py` | run extraction, write artifact | **no change** — `model_dump()` serializes the new fields automatically |
| `pipeline/assets/resolved_entities.py` | decide-only concept resolution | emit `surface` (original name) alongside canonical `name`, via a new `resolved_concept_row` helper |
| `pipeline/assets/graph_write.py` | sole graph writer | add pure edge-row builders; 3 new Cypher passes; wire into asset; counts + `skipped_refs` metric |
| `pipeline/schema.py` | schema vocabulary | **no change** — `DEFINES`/`USES`/`DEPENDS_ON` + their patterns are already declared |
| `tests/test_extraction.py` | unit | link-field parse + merge-union tests |
| `tests/test_resolved_entities.py` | unit (new file) | `resolved_concept_row` carries `surface` |
| `tests/test_graph_write.py` | unit | edge-row builder tests (map, skip-unknown, skip-self) |
| `tests/integration/test_end_to_end.py` | integration | assert new edges idempotent |
| `README.md` | docs | note the 3 new edges in the `graph_write` description + asset-DAG |

---

## Task 1: Add link fields to the extraction models

**Files:**
- Modify: `pipeline/extraction.py` (the `Definition` and `Result` models, and `SYSTEM_PROMPT`)
- Test: `tests/test_extraction.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_extraction.py`:

```python
def test_parse_extraction_reads_link_fields():
    payload = {
        "concepts": [{"name": "BSDE", "kind": "concept"}],
        "definitions": [{"term": "BSDE", "statement": "$dY=...$", "defines": ["BSDE"]}],
        "results": [{"name": "Thm 1", "kind": "theorem", "statement": "$x=y$",
                     "uses": ["BSDE"], "depends_on": ["Lemma 2.4"]}],
    }
    r = parse_extraction(payload)
    assert r.definitions[0].defines == ["BSDE"]
    assert r.results[0].uses == ["BSDE"]
    assert r.results[0].depends_on == ["Lemma 2.4"]


def test_parse_extraction_defaults_link_fields_to_empty():
    payload = {
        "definitions": [{"term": "X", "statement": "s"}],
        "results": [{"name": "T", "kind": "lemma", "statement": "s"}],
    }
    r = parse_extraction(payload)
    assert r.definitions[0].defines == []
    assert r.results[0].uses == []
    assert r.results[0].depends_on == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra dev pytest tests/test_extraction.py::test_parse_extraction_reads_link_fields tests/test_extraction.py::test_parse_extraction_defaults_link_fields_to_empty -v`
Expected: FAIL — `AttributeError: 'Definition' object has no attribute 'defines'` (and similar for `uses`/`depends_on`).

- [ ] **Step 3: Add the fields to the models**

In `pipeline/extraction.py`, add to the `Definition` model (after the `statement` field, before the `_strip_term` validator):

```python
    defines: list[str] = Field(
        default_factory=list,
        description="Names of the concepts (from the concepts you extract in this same "
        "response) that this definition defines. Use the exact concept name strings; "
        "usually one. Leave empty if unsure.",
    )
```

Add to the `Result` model (after the `statement` field, before the `_strip_name` validator):

```python
    uses: list[str] = Field(
        default_factory=list,
        description="Names of the concepts (from the concepts you extract in this same "
        "response) that this result invokes or relies on. Use the exact concept name "
        "strings. Omit any you are unsure of.",
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description='Labels of OTHER results this result depends on, e.g. ["Lemma 2.4"]. '
        "Use the exact result labels as they appear. Leave empty if none or unsure.",
    )
```

- [ ] **Step 4: Add the faithfulness clause to `SYSTEM_PROMPT`**

In `pipeline/extraction.py`, replace the entire `SYSTEM_PROMPT` assignment with this version (the only change is the appended faithfulness clause on the link fields):

```python
SYSTEM_PROMPT = """You are an information-extraction assistant for STEM research papers \
(most often rooted in mathematics, statistics, or AI / machine learning, but spanning the \
sciences and engineering broadly). From the chunk, populate the concepts, definitions, and \
results of the response schema, following each field's description. Emit nothing not asserted \
by the text. When filling a definition's `defines`, a result's `uses`, or a result's \
`depends_on`, reference ONLY names you have already produced in this same response; if \
unsure, leave the list empty."""
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_extraction.py -v`
Expected: PASS (all tests, including the existing ones).

- [ ] **Step 6: Commit**

```bash
git add pipeline/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): add defines/uses/depends_on link fields to entity models"
```

---

## Task 2: Union link lists in `merge_results`

Chunks overlap, so the same definition/result is extracted from adjacent chunks — and each copy may carry *different* link names. `merge_results` keeps the first copy of an entity; it must union the later copies' link lists into the kept one so no link is lost.

**Files:**
- Modify: `pipeline/extraction.py` (`merge_results`, plus a small helper)
- Test: `tests/test_extraction.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_extraction.py`:

```python
def test_merge_results_unions_link_lists_across_overlapping_chunks():
    p1 = ExtractionResult(
        definitions=[Definition(term="BSDE", statement="$s$", defines=["BSDE"])],
        results=[Result(name="T1", kind="theorem", statement="$x=y$",
                        uses=["BSDE"], depends_on=["Lemma 2.4"])],
    )
    p2 = ExtractionResult(
        definitions=[Definition(term="BSDE", statement="$s$", defines=["Backward SDE"])],
        results=[Result(name="T1", kind="theorem", statement="$x=y$",
                        uses=["Feynman-Kac"], depends_on=["Lemma 2.4"])],
    )
    merged = merge_results([p1, p2])
    assert len(merged.definitions) == 1
    assert merged.definitions[0].defines == ["BSDE", "Backward SDE"]   # unioned, order-preserved
    assert len(merged.results) == 1
    assert merged.results[0].uses == ["BSDE", "Feynman-Kac"]
    assert merged.results[0].depends_on == ["Lemma 2.4"]              # deduped, not doubled
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --extra dev pytest tests/test_extraction.py::test_merge_results_unions_link_lists_across_overlapping_chunks -v`
Expected: FAIL — merged definition/result keeps only the first copy's links (`defines == ["BSDE"]`), so the assertion on `["BSDE", "Backward SDE"]` fails.

- [ ] **Step 3: Add the helper and rewrite `merge_results`**

In `pipeline/extraction.py`, add this helper just above `merge_results`:

```python
def _extend_unique(dst: list[str], src: list[str]) -> None:
    """Append items from src not already in dst, preserving order. Mutates dst in place
    (and thus the kept model it belongs to); inputs are not read again after merge."""
    for item in src:
        if item not in dst:
            dst.append(item)
```

Replace the `definitions` and `results` blocks of `merge_results` (the two loops that build `definitions` and `results`) with these, which key the kept entity so later copies can union into it:

```python
    seen_d: dict[str, Definition] = {}
    definitions = []
    for p in parts:
        for d in p.definitions:
            k = normalize_statement(d.statement)
            kept = seen_d.get(k)
            if kept is None:
                seen_d[k] = d
                definitions.append(d)
            else:
                _extend_unique(kept.defines, d.defines)
    seen_r: dict[tuple[str, str], Result] = {}
    results = []
    for p in parts:
        for r in p.results:
            k = (r.kind, normalize_statement(r.statement))
            kept = seen_r.get(k)
            if kept is None:
                seen_r[k] = r
                results.append(r)
            else:
                _extend_unique(kept.uses, r.uses)
                _extend_unique(kept.depends_on, r.depends_on)
```

(Leave the `concepts` block unchanged; concepts have no link fields.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_extraction.py -v`
Expected: PASS — including the existing `test_merge_results_dedupes_*` and `test_merge_results_keeps_distinct_results_of_different_kind` tests.

- [ ] **Step 5: Commit**

```bash
git add pipeline/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): union link lists when merging entities across chunks"
```

---

## Task 3: Emit the surface name from `resolved_entities`

`resolved_entities` currently overwrites each concept's `name` with the canonical and drops the original. `graph_write` needs the original (surface) name to translate the by-name `defines`/`uses` references onto canonical Concept nodes. Extract the row construction into a pure helper so it is unit-testable without the asset's Postgres/OpenAI/MinIO machinery.

**Files:**
- Modify: `pipeline/assets/resolved_entities.py`
- Test: `tests/test_resolved_entities.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_resolved_entities.py`:

```python
from pipeline.assets.resolved_entities import resolved_concept_row


def test_resolved_concept_row_carries_surface_and_canonical():
    row = resolved_concept_row(
        surface="BSDE",
        canonical="Backward Stochastic Differential Equation",
        kind="concept",
        action="merge",
        embedding=[0.1, 0.2],
    )
    assert row["surface"] == "BSDE"                                   # original, for link resolution
    assert row["name"] == "Backward Stochastic Differential Equation"  # canonical, the node key
    assert row["kind"] == "concept"
    assert row["action"] == "merge"
    assert row["embedding"] == [0.1, 0.2]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --extra dev pytest tests/test_resolved_entities.py -v`
Expected: FAIL — `ImportError: cannot import name 'resolved_concept_row'`.

- [ ] **Step 3: Add the helper and use it in the asset**

In `pipeline/assets/resolved_entities.py`, add this module-level function (above the `@asset` decorator):

```python
def resolved_concept_row(surface: str, canonical: str, kind: str, action: str,
                         embedding: list[float]) -> dict:
    """One resolved-concept record. `surface` is the name as extracted (used by graph_write
    to map defines/uses references); `name` is the canonical node key; `embedding` is upserted
    by graph_write keyed on the canonical name."""
    return {"surface": surface, "name": canonical, "kind": kind,
            "action": action, "embedding": embedding}
```

Then replace the existing `resolved.append({...})` call in the asset body with:

```python
                resolved.append(resolved_concept_row(
                    surface=c["name"], canonical=canonical, kind=c["kind"],
                    action=action.value, embedding=v,
                ))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --extra dev pytest tests/test_resolved_entities.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/assets/resolved_entities.py tests/test_resolved_entities.py
git commit -m "feat(resolver): emit surface name alongside canonical for link resolution"
```

---

## Task 4: Edge-row builders in `graph_write` (pure functions)

Three pure functions translate the by-name links into Cypher parameter rows, resolving concept names through the `surface→canonical` map and result names through the `result-name→result-id` map. Unresolvable names (and self-dependencies) are skipped and counted. These are the unit-tested core; the asset wiring (Task 5) is integration-tested (Task 6), matching this repo's documented approach for asset bodies (see the note in `tests/test_graph_write.py`).

**Files:**
- Modify: `pipeline/assets/graph_write.py` (add functions after `result_rows`)
- Test: `tests/test_graph_write.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_graph_write.py` (extend the import on line 1-3 to include the new names):

```python
from pipeline.assets.graph_write import (
    concept_rows, definition_rows, result_rows, normalize_statement, def_id, result_id,
    defines_edge_rows, uses_edge_rows, depends_on_edge_rows, result_name_index,
)


# Keys are LOWERCASED surface names — concepts are deduped case-insensitively upstream,
# so link resolution must match case-insensitively too.
_SURFACE_TO_CANON = {"bsde": "Backward SDE", "feynman-kac": "Nonlinear Feynman-Kac"}


def test_defines_edge_rows_is_case_insensitive_and_skips_unknown():
    # "BSDE" (upper) must resolve against the lowercased "bsde" key; "Ghost Concept" is unknown.
    defs = [{"term": "BSDE", "statement": "$s$", "defines": ["BSDE", "Ghost Concept"]}]
    rows, skipped = defines_edge_rows("p1", defs, _SURFACE_TO_CANON)
    assert rows == [{"def_id": def_id("p1", "$s$"), "canonical": "Backward SDE"}]
    assert skipped == 1


def test_uses_edge_rows_is_case_insensitive_and_skips_unknown():
    results = [{"name": "T1", "kind": "theorem", "statement": "$x=y$",
                "uses": ["BSDE", "Feynman-Kac", "Nope"]}]
    rows, skipped = uses_edge_rows("p1", results, _SURFACE_TO_CANON)
    rid = result_id("p1", "theorem", "$x=y$")
    assert rows == [{"res_id": rid, "canonical": "Backward SDE"},
                    {"res_id": rid, "canonical": "Nonlinear Feynman-Kac"}]
    assert skipped == 1


def test_result_name_index_drops_empty_and_ambiguous_labels():
    rrows = [
        {"name": "Theorem 1", "id": "p1:theorem:aaa"},
        {"name": "Theorem 1", "id": "p1:theorem:bbb"},   # duplicate label → both dropped
        {"name": "Lemma 2.4", "id": "p1:lemma:ccc"},
        {"name": "", "id": "p1:theorem:ddd"},            # empty label → dropped
    ]
    assert result_name_index(rrows) == {"Lemma 2.4": "p1:lemma:ccc"}


def test_depends_on_edge_rows_maps_names_and_skips_self_and_unknown():
    results = [
        {"name": "Theorem 1", "kind": "theorem", "statement": "$a$",
         "depends_on": ["Lemma 2.4", "Theorem 1", "Missing"]},
        {"name": "Lemma 2.4", "kind": "lemma", "statement": "$b$", "depends_on": []},
    ]
    # Build the map exactly as the asset does (collision-safe), so the test proves real behavior.
    name_to_id = result_name_index(
        [{"name": r["name"], "id": result_id("p1", r["kind"], r["statement"])} for r in results]
    )
    rows, skipped = depends_on_edge_rows("p1", results, name_to_id)
    assert rows == [{"res_id": result_id("p1", "theorem", "$a$"),
                     "dep_id": result_id("p1", "lemma", "$b$")}]
    assert skipped == 2   # self-reference "Theorem 1" + unknown "Missing"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra dev pytest tests/test_graph_write.py -v`
Expected: FAIL — `ImportError: cannot import name 'defines_edge_rows'`.

- [ ] **Step 3: Implement the builders**

In `pipeline/assets/graph_write.py`, add after the `result_rows` function:

```python
def result_name_index(rrows: list[dict]) -> dict[str, str]:
    """Map result label -> result id, EXCLUDING empty and ambiguous (duplicate) labels.

    Result identity is (kind, normalized statement), NOT name, so two distinct results can
    share a label. Keying a plain dict on name would let a depends_on reference resolve to the
    wrong Result (last-wins). Dropping ambiguous labels makes those references skip+count
    instead of fabricating a wrong edge.
    """
    counts: dict[str, int] = {}
    for r in rrows:
        if r["name"]:
            counts[r["name"]] = counts.get(r["name"], 0) + 1
    return {r["name"]: r["id"] for r in rrows if r["name"] and counts[r["name"]] == 1}


def defines_edge_rows(paper_id: str, definitions: list[dict],
                      surface_to_canon: dict[str, str]) -> tuple[list[dict], int]:
    """Definition -> Concept rows. `surface_to_canon` is keyed on LOWERCASED surface names
    (concepts are deduped case-insensitively upstream). Skips names with no canonical."""
    rows, skipped = [], 0
    for d in definitions:
        did = def_id(paper_id, d["statement"])
        for name in d.get("defines", []):
            canon = surface_to_canon.get(name.lower())
            if canon is None:
                skipped += 1
                continue
            rows.append({"def_id": did, "canonical": canon})
    return rows, skipped


def uses_edge_rows(paper_id: str, results: list[dict],
                   surface_to_canon: dict[str, str]) -> tuple[list[dict], int]:
    """Result -> Concept rows. `surface_to_canon` is keyed on LOWERCASED surface names.
    Skips names with no canonical."""
    rows, skipped = [], 0
    for r in results:
        rid = result_id(paper_id, r["kind"], r["statement"])
        for name in r.get("uses", []):
            canon = surface_to_canon.get(name.lower())
            if canon is None:
                skipped += 1
                continue
            rows.append({"res_id": rid, "canonical": canon})
    return rows, skipped


def depends_on_edge_rows(paper_id: str, results: list[dict],
                         name_to_result_id: dict[str, str]) -> tuple[list[dict], int]:
    """Result -> Result rows. Skips unknown/ambiguous result names and self-dependencies."""
    rows, skipped = [], 0
    for r in results:
        rid = result_id(paper_id, r["kind"], r["statement"])
        for dep_name in r.get("depends_on", []):
            dep = name_to_result_id.get(dep_name)
            if dep is None or dep == rid:
                skipped += 1
                continue
            rows.append({"res_id": rid, "dep_id": dep})
    return rows, skipped
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_graph_write.py -v`
Expected: PASS (all, including the existing row-builder tests).

- [ ] **Step 5: Commit**

```bash
git add pipeline/assets/graph_write.py tests/test_graph_write.py
git commit -m "feat(graph_write): pure edge-row builders for DEFINES/USES/DEPENDS_ON"
```

---

## Task 5: Wire the three edge passes into the `graph_write` asset

Add the Cypher and call the builders inside the existing Neo4j session, after the node-creating passes (so the Definition/Result/Concept nodes already exist), and surface the counts in metadata. The asset body itself is integration-verified (Task 6) — the repo deliberately avoids brittle nested-driver mocking for asset bodies.

**Files:**
- Modify: `pipeline/assets/graph_write.py` (Cypher constants + asset body + metadata)

- [ ] **Step 1: Add the three Cypher constants**

In `pipeline/assets/graph_write.py`, after the `MERGE_CITES` constant (around line 86), add:

```python
WRITE_DEFINES = """
UNWIND $rows AS row
  MATCH (d:Definition {id: row.def_id})
  MATCH (c:Concept {name: row.canonical})
  MERGE (d)-[:DEFINES]->(c)
"""

WRITE_RESULT_USES = """
UNWIND $rows AS row
  MATCH (r:Result {id: row.res_id})
  MATCH (c:Concept {name: row.canonical})
  MERGE (r)-[:USES]->(c)
"""

WRITE_RESULT_DEPENDS = """
UNWIND $rows AS row
  MATCH (r1:Result {id: row.res_id})
  MATCH (r2:Result {id: row.dep_id})
  MERGE (r1)-[:DEPENDS_ON]->(r2)
"""
```

- [ ] **Step 2: Build the maps + edge rows and run the passes**

In the asset body, immediately after the existing `s.run(WRITE_RESULTS, paper_id=paper_id, rows=rrows)` line, insert:

```python
        # Lowercased keys: concepts are deduped case-insensitively upstream, so link names
        # (which may differ in case from the kept concept) must resolve case-insensitively.
        surface_to_canon = {c.get("surface", c["name"]).lower(): c["name"] for c in concepts}
        name_to_result_id = result_name_index(rrows)  # collision-safe (drops ambiguous labels)
        def_edges, sk_def = defines_edge_rows(paper_id, resolved.get("definitions", []),
                                              surface_to_canon)
        use_edges, sk_use = uses_edge_rows(paper_id, resolved.get("results", []),
                                           surface_to_canon)
        dep_edges, sk_dep = depends_on_edge_rows(paper_id, resolved.get("results", []),
                                                 name_to_result_id)
        s.run(WRITE_DEFINES, rows=def_edges)
        s.run(WRITE_RESULT_USES, rows=use_edges)
        s.run(WRITE_RESULT_DEPENDS, rows=dep_edges)
```

(`concepts`, `rrows`, and `resolved` are already in scope from earlier in the asset body; `result_name_index` is defined in the same module by Task 4.)

- [ ] **Step 3: Add counts to the returned metadata**

In the `MaterializeResult(metadata={...})` at the end of the asset, add these entries:

```python
        "defines": MetadataValue.int(len(def_edges)),
        "uses": MetadataValue.int(len(use_edges)),
        "depends_on": MetadataValue.int(len(dep_edges)),
        "skipped_refs": MetadataValue.int(sk_def + sk_use + sk_dep),
```

- [ ] **Step 4: Run the full unit suite to confirm no regressions**

Run: `uv run --extra dev pytest`
Expected: PASS — the unit suite (integration tests are skipped by default per `addopts = -m 'not integration'`). This confirms imports resolve and nothing else broke.

- [ ] **Step 5: Lint check**

Run: `uv run --extra dev ruff check pipeline/assets/graph_write.py`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add pipeline/assets/graph_write.py
git commit -m "feat(graph_write): write DEFINES/USES/DEPENDS_ON edges + skipped-refs metric"
```

---

## Task 6: Integration coverage for the new edges

Verifies end-to-end that the edges are written and that re-running a paper does not duplicate them. Requires live services + fixture env vars and only runs under `--run-integration`.

**Files:**
- Modify: `tests/integration/test_end_to_end.py`

- [ ] **Step 1: Add the edge-presence + idempotency integration test**

Append to `tests/integration/test_end_to_end.py`:

```python
@pytest.mark.integration
def test_within_paper_edges_idempotent():
    """Re-running a paper does not duplicate DEFINES/USES/DEPENDS_ON edges (the deterministic
    guarantee). NOTE: edge *presence* is not asserted here — it depends on the LLM populating
    defines/uses/depends_on with names that resolve to listed concepts/results, which is
    fixture- and model-dependent. To guarantee presence, point INTEGRATION_FIXTURE_HASH at a
    curated paper known to yield each edge type and add per-type assertions below."""
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    key = _required_env("INTEGRATION_FIXTURE_HASH")
    instance.add_dynamic_partitions(DOCUMENTS_PARTITION, [key])

    def edge_counts():
        new = new_neo4j_from_env()
        with new.get_driver().session(database=new.database) as s:
            return {
                "defines": s.run(
                    "MATCH (:Paper {document_id:$k})-[:STATES]->(:Definition)-[e:DEFINES]->(:Concept) "
                    "RETURN count(e) AS n", k=key).single()["n"],
                "uses": s.run(
                    "MATCH (:Paper {document_id:$k})-[:STATES]->(:Result)-[e:USES]->(:Concept) "
                    "RETURN count(e) AS n", k=key).single()["n"],
                "depends_on": s.run(
                    "MATCH (:Paper {document_id:$k})-[:STATES]->(:Result)-[e:DEPENDS_ON]->(:Result) "
                    "RETURN count(e) AS n", k=key).single()["n"],
            }

    materialize(_ASSETS, partition_key=key, resources=_res(), instance=instance)
    first = edge_counts()
    materialize(_ASSETS, partition_key=key, resources=_res(), instance=instance)
    assert edge_counts() == first   # MERGE on stable ids ⇒ no duplicate edges on re-run
```

- [ ] **Step 2: Run the new integration test (requires live services + fixtures)**

Run: `uv run --extra dev pytest tests/integration/test_end_to_end.py::test_within_paper_edges_idempotent --run-integration -v`
Expected: PASS when services are up and `INTEGRATION_FIXTURE_HASH` points at a fixture PDF; otherwise SKIP (missing env var). If you cannot run live services, confirm the test is collected (not erroring) with: `uv run --extra dev pytest tests/integration/test_end_to_end.py --collect-only -q`.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_end_to_end.py
git commit -m "test(integration): assert DEFINES/USES/DEPENDS_ON edges + idempotency"
```

---

## Task 7: Update the README

The README still describes the entities as islands and omits the new edges. Bring it in line so the docs don't contradict the shipped graph.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the `graph_write` description**

Find the `graph_write` line in the "What it does" section (it currently lists "`Chunk`/`Concept`/`Definition`/`Result` nodes, `Paper→Concept` `DISCUSSES`/`DERIVED_FROM`, and `CITES` edges") and add the three new edges, e.g. append: "; plus `Definition–DEFINES→Concept`, `Result–USES→Concept`, and `Result–DEPENDS_ON→Result` (within-paper semantic links)."

- [ ] **Step 2: Update the Mermaid `graph_write` node**

In the asset-DAG Mermaid block, extend the `write` node label to mention the new edges (e.g. add a line "DEFINES · USES · DEPENDS_ON").

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: note DEFINES/USES/DEPENDS_ON edges in README"
```

---

## Done criteria

- `uv run --extra dev pytest` is green (unit suite).
- Re-ingest is idempotent for the new edges (integration); edge *presence* is fixture/model-dependent and only asserted with a curated fixture (see Task 6 note).
- `skipped_refs` is visible in the `graph_write` materialization metadata, surfacing any references the LLM made to entities it did not actually list (including names dropped for case/collision reasons).
- `pipeline/schema.py`, `pipeline/extraction_anthropic.py`, and `pipeline/assets/extracted_graph.py` are unchanged (edges already declared; new fields flow through the shared models / `model_dump`).
- **Stale-artifact migration is deliberate, not vestigial:** the `c.get("surface", c["name"])` fallback and the `d.get("defines", [])`/`r.get("uses"/"depends_on", [])` reads mean `graph_write` run on an *old* `resolved.json` (pre-`surface`, pre-link-fields) degrades gracefully to "no new edges, no crash." The normal path re-runs extraction/resolution and populates the fields.

## Out of scope (Phase 2 — committed, separate plan)

Concept↔concept hybrid edges (`SPECIALIZES`/`RELATED_TO`/`COMPARES_WITH`/`OTHER` with `description` + `confidence` + cross-paper `support_count`/`source_paper_ids` merge). See spec §5. Also deferred: the optional `PATTERNS`-enforcement guard in `graph_write` (spec §7).
