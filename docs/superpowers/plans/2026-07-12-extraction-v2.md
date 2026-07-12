# Book/Paper Extraction v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extraction captures notation and proof sketches, statements are never heading-echoes, cross-chunk/cross-chapter dependency references resolve into graph edges via a post-pass linker, and front/back-matter chapters are classified and skipped.

**Architecture:** Additive Pydantic schema changes + one shared book-aware system prompt (both pipelines); chapter roles assigned at structure time gate partition registration; a new per-book `book_link_resolution` asset runs after all chapters are written and turns free-text refs into `DEPENDS_ON`/`PROVED_IN`/`DENOTES` edges using deterministic label normalization with an optional batched LLM fallback.

**Tech Stack:** Python 3.12, Pydantic v2, Dagster (dynamic partitions, sensors), Neo4j (Aura), MinIO artifacts, Anthropic SDK (`messages.parse`).

**Spec:** `docs/superpowers/specs/2026-07-12-book-extraction-v2-design.md`

## Global Constraints

- All schema fields ADDITIVE with defaults — payloads produced by v1 must still validate (spec §1).
- One shared `SYSTEM_PROMPT` for papers and books; no fork (spec §2).
- `Notation.id` is per-document: `<book_id>:not:<hash12(normalized symbol)>` — never global by symbol (spec §4).
- Linker never guesses: unresolved refs are logged and dropped (spec §5).
- Extraction runs only for chapter roles `content | notation_guide | exercises` (spec §3).
- Work on branch `feat/extraction-v2` in this worktree. Run tests with `uv run pytest` from the worktree root. Never run `git stash`.
- Test conventions: plain pytest functions in `tests/test_*.py`, no API calls in unit tests.

---

### Task 1: Schema — Notation, ProofSketch, statement flags

**Files:**
- Modify: `pipeline/extraction/extraction.py` (models section, lines ~18–115)
- Test: `tests/test_extraction.py` (append)

**Interfaces:**
- Produces: `Notation(symbol_latex: str, meaning: str, concept: str = "")`; `ProofSketch(sketch: str, technique: str = "")`; new `Result` fields `proof: ProofSketch | None = None`, `proof_present: bool = False`, `statement_complete: bool = True`; new `Definition` field `uses: list[str] = []`; new `ExtractionResult` field `notations: list[Notation] = []`. Tasks 2, 3, 4, 6, 7 rely on these exact names.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_extraction.py`:

```python
from pipeline.extraction.extraction import (
    ExtractionResult, Notation, ProofSketch, parse_extraction,
)


def test_v1_payload_still_validates():
    er = parse_extraction({
        "concepts": [{"name": "Brownian motion", "kind": "concept"}],
        "definitions": [{"term": "martingale", "statement": "A process such that..."}],
        "results": [{"kind": "theorem", "statement": "Every $L^2$ martingale converges."}],
    })
    assert er.notations == []
    assert er.results[0].proof is None
    assert er.results[0].proof_present is False
    assert er.results[0].statement_complete is True
    assert er.definitions[0].uses == []


def test_notation_and_proof_roundtrip():
    er = ExtractionResult(
        notations=[Notation(symbol_latex="$W_t$", meaning="standard Brownian motion",
                            concept="Brownian motion")],
        results=[{
            "kind": "theorem", "name": "9.7. Theorem.",
            "statement": "If $X_n \\to X$ a.s. and $|X_n| \\le Y$...",
            "proof": {"sketch": "Apply Fatou to $Y \\pm X_n$.", "technique": "Fatou's lemma"},
            "proof_present": True, "statement_complete": True,
        }],
    )
    dumped = er.model_dump()
    assert dumped["notations"][0]["symbol_latex"] == "$W_t$"
    back = ExtractionResult.model_validate(dumped)
    assert isinstance(back.results[0].proof, ProofSketch)
    assert back.results[0].proof.technique == "Fatou's lemma"


def test_notation_symbol_stripped():
    n = Notation(symbol_latex="  $\\mu$ ", meaning="a measure")
    assert n.symbol_latex == "$\\mu$"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_extraction.py -k "v1_payload or notation" -v`
Expected: FAIL — `ImportError: cannot import name 'Notation'`

- [ ] **Step 3: Implement the models**

In `pipeline/extraction/extraction.py`, after the `Concept` class add:

```python
class Notation(BaseModel):
    symbol_latex: str = Field(
        description="The symbol or abbreviation being introduced, rendered as LaTeX in $...$ "
        'when mathematical (e.g. "$W_t$", "$\\sigma(\\mathcal{C})$") or verbatim when textual '
        '(e.g. "a.e.", "DF"). Only symbols the text INTRODUCES here ("Let X denote...", '
        '"we write ... for ..."), never symbols merely used.'
    )
    meaning: str = Field(
        description="What the symbol denotes, in one short phrase. LaTeX for any math."
    )
    concept: str = Field(
        default="",
        description="If the symbol denotes a concept you extracted in this same response, "
        "its exact name (e.g. \"Brownian motion\" for $W_t$). Empty otherwise.",
    )

    @field_validator("symbol_latex", "meaning")
    @classmethod
    def _strip(cls, v: str) -> str:
        return v.strip()


class ProofSketch(BaseModel):
    sketch: str = Field(
        description="2-4 sentence sketch of the proof: overall strategy and the key steps. "
        "NEVER a transcription of the proof text. LaTeX for any math."
    )
    technique: str = Field(
        default="",
        description='The main technique in a few words, e.g. "monotone-class argument", '
        '"Borel-Cantelli", "truncation + dominated convergence". Empty if unclear.',
    )
```

In `Definition`, after `defines` add:

```python
    uses: list[str] = Field(
        default_factory=list,
        description="Names of concepts (from this same response) that the definition's "
        "statement relies on. Leave empty if none or unsure.",
    )
```

In `Result`, replace the `depends_on` field description and add the three new fields after it:

```python
    depends_on: list[str] = Field(
        default_factory=list,
        description='Labels of OTHER results this result depends on or is proved from, as '
        'printed in the text, e.g. ["Lemma 2.4", "Theorem 6.13"]. The referenced result may '
        "be anywhere in the source — earlier or later chapters included; you do NOT need to "
        "have extracted it. Leave empty if none.",
    )
    proof: ProofSketch | None = Field(
        default=None,
        description="If the proof (or its beginning) is visible in this chunk, a short "
        "sketch of it. null when no proof text is visible.",
    )
    proof_present: bool = Field(
        default=False,
        description="true iff proof text for THIS result appears in this chunk.",
    )
    statement_complete: bool = Field(
        default=True,
        description="false iff the statement is cut off by the end of the chunk and you "
        "could only extract part of it.",
    )
```

In `ExtractionResult`, after `results` add:

```python
    notations: list[Notation] = Field(
        default_factory=list,
        description="Symbols and abbreviations INTRODUCED in the chunk (not merely used).",
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_extraction.py -v`
Expected: all PASS (old tests too — fields are additive)

- [ ] **Step 5: Commit**

```bash
git add pipeline/extraction/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): Notation + ProofSketch models, statement flags, unrestricted depends_on"
```

---

### Task 2: merge_results v2 — complete statements win, notations dedup

**Files:**
- Modify: `pipeline/extraction/extraction.py` (`merge_results`, lines ~199–235)
- Test: `tests/test_extraction.py` (append)

**Interfaces:**
- Consumes: Task 1 models.
- Produces: `merge_results(parts: list[ExtractionResult]) -> ExtractionResult` — same signature; now (a) collapses same-label results preferring `statement_complete=True` then longer statement, carrying over `uses`/`depends_on`/`proof`; (b) dedups `notations` case-insensitively on `symbol_latex`, first non-empty `concept` wins.

- [ ] **Step 1: Write the failing tests**

```python
from pipeline.extraction.extraction import merge_results


def _er(**kw):
    return ExtractionResult(**kw)


def test_merge_prefers_complete_statement_on_same_label():
    truncated = _er(results=[{"kind": "lemma", "name": "3.4. Composition Lemma.",
                              "statement": "Composition Lemma.",
                              "statement_complete": False, "depends_on": ["Lemma 3.3"]}])
    full = _er(results=[{"kind": "lemma", "name": "3.4. Composition Lemma.",
                         "statement": "If $f$ is measurable and $g$ is Borel, then "
                                      "$g \\circ f$ is measurable.",
                         "statement_complete": True,
                         "proof": {"sketch": "Preimages compose.", "technique": ""}}])
    merged = merge_results([truncated, full])
    assert len(merged.results) == 1
    r = merged.results[0]
    assert r.statement_complete is True
    assert "Borel" in r.statement
    assert r.depends_on == ["Lemma 3.3"]      # carried from the discarded variant
    assert r.proof is not None                # carried from the kept variant


def test_merge_keeps_distinct_unlabeled_results():
    a = _er(results=[{"kind": "theorem", "statement": "Statement one."}])
    b = _er(results=[{"kind": "theorem", "statement": "Statement two."}])
    assert len(merge_results([a, b]).results) == 2


def test_merge_dedups_notations_case_insensitive():
    a = _er(notations=[{"symbol_latex": "$W_t$", "meaning": "Brownian motion"}])
    b = _er(notations=[{"symbol_latex": "$w_T$", "meaning": "Brownian motion",
                        "concept": "Brownian motion"}])
    merged = merge_results([a, b])
    assert len(merged.notations) == 1
    assert merged.notations[0].concept == "Brownian motion"  # non-empty concept adopted
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_extraction.py -k merge -v`
Expected: `test_merge_prefers_complete_statement_on_same_label` FAILS with 2 results (different normalized statements don't dedup today); notation test fails with `AttributeError` or 2 notations.

- [ ] **Step 3: Implement**

Replace the results section of `merge_results` and add notation handling. Full new function body (replacing lines 199-235):

```python
def _better_statement(a: Result, b: Result) -> Result:
    """Between two same-label variants, prefer complete, then longer statement."""
    if a.statement_complete != b.statement_complete:
        return a if a.statement_complete else b
    return a if len(a.statement) >= len(b.statement) else b


def merge_results(parts: list[ExtractionResult]) -> ExtractionResult:
    # Chunks overlap, so the same concept/definition/result is extracted from adjacent chunks.
    # Dedup all three by the same normalized key graph_write uses for ids, so overlap doesn't
    # mint duplicate nodes.
    seen_c, concepts = set(), []
    for p in parts:
        for c in p.concepts:
            if _is_notation_only(c.name):
                continue  # bare notation is never a concept (backstop; primary fix is the prompt)
            if c.name.lower() not in seen_c:
                seen_c.add(c.name.lower())
                concepts.append(c)
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
                _extend_unique(kept.uses, d.uses)
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
                if kept.proof is None:
                    kept.proof = r.proof
                kept.proof_present = kept.proof_present or r.proof_present

    # Second pass: a statement split across a chunk boundary yields a truncated variant and a
    # complete variant with the SAME printed label but different normalized statements. Collapse
    # by (kind, label), keeping the better statement and unioning reference lists.
    by_label: dict[tuple[str, str], Result] = {}
    collapsed: list[Result] = []
    for r in results:
        if not r.name:
            collapsed.append(r)
            continue
        k = (r.kind, r.name.strip().lower())
        kept = by_label.get(k)
        if kept is None:
            by_label[k] = r
            collapsed.append(r)
        else:
            winner = _better_statement(kept, r)
            loser = r if winner is kept else kept
            _extend_unique(winner.uses, loser.uses)
            _extend_unique(winner.depends_on, loser.depends_on)
            if winner.proof is None:
                winner.proof = loser.proof
            winner.proof_present = winner.proof_present or loser.proof_present
            if winner is not kept:
                by_label[k] = winner
                collapsed[collapsed.index(kept)] = winner
    results = collapsed

    seen_n: dict[str, Notation] = {}
    notations = []
    for p in parts:
        for n in p.notations:
            k = n.symbol_latex.lower()
            kept = seen_n.get(k)
            if kept is None:
                seen_n[k] = n
                notations.append(n)
            elif not kept.concept and n.concept:
                kept.concept = n.concept
    return ExtractionResult(concepts=concepts, definitions=definitions,
                            results=results, notations=notations)
```

- [ ] **Step 4: Run the full test file**

Run: `uv run pytest tests/test_extraction.py tests/test_book_extraction.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add pipeline/extraction/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): merge collapses same-label truncated statements; notation dedup"
```

---

### Task 3: SYSTEM_PROMPT v2 — book-aware, notation routing, exemplars

**Files:**
- Modify: `pipeline/extraction/extraction.py` (`SYSTEM_PROMPT`, lines ~115–140)
- Test: `tests/test_extraction.py` (append)

**Interfaces:**
- Produces: rewritten `SYSTEM_PROMPT` string constant (same name; both provider paths pick it up automatically).

- [ ] **Step 1: Write the failing tests**

```python
from pipeline.extraction.extraction import SYSTEM_PROMPT


def test_prompt_routes_notation_not_concepts():
    assert "notations" in SYSTEM_PROMPT
    assert "never leave raw Unicode math" in SYSTEM_PROMPT.lower() or \
           "never leave raw unicode math" in SYSTEM_PROMPT.lower()
    # old absolute ban must be gone (replaced by routing)
    assert "Bare mathematical notation is never a concept" not in SYSTEM_PROMPT


def test_prompt_has_statement_and_frontmatter_rules():
    assert "never copy the heading" in SYSTEM_PROMPT.lower() or \
           "never echo the heading" in SYSTEM_PROMPT.lower()
    assert "statement_complete" in SYSTEM_PROMPT
    assert "table of contents" in SYSTEM_PROMPT.lower()


def test_prompt_exceeds_opus_cache_minimum():
    # 4096 tokens ≈ 16.4k chars; require headroom so edits can't silently drop below.
    assert len(SYSTEM_PROMPT) > 17000, len(SYSTEM_PROMPT)


def test_prompt_contains_exemplars():
    assert SYSTEM_PROMPT.count("EXAMPLE INPUT") >= 2
    assert SYSTEM_PROMPT.count("EXAMPLE OUTPUT") >= 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_extraction.py -k prompt -v`
Expected: FAIL on all four

- [ ] **Step 3: Replace SYSTEM_PROMPT**

Replace the existing `SYSTEM_PROMPT = """..."""` with the version below. The rules section is the contract; the exemplars are realistic mathematical-text chunks with complete expected JSON. (Exemplar sources are written in the style of a measure-theoretic probability text but are original phrasings — no verbatim book text.)

```python
SYSTEM_PROMPT = """You are an information-extraction assistant for STEM research papers and \
mathematical books (most often rooted in mathematics, statistics, or AI / machine learning, \
but spanning the sciences and engineering broadly). From the chunk, populate the concepts, \
definitions, results, and notations of the response schema, following each field's \
description. Emit nothing not asserted by the text. When filling a definition's `defines` or \
`uses`, a result's `uses`, or a notation's `concept`, reference ONLY concept names you have \
already produced in this same response; if unsure, leave it empty. A result's `depends_on` is \
different: it holds printed labels of other results (e.g. "Lemma 2.4") and may reference \
results ANYWHERE in the source, including ones you have not seen.

Rules that govern every field:

1. ROUTING. Named ideas/objects/frameworks/algorithms (glossary headwords) go in `concepts`. \
Symbols and abbreviations go in `notations`, never in `concepts`: from "Let $W_t$ be a \
standard Brownian motion", emit concept "Brownian motion" AND notation {symbol_latex: \
"$W_t$", meaning: "standard Brownian motion", concept: "Brownian motion"}. Only record a \
notation where the text INTRODUCES it ("Let X denote...", "we write ... for ...", glossary \
lines like "a.e.: almost everywhere") — not where a symbol is merely used.

2. LATEX. Render ALL mathematical notation as LaTeX — inline in $...$, display in $$...$$ — \
actively converting Unicode or plaintext math. Source text "ũ(x,t) = (σ²/2) ∇ ln ρ̃(x,t)" \
must be written as $\\tilde u(x,t) = \\tfrac{\\sigma^2}{2}\\,\\nabla \\ln \\tilde\\rho(x,t)$. \
Never leave raw Unicode math in any field.

3. STATEMENTS. A result's `statement` is the mathematical content that FOLLOWS the printed \
heading. Never copy the heading or label into `statement` — "3.4. Composition Lemma." is a \
`name`, not a statement. If the statement body is cut off by the end of the chunk, extract \
the visible part verbatim-faithfully and set statement_complete=false; a later chunk will \
carry the rest.

4. PROOFS. When proof text for a result is visible in the chunk (even partially), set \
proof_present=true on that result and fill `proof` with a 2-4 sentence sketch: the overall \
strategy and key steps, naming the main technique. NEVER transcribe the proof. If the chunk \
shows a proof of a result whose statement is NOT in this chunk, emit the result with its \
printed label in `name`, statement_complete=false, an empty or best-effort `statement`, and \
the proof fields filled.

5. SKIP NON-CONTENT. Emit nothing from a table of contents, index, copyright page, or list \
of references: no concepts, no definitions, no results, no notations. (A notation guide / \
list of symbols IS content: extract its entries as notations.)

EXAMPLE INPUT (theorem with visible proof):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 4: Integration, Section 4.2 Convergence \
theorems.

4.5. THEOREM (Monotone convergence). Let (f_n) be a sequence of non-negative measurable \
functions with f_n ↑ f pointwise. Then μ(f_n) ↑ μ(f) ≤ ∞.

Proof. Since f_n ≤ f_{n+1} ≤ f, the sequence μ(f_n) is non-decreasing and bounded above by \
μ(f), so the limit L := lim μ(f_n) exists in [0,∞] and L ≤ μ(f). For the reverse inequality \
fix a simple function s ≤ f and c ∈ (0,1), and set E_n := {x : f_n(x) ≥ c s(x)}. The sets E_n \
increase to the whole space, whence μ(f_n) ≥ c μ(s 1_{E_n}) → c μ(s) by continuity of the \
integral of simple functions along increasing sets. Letting c ↑ 1 and taking the supremum \
over simple s ≤ f gives L ≥ μ(f). Recall f_n ↑ f means f_n(x) is non-decreasing in n for \
every x with limit f(x).
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "Monotone convergence theorem", "kind": "concept"},
              {"name": "measurable function", "kind": "concept"},
              {"name": "simple function", "kind": "concept"}],
 "definitions": [],
 "results": [{"name": "4.5. THEOREM (Monotone convergence).", "kind": "theorem",
   "statement": "Let $(f_n)$ be a sequence of non-negative measurable functions with $f_n \\uparrow f$ pointwise. Then $\\mu(f_n) \\uparrow \\mu(f) \\le \\infty$.",
   "uses": ["measurable function"], "depends_on": [],
   "proof": {"sketch": "Monotonicity gives the limit $L \\le \\mu(f)$ at once. For the reverse inequality, fix a simple $s \\le f$ and $c \\in (0,1)$; on the increasing sets $E_n = \\{f_n \\ge c s\\}$ the integral inequality $\\mu(f_n) \\ge c\\,\\mu(s 1_{E_n})$ passes to the limit, and letting $c \\uparrow 1$ then taking the supremum over simple $s \\le f$ yields $L \\ge \\mu(f)$.",
             "technique": "approximation by simple functions"},
   "proof_present": true, "statement_complete": true}],
 "notations": []}

EXAMPLE INPUT (notation introduction + definition):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 2: Measure spaces, Section 2.1 \
σ-algebras.

We write σ(C) for the smallest σ-algebra containing a class C of subsets of Ω, and call it \
the σ-algebra generated by C. Throughout, "a.e." abbreviates "almost everywhere": a property \
holds a.e. if the set where it fails is null.

2.3. DEFINITION. Borel σ-algebra. Let (S, τ) be a topological space. The Borel σ-algebra \
B(S) is σ(τ), the σ-algebra generated by the open sets. Elements of B(S) are Borel sets.
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "σ-algebra generated by a class", "kind": "concept"},
              {"name": "Borel σ-algebra", "kind": "concept"}],
 "definitions": [{"term": "Borel $\\sigma$-algebra", "name": "2.3. DEFINITION.",
   "statement": "Let $(S, \\tau)$ be a topological space. The Borel $\\sigma$-algebra $\\mathcal{B}(S)$ is $\\sigma(\\tau)$, the $\\sigma$-algebra generated by the open sets. Elements of $\\mathcal{B}(S)$ are Borel sets.",
   "defines": ["Borel σ-algebra"], "uses": ["σ-algebra generated by a class"]}],
 "results": [],
 "notations": [{"symbol_latex": "$\\sigma(C)$",
                "meaning": "the smallest $\\sigma$-algebra containing the class $C$",
                "concept": "σ-algebra generated by a class"},
               {"symbol_latex": "a.e.", "meaning": "almost everywhere", "concept": ""}]}

EXAMPLE INPUT (statement cut off at chunk boundary; forward dependency):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 7: Martingales, Section 7.4 Convergence.

By Corollary 7.2 and the upcrossing bound of Lemma 7.9 below, we can now prove the main \
convergence result.

7.10. THEOREM (Martingale convergence). Let X be a supermartingale bounded in L^1, that is \
sup_n E|X_n| < ∞. Then X_∞ := lim X_n exists almost surely and
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "supermartingale", "kind": "concept"},
              {"name": "martingale convergence theorem", "kind": "concept"}],
 "definitions": [],
 "results": [{"name": "7.10. THEOREM (Martingale convergence).", "kind": "theorem",
   "statement": "Let $X$ be a supermartingale bounded in $L^1$, that is $\\sup_n E|X_n| < \\infty$. Then $X_\\infty := \\lim X_n$ exists almost surely and",
   "uses": ["supermartingale"], "depends_on": ["Corollary 7.2", "Lemma 7.9"],
   "proof": null, "proof_present": false, "statement_complete": false}],
 "notations": [{"symbol_latex": "$X_\\infty$",
                "meaning": "the almost-sure limit $\\lim X_n$ of the process",
                "concept": ""}]}"""  # noqa: RUF001
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_extraction.py -v`
Expected: all PASS. If `test_prompt_exceeds_opus_cache_minimum` fails on length, extend the rules prose (NOT filler — add a fourth exemplar of an exercises-section chunk following the same pattern) until > 17,000 chars.

- [ ] **Step 5: Commit**

```bash
git add pipeline/extraction/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): book-aware system prompt with notation routing + exemplars (cache-effective)"
```

---

### Task 4: Plumb notations + proof occurrences through the chapter pipeline

**Files:**
- Modify: `pipeline/books/extraction.py` (`attach_pages`, `chapter_payload`)
- Modify: `pipeline/assets/book_chapter_extraction.py` (per-chunk loop, payload assembly)
- Test: `tests/test_book_extraction.py` (append)

**Interfaces:**
- Consumes: Task 1/2 models and merge.
- Produces: chapter payload gains, per section dict: `"notations": [{symbol_latex, meaning, concept}]` and `"proof_chunks": [{"result_key": [kind, norm_statement], "label": str, "position": int}]`; each result dict gains `proof`, `proof_present`, `statement_complete` (via `model_dump()`, automatic). `attach_pages` new signature: `attach_pages(merged, chunk_extractions: list[tuple[ExtractionResult, int, int]]) -> tuple[list[dict], list[dict], list[dict]]` — tuples are `(er, page_start, chunk_position)`, third return is the proof-chunk rows. Tasks 6 and 7 consume these payload keys.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_book_extraction.py`:

```python
from pipeline.books.extraction import attach_pages, chapter_payload
from pipeline.extraction.extraction import ExtractionResult, merge_results


def test_attach_pages_returns_proof_chunk_rows():
    er1 = ExtractionResult(results=[{
        "kind": "theorem", "name": "9.7. Theorem.", "statement": "Full statement here.",
        "proof_present": True, "proof": {"sketch": "Sketchy.", "technique": "t"}}])
    er2 = ExtractionResult(results=[{
        "kind": "theorem", "name": "9.7. Theorem.", "statement": "Full statement here.",
        "proof_present": True}])
    merged = merge_results([er1, er2])
    defs, results, proof_rows = attach_pages(merged, [(er1, 101, 7), (er2, 102, 8)])
    assert results[0]["proof"]["sketch"] == "Sketchy."
    positions = sorted(pr["position"] for pr in proof_rows)
    assert positions == [7, 8]
    assert all(pr["label"] == "9.7. Theorem." for pr in proof_rows)


def test_chapter_payload_carries_section_notations():
    payload = chapter_payload(
        "title:test book", {"id": "title:test book:ch:1", "number": 1, "title": "Ch"},
        [{"section_id": "s1", "definitions": [], "results": [], "proof_chunks": [],
          "notations": [{"symbol_latex": "$\\mu$", "meaning": "a measure", "concept": ""}]}],
        [])
    assert payload["sections"][0]["notations"][0]["symbol_latex"] == "$\\mu$"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_extraction.py -v`
Expected: FAIL — `attach_pages` takes 2-tuples and returns 2 values.

- [ ] **Step 3: Implement `attach_pages` v2 in `pipeline/books/extraction.py`**

Replace `attach_pages` with:

```python
def attach_pages(merged: ExtractionResult,
                 chunk_extractions: list[tuple[ExtractionResult, int, int]],
                 ) -> tuple[list[dict], list[dict], list[dict]]:
    """Attach first-seen page to each merged definition/result; collect per-chunk proof
    locations. chunk_extractions tuples are (extraction, page_start, chunk_position)."""
    def_pages: dict[str, int] = {}
    res_pages: dict[tuple[str, str], int] = {}
    proof_rows: list[dict] = []
    for er, page, position in chunk_extractions:
        for d in er.definitions:
            def_pages.setdefault(normalize_statement(d.statement), page)
        for r in er.results:
            key = (r.kind, normalize_statement(r.statement))
            res_pages.setdefault(key, page)
            if r.proof_present:
                proof_rows.append({"result_key": list(key), "label": r.name,
                                   "position": position})
    defs = [{**d.model_dump(), "page": def_pages.get(normalize_statement(d.statement))}
            for d in merged.definitions]
    results = [{**r.model_dump(),
                "page": res_pages.get((r.kind, normalize_statement(r.statement)))}
               for r in merged.results]
    return defs, results, proof_rows
```

(`chapter_payload` needs no change — sections flow through as dicts; the asset adds the new keys.)

- [ ] **Step 4: Update the asset loop in `pipeline/assets/book_chapter_extraction.py`**

Replace the per-chunk loop and section assembly (the `try:` block body) with:

```python
        for i, row in enumerate(chunks):
            section = sections_by_id[row["section_id"]]
            t0 = time.monotonic()
            er = extract_one(chunk_with_context(meta.get("title") or meta["book_id"],
                                                chapter, section, row["text"]))
            context.log.info(
                f"extraction: chunk {i + 1}/{n} done in {time.monotonic() - t0:.1f}s")
            per_section.setdefault(row["section_id"], []).append(
                (er, row["page_start"], row["position"]))

        section_outputs, section_merges = [], []
        for sec_id, triples in per_section.items():
            merged = merge_results([er for er, _, _ in triples])
            section_merges.append(merged)
            defs, results, proof_rows = attach_pages(merged, triples)
            section_outputs.append({"section_id": sec_id,
                                    "definitions": defs, "results": results,
                                    "proof_chunks": proof_rows,
                                    "notations": [nt.model_dump()
                                                  for nt in merged.notations]})
```

(`book_chapter_resolved.passthrough_payload` spreads `{**payload}` so the new keys flow through untouched — verify by reading `pipeline/assets/book_chapter_resolved.py:24-27`, no change needed there.)

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_book_extraction.py tests/test_book_resolved.py -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/books/extraction.py pipeline/assets/book_chapter_extraction.py tests/test_book_extraction.py
git commit -m "feat(books): carry notations and proof-chunk locations through chapter payloads"
```

---

### Task 5: Chapter roles — classify at structure time, gate partitions

**Files:**
- Create: `pipeline/books/roles.py`
- Modify: `pipeline/assets/book_structure.py` (role assignment + partition gating)
- Modify: `pipeline/books/outline.py` (`structure_artifact` gains `role`)
- Modify: `pipeline/books/write.py` (`chapter_rows` + `WRITE_CHAPTERS` carry `role`)
- Test: Create `tests/test_book_roles.py`

**Interfaces:**
- Produces: `classify_roles(chapters: list[dict]) -> dict[int, str | None]` (number → role, `None` = ambiguous); `EXTRACT_ROLES = frozenset({"content", "notation_guide", "exercises"})`; `resolve_ambiguous(client, model, pending: list[dict], timeout: float) -> dict[int, str]`. `structure_artifact(book_id, sha, chapters, roles: dict[int, str])` — new fourth parameter; each chapter dict gains `"role"`. Task 8's runbook and the sensor gating rely on `EXTRACT_ROLES`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_book_roles.py`:

```python
from pipeline.books.roles import EXTRACT_ROLES, classify_roles

WILLIAMS = [
    {"number": 1, "title": "Title", "page_start": 1, "page_end": 1},
    {"number": 2, "title": "Copyright", "page_start": 2, "page_end": 2},
    {"number": 3, "title": "Contents", "page_start": 3, "page_end": 8},
    {"number": 4, "title": "Preface", "page_start": 9, "page_end": 10},
    {"number": 5, "title": "A Question of Terminology", "page_start": 11, "page_end": 11},
    {"number": 6, "title": "A Guide to Notation", "page_start": 12, "page_end": 14},
    {"number": 7, "title": "0 A Branching-Process Example", "page_start": 15, "page_end": 27},
    {"number": 8, "title": "Part A:  Foundations", "page_start": 28, "page_end": 96},
    {"number": 9, "title": "Part B: Martingale Theory", "page_start": 97, "page_end": 185},
    {"number": 10, "title": "Part C: Characteristic Functions", "page_start": 186, "page_end": 205},
    {"number": 11, "title": "Appendices", "page_start": 206, "page_end": 237},
    {"number": 12, "title": "E Exercises", "page_start": 238, "page_end": 256},
    {"number": 13, "title": "References", "page_start": 257, "page_end": 259},
    {"number": 14, "title": "Index", "page_start": 260, "page_end": 265},
]


def test_williams_roles():
    roles = classify_roles(WILLIAMS)
    assert roles[1] == "front_matter"       # Title
    assert roles[2] == "front_matter"       # Copyright
    assert roles[3] == "front_matter"       # Contents
    assert roles[4] == "front_matter"       # Preface
    assert roles[6] == "notation_guide"     # A Guide to Notation
    assert roles[8] == "content"            # Part A
    assert roles[11] == "content"           # Appendices hold real math
    assert roles[12] == "exercises"
    assert roles[13] == "back_matter"       # References
    assert roles[14] == "back_matter"       # Index


def test_unmatched_multi_page_defaults_to_none_then_content_is_safe():
    roles = classify_roles([{"number": 1, "title": "Interlude", "page_start": 1,
                             "page_end": 30}])
    # Unrecognized titles are ambiguous (None) — the asset resolves via LLM or
    # defaults to content, never silently skips.
    assert roles[1] is None


def test_extract_roles_set():
    assert EXTRACT_ROLES == frozenset({"content", "notation_guide", "exercises"})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_roles.py -v`
Expected: FAIL — `ModuleNotFoundError: pipeline.books.roles`

- [ ] **Step 3: Create `pipeline/books/roles.py`**

```python
"""Chapter role classification: heuristics first, one batched LLM tie-break for the
ambiguous residue (spec 2026-07-12 §3). Roles gate which chapters get extraction
partitions; misclassifying as content only wastes a few LLM calls, so every fallback
lands on content."""
from __future__ import annotations

import json
import re

EXTRACT_ROLES = frozenset({"content", "notation_guide", "exercises"})
ALL_ROLES = EXTRACT_ROLES | {"front_matter", "back_matter"}

_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(notation|symbols|nomenclature)\b", re.I), "notation_guide"),
    (re.compile(r"\bexercises?\b", re.I), "exercises"),
    (re.compile(r"^(title|copyright|colophon|contents|table of contents|preface|foreword|"
                r"acknowledg\w*|dedication|about the author|a question of terminology)\b",
                re.I), "front_matter"),
    (re.compile(r"^(index|references|bibliography|glossary|list of (figures|tables))\b",
                re.I), "back_matter"),
]


def _heuristic(title: str, page_start: int, page_end: int) -> str | None:
    t = title.strip()
    for pat, role in _RULES:
        if pat.search(t):
            return role
    if page_end - page_start >= 4:
        return "content"  # multi-page, no front/back-matter signal: real content
    return None  # short + unrecognized: ambiguous


def classify_roles(chapters: list[dict]) -> dict[int, str | None]:
    """number -> role, or None where the heuristics can't decide."""
    return {ch["number"]: _heuristic(ch["title"], ch["page_start"], ch["page_end"])
            for ch in chapters}


_TIEBREAK_PROMPT = (
    "You are classifying book chapters by role. Given the full chapter list of a book and "
    "a subset that needs classification, answer with a JSON object mapping chapter number "
    "(as a string) to one of: content, notation_guide, exercises, front_matter, back_matter. "
    "content = real subject matter worth extracting. Answer with the JSON object only.\n\n"
    "Full chapter list:\n{listing}\n\nClassify these chapter numbers: {pending}"
)


def resolve_ambiguous(client, model: str, chapters: list[dict],
                      pending: list[int], timeout: float = 60.0) -> dict[int, str]:
    """One batched call for the ambiguous residue. Any failure -> content (safe default)."""
    listing = "\n".join(f'{c["number"]}: "{c["title"]}" '
                        f'(pages {c["page_start"]}-{c["page_end"]})' for c in chapters)
    try:
        resp = client.messages.create(
            model=model, max_tokens=1024, timeout=timeout,
            messages=[{"role": "user", "content": _TIEBREAK_PROMPT.format(
                listing=listing, pending=pending)}])
        text = next(b.text for b in resp.content if b.type == "text")
        raw = json.loads(text[text.index("{"):text.rindex("}") + 1])
        out = {}
        for n in pending:
            role = raw.get(str(n), "content")
            out[n] = role if role in ALL_ROLES else "content"
        return out
    except Exception:  # noqa: BLE001 — classification must never sink an ingestion
        return {n: "content" for n in pending}
```

Note the test for a 1-page unmatched title expects `None` — the example uses `page_end=30`? No: re-check. `"Interlude", 1..30` is multi-page → heuristic returns `content`, but the test expects `None`. **Fix the test, not the code**: change that test's chapter to `"page_start": 1, "page_end": 3` (short + unrecognized → `None`). The corrected test:

```python
def test_unmatched_short_chapter_is_ambiguous():
    roles = classify_roles([{"number": 1, "title": "Interlude", "page_start": 1,
                             "page_end": 3}])
    assert roles[1] is None
```

- [ ] **Step 4: Wire roles into `structure_artifact` (`pipeline/books/outline.py`)**

Change the signature and chapter dict:

```python
def structure_artifact(book_id: str, sha: str, chapters: list[ChapterNode],
                       roles: dict[int, str] | None = None) -> dict:
    out = {"book_id": book_id, "chapters": []}
    for ch in chapters:
        out["chapters"].append({
            "id": chapter_node_id(book_id, ch.number),
            "key": f"{sha}:ch{ch.number:02d}",
            "number": ch.number, "title": ch.title,
            "role": (roles or {}).get(ch.number, "content"),
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

- [ ] **Step 5: Gate partition registration in `pipeline/assets/book_structure.py`**

Replace the body after `chapters = build_structure(...)` (keep the QuarantineError handling) with:

```python
    roles = classify_roles([{"number": c.number, "title": c.title,
                             "page_start": c.page_start, "page_end": c.page_end}
                            for c in chapters])
    pending = [n for n, r in roles.items() if r is None]
    if pending:
        ar = context.resources.anthropic
        resolved = resolve_ambiguous(
            ar.get_client(), ar.summary_model,
            [{"number": c.number, "title": c.title, "page_start": c.page_start,
              "page_end": c.page_end} for c in chapters],
            pending, timeout=ar.request_timeout)
        roles.update(resolved)

    artifact = structure_artifact(meta["book_id"], key, chapters, roles)
    s3.put_object(Bucket=TRIAGE_BUCKET, Key=f"{key}.structure.json",
                  Body=json.dumps(artifact).encode("utf-8"))

    chapter_keys = [ch["key"] for ch in artifact["chapters"]
                    if ch["role"] in EXTRACT_ROLES]
    skipped = [f'{ch["number"]}:{ch["role"]}' for ch in artifact["chapters"]
               if ch["role"] not in EXTRACT_ROLES]
    context.instance.add_dynamic_partitions(BOOK_CHAPTERS_PARTITION, chapter_keys)
    return MaterializeResult(metadata={
        "book_id": meta["book_id"],
        "chapters": MetadataValue.int(len(artifact["chapters"])),
        "extracted_chapters": MetadataValue.int(len(chapter_keys)),
        "skipped_roles": MetadataValue.text(", ".join(skipped) or "none"),
        "sections": MetadataValue.int(sum(len(c["sections"]) for c in artifact["chapters"])),
        "chapter_partitions": MetadataValue.text(", ".join(chapter_keys)),
    })
```

Imports to add at top: `from pipeline.books.roles import EXTRACT_ROLES, classify_roles, resolve_ambiguous` and add `"anthropic"` to `required_resource_keys` on the `@asset` decorator.

- [ ] **Step 6: Persist role on the Chapter node (`pipeline/books/write.py`)**

In `chapter_rows`, add `"role": ch.get("role", "content"),` to the row dict. In `WRITE_CHAPTERS`, extend the SET clause:

```cypher
  SET ch.number = row.number, ch.title = row.title, ch.role = row.role,
      ch.page_start = row.page_start, ch.page_end = row.page_end
```

- [ ] **Step 7: Run tests**

Run: `uv run pytest tests/test_book_roles.py tests/test_book_outline.py tests/test_book_write.py -v`
Expected: PASS. If `test_book_outline.py` pins the old `structure_artifact` signature, update those call sites with `roles=None` semantics (the default keeps old behavior: everything `content`).

- [ ] **Step 8: Commit**

```bash
git add pipeline/books/roles.py pipeline/books/outline.py pipeline/books/write.py pipeline/assets/book_structure.py tests/
git commit -m "feat(books): chapter role classification gates extraction partitions"
```

---

### Task 6: Graph write — Notation nodes, Proof nodes, constraints

**Files:**
- Modify: `pipeline/books/identity.py` (add `notation_node_id`)
- Modify: `pipeline/books/write.py` (new Cypher + row builders)
- Modify: `pipeline/assets/book_chapter_graph_write.py` (write the new rows)
- Modify: `pipeline/graph/schema.py` (constraints)
- Test: `tests/test_book_write.py` (append)

**Interfaces:**
- Consumes: Task 4 payload keys (`sections[].notations`, `sections[].proof_chunks`, result dicts with `proof`).
- Produces: `notation_node_id(book_id: str, symbol_latex: str) -> str` (= `f"{book_id}:not:{_hash12(norm_symbol)}"`); `WRITE_BOOK_NOTATIONS`, `WRITE_BOOK_PROOFS`, `WRITE_DEF_USES` Cypher constants; `book_notation_rows(book_id, section_id, notations, surface_to_canon) -> list[dict]`; `book_proof_rows(owner, section_id, results) -> list[dict]`. Constraints `notation_id`, `proof_id` in `schema.py`. Task 7 links `PROVED_IN`; this task writes `HAS_PROOF` + notations.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_book_write.py`:

```python
from pipeline.books.identity import notation_node_id
from pipeline.books.write import book_notation_rows, book_proof_rows


def test_notation_id_is_per_book_and_symbol_normalized():
    a = notation_node_id("title:probability with martingales", "$W_t$")
    b = notation_node_id("title:probability with martingales", "$w_T$")
    c = notation_node_id("title:another book", "$W_t$")
    assert a == b          # case/whitespace-insensitive within a book
    assert a != c          # never collides across books
    assert a.split(":not:")[0] == "title:probability with martingales"


def test_book_notation_rows_resolve_concept_via_canon_map():
    rows = book_notation_rows(
        "title:b", "sec1",
        [{"symbol_latex": "$W_t$", "meaning": "Brownian motion", "concept": "brownian motion"},
         {"symbol_latex": "a.e.", "meaning": "almost everywhere", "concept": ""}],
        {"brownian motion": "Brownian motion"})
    assert rows[0]["concept"] == "Brownian motion"
    assert rows[1]["concept"] is None
    assert rows[0]["section_id"] == "sec1"


def test_book_proof_rows_only_for_results_with_sketch():
    results = [
        {"kind": "theorem", "statement": "S1", "proof": {"sketch": "sk", "technique": "t"}},
        {"kind": "lemma", "statement": "S2", "proof": None},
    ]
    rows = book_proof_rows("ch1", "sec1", results)
    assert len(rows) == 1
    assert rows[0]["sketch"] == "sk"
    assert rows[0]["id"].endswith(":proof")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_book_write.py -v`
Expected: FAIL — ImportError

- [ ] **Step 3: Implement**

`pipeline/books/identity.py` — add (mirroring the existing id helpers there; reuse the module's existing hashing helper if one exists, else):

```python
import hashlib
import re


def _norm_symbol(symbol_latex: str) -> str:
    return re.sub(r"\s+", "", symbol_latex.strip().lower())


def notation_node_id(book_id: str, symbol_latex: str) -> str:
    h = hashlib.sha256(_norm_symbol(symbol_latex).encode()).hexdigest()[:12]
    return f"{book_id}:not:{h}"
```

`pipeline/books/write.py` — add:

```python
WRITE_BOOK_NOTATIONS = """
UNWIND $rows AS row
  MATCH (s:Section {id: row.section_id})
  MERGE (n:Notation {id: row.id})
  SET n.symbol_latex = row.symbol_latex, n.meaning = row.meaning
  MERGE (n)-[:INTRODUCED_IN]->(s)
  FOREACH (_ IN CASE WHEN row.concept IS NULL THEN [] ELSE [1] END |
    MERGE (c:Concept {name: row.concept})
    MERGE (n)-[:DENOTES]->(c))
"""

WRITE_BOOK_PROOFS = """
UNWIND $rows AS row
  MATCH (r:Result {id: row.result_id})
  MERGE (p:Proof {id: row.id})
  SET p.sketch = row.sketch, p.technique = row.technique
  MERGE (r)-[:HAS_PROOF]->(p)
"""

WRITE_DEF_USES = """
UNWIND $rows AS row
  MATCH (d:Definition {id: row.def_id})
  MATCH (c:Concept {name: row.concept})
  MERGE (d)-[:USES]->(c)
"""


def book_notation_rows(book_id: str, section_id: str, notations: list[dict],
                       surface_to_canon: dict[str, str]) -> list[dict]:
    from pipeline.books.identity import notation_node_id
    return [{"id": notation_node_id(book_id, n["symbol_latex"]),
             "symbol_latex": n["symbol_latex"], "meaning": n["meaning"],
             "concept": surface_to_canon.get(n.get("concept", "").lower()) or None,
             "section_id": section_id} for n in notations]


def book_proof_rows(owner: str, section_id: str, results: list[dict]) -> list[dict]:
    rows = []
    for r in results:
        pr = r.get("proof")
        if not pr:
            continue
        rid = result_id(owner, r["kind"], r["statement"])
        rows.append({"id": f"{rid}:proof", "result_id": rid,
                     "sketch": pr["sketch"], "technique": pr.get("technique", "")})
    return rows


def def_uses_rows(owner: str, definitions: list[dict],
                  surface_to_canon: dict[str, str]) -> list[dict]:
    rows = []
    for d in definitions:
        did = def_id(owner, d["statement"])
        for name in d.get("uses", []):
            canon = surface_to_canon.get(name.lower())
            if canon:
                rows.append({"def_id": did, "concept": canon})
    return rows
```

`pipeline/assets/book_chapter_graph_write.py` — in the per-section loop add:

```python
        nrows_all = getattr(book_chapter_graph_write, "_nrows", None)  # (delete this line — see below)
```

Concretely: accumulate `nrows` and `prows` and `du_rows` alongside the existing lists:

```python
    nrows, prows, du_rows = [], [], []
    for sec in payload.get("sections", []):
        sid = sec["section_id"]
        # ... existing drows/rrows/edges lines stay ...
        nrows.extend(book_notation_rows(book_id, sid, sec.get("notations", []),
                                        surface_to_canon))
        prows.extend(book_proof_rows(owner, sid, sec.get("results", [])))
        du_rows.extend(def_uses_rows(owner, sec.get("definitions", []), surface_to_canon))
```

and inside the Neo4j session, after `s.run(WRITE_RESULT_DEPENDS, rows=dep_edges)`:

```python
        s.run(WRITE_BOOK_NOTATIONS, rows=nrows)
        s.run(WRITE_BOOK_PROOFS, rows=prows)
        s.run(WRITE_DEF_USES, rows=du_rows)
```

Extend imports from `pipeline.books.write` accordingly, and add to the returned metadata: `"notations": MetadataValue.int(len(nrows)), "proofs": MetadataValue.int(len(prows)),`.

`pipeline/graph/schema.py` — alongside the existing constraint statements add:

```python
CREATE CONSTRAINT notation_id IF NOT EXISTS
FOR (n:Notation) REQUIRE n.id IS UNIQUE
```

```python
CREATE CONSTRAINT proof_id IF NOT EXISTS
FOR (p:Proof) REQUIRE p.id IS UNIQUE
```

(match the file's exact existing formatting for constraint blocks — see `chapter_id`/`section_id` added at lines 159-163.)

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_book_write.py tests/test_cypher.py -v`
Expected: PASS (if `test_cypher.py` validates Cypher constants by parsing, the new constants get covered automatically; if it enumerates constants, add the three new names).

- [ ] **Step 5: Commit**

```bash
git add pipeline/books/identity.py pipeline/books/write.py pipeline/assets/book_chapter_graph_write.py pipeline/graph/schema.py tests/test_book_write.py
git commit -m "feat(graph): Notation and Proof nodes, DEF-USES edges, uniqueness constraints"
```

---

### Task 7: Label normalizer + book_link_resolution asset + sensor + job

**Files:**
- Create: `pipeline/books/labels.py`
- Create: `pipeline/assets/book_link_resolution.py`
- Modify: `pipeline/runtime/jobs.py` (new job `resolve_book_links`)
- Modify: `pipeline/runtime/sensors.py` (new `book_links_sensor`)
- Modify: `pipeline/definitions.py` (register asset, job, sensor)
- Modify: `pipeline/assets/__init__.py` (export the new asset module, matching how siblings are exported)
- Test: Create `tests/test_book_labels.py`; append to `tests/test_book_write.py` if needed

**Interfaces:**
- Consumes: Neo4j Results written by Task 6 (`id`, `name`, `kind` per book via id prefix); MinIO payloads `{sha}:chNN.resolved.json` with Task 4's `proof_chunks` + result `depends_on`.
- Produces: `parse_label(s: str) -> tuple[str | None, str | None, str]` (kind, numeric tag, normalized phrase); `build_label_index(rows: list[dict]) -> LabelIndex`; `LabelIndex.resolve(ref: str) -> str | None` (result id or None; only unique matches). Asset `book_link_resolution` (books partition). Job `resolve_book_links`. Sensor `book_links_sensor`.

- [ ] **Step 1: Write the failing normalizer tests**

Create `tests/test_book_labels.py`:

```python
from pipeline.books.labels import LabelIndex, build_label_index, parse_label


def test_parse_label_extracts_kind_and_tag():
    assert parse_label("Lemma 9.6") == ("lemma", "9.6", "")
    assert parse_label("9.6. Lemma.") == ("lemma", "9.6", "")
    assert parse_label("Theorem 3.12. Skorokod representation of a random variable "
                       "with prescribed distribution function.") == (
        "theorem", "3.12",
        "skorokod representation of a random variable with prescribed distribution function")
    assert parse_label("5.3. MON") == (None, "5.3", "mon")
    assert parse_label("the Monotone-Convergence Theorem") == (
        "theorem", None, "the monotone convergence")


NODES = [
    {"id": "b:ch9:lemma:aaa", "name": "9.6. Lemma.", "kind": "lemma"},
    {"id": "b:ch9:theorem:bbb", "name": "9.7. Theorem. Dominated-Convergence Theorem",
     "kind": "theorem"},
    {"id": "b:ch5:theorem:ccc", "name": "5.3. MON", "kind": "theorem"},
    {"id": "b:ch5:theorem:ddd", "name": "5.3. MON", "kind": "theorem"},  # dup label
]


def test_index_resolves_by_kind_and_tag():
    idx = build_label_index(NODES)
    assert idx.resolve("Lemma 9.6") == "b:ch9:lemma:aaa"
    assert idx.resolve("Theorem 9.7") == "b:ch9:theorem:bbb"


def test_index_resolves_named_theorem_phrase():
    idx = build_label_index(NODES)
    assert idx.resolve("Dominated-Convergence Theorem") == "b:ch9:theorem:bbb"


def test_index_refuses_ambiguous():
    idx = build_label_index(NODES)
    assert idx.resolve("5.3. MON") is None      # two nodes share the tag — never guess
    assert idx.resolve("Lemma 99.9") is None    # no match
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_book_labels.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Implement `pipeline/books/labels.py`**

```python
"""Deterministic result-label normalization for the post-extraction linking pass.
Handles both directions of the format mismatch: models write "Lemma 9.6", nodes are
named "9.6. Lemma."; prose references name theorems ("the Monotone-Convergence
Theorem"). Only unique matches resolve — ambiguity returns None (spec §5)."""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

_KINDS = {"theorem": "theorem", "thm": "theorem", "lemma": "lemma", "lem": "lemma",
          "proposition": "proposition", "prop": "proposition",
          "corollary": "corollary", "cor": "corollary"}
_TAG = re.compile(r"\b(\d+(?:\.\d+)+|\d+)\b")
_KIND = re.compile("|".join(_KINDS), re.I)
_WORD = re.compile(r"[a-z]+")


def parse_label(s: str) -> tuple[str | None, str | None, str]:
    """(kind, numeric tag, normalized residual phrase). Any part may be missing."""
    low = s.lower()
    km = _KIND.search(low)
    kind = _KINDS[km.group(0)] if km else None
    tm = _TAG.search(low)
    tag = tm.group(1) if tm else None
    residue = low
    if km:
        residue = residue.replace(km.group(0), " ", 1)
    if tm:
        residue = residue.replace(tm.group(1), " ", 1)
    phrase = " ".join(_WORD.findall(residue))
    return kind, tag, phrase


@dataclass
class LabelIndex:
    by_kind_tag: dict[tuple[str, str], list[str]] = field(default_factory=lambda: defaultdict(list))
    by_tag: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    by_phrase: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def resolve(self, ref: str) -> str | None:
        kind, tag, phrase = parse_label(ref)
        candidates: list[str] = []
        if kind and tag:
            candidates = self.by_kind_tag.get((kind, tag), [])
        if not candidates and tag:
            candidates = self.by_tag.get(tag, [])
        if not candidates and phrase and len(phrase.split()) >= 2:
            candidates = self.by_phrase.get(phrase, [])
        return candidates[0] if len(candidates) == 1 else None


def build_label_index(rows: list[dict]) -> LabelIndex:
    idx = LabelIndex()
    for row in rows:
        if not row.get("name"):
            continue
        kind, tag, phrase = parse_label(row["name"])
        kind = kind or row.get("kind")
        if tag:
            idx.by_tag[tag].append(row["id"])
            if kind:
                idx.by_kind_tag[(kind, tag)].append(row["id"])
        if phrase and len(phrase.split()) >= 2:
            idx.by_phrase[phrase].append(row["id"])
    return idx
```

Check the phrase test: `parse_label("the Monotone-Convergence Theorem")` — hyphen splits into words `the monotone convergence` after removing kind word `theorem`. Node phrase for "9.7. Theorem. Dominated-Convergence Theorem" → tag 9.7 removed, kind removed once (first "theorem"), residue words `dominated convergence theorem`. Ref "Dominated-Convergence Theorem" → phrase `dominated convergence`. These differ (`theorem` residue on the node). To make phrase matching symmetric, strip ALL kind words from phrases in both `parse_label` callers: change the phrase line to:

```python
    phrase = " ".join(w for w in _WORD.findall(residue) if w not in _KINDS)
```

and in the test expectations drop kind words accordingly (`"the monotone convergence"` stays, since "theorem" is stripped). Adjust `test_parse_label_extracts_kind_and_tag`'s long-phrase expectation to the kind-stripped form.

- [ ] **Step 4: Run normalizer tests**

Run: `uv run pytest tests/test_book_labels.py -v`
Expected: PASS

- [ ] **Step 5: Create the asset `pipeline/assets/book_link_resolution.py`**

```python
"""book_link_resolution: per-book post-extraction linking pass (spec §5). Reads every
chapter payload from MinIO + the book's Result label index from Neo4j; resolves free-text
depends_on refs and proof locations into DEPENDS_ON / PROVED_IN edges. Deterministic
normalization first; one batched LLM call for the fuzzy residue; unmatched refs are
logged and dropped, never guessed."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.assets.graph_write import WRITE_RESULT_DEPENDS, result_id
from pipeline.books.labels import build_label_index
from pipeline.runtime.partitions import books_partitions_def
from pipeline.runtime.storage import EXTRACTED_BUCKET, TRIAGE_BUCKET

FETCH_BOOK_RESULTS = """
MATCH (b:Book {id: $book_id})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(r:Result)
RETURN r.id AS id, r.name AS name, r.kind AS kind
"""

WRITE_PROVED_IN = """
UNWIND $rows AS row
  MATCH (r:Result {id: row.result_id})
  MATCH (c:Chunk)-[:PART_OF]->(:Section {id: row.section_id})
  WHERE c.position = row.position
  MERGE (r)-[:PROVED_IN]->(c)
"""

_FUZZY_PROMPT = (
    "Match each unresolved reference to at most one result label from the list, by meaning. "
    "Answer with a JSON object mapping reference string to the EXACT label string, omitting "
    "references that match nothing or are ambiguous. JSON only.\n\n"
    "Result labels:\n{labels}\n\nUnresolved references:\n{refs}"
)


def _fuzzy_resolve(client, model: str, refs: list[str], label_to_id: dict[str, str],
                   timeout: float) -> dict[str, str]:
    if not refs:
        return {}
    try:
        resp = client.messages.create(
            model=model, max_tokens=2048, timeout=timeout,
            messages=[{"role": "user", "content": _FUZZY_PROMPT.format(
                labels="\n".join(label_to_id), refs="\n".join(refs))}])
        text = next(b.text for b in resp.content if b.type == "text")
        raw = json.loads(text[text.index("{"):text.rindex("}") + 1])
        return {ref: label_to_id[lbl] for ref, lbl in raw.items() if lbl in label_to_id}
    except Exception:  # noqa: BLE001 — linking is best-effort; never sink the run
        return {}


@asset(partitions_def=books_partitions_def(),
       required_resource_keys={"minio", "neo4j_new", "anthropic"})
def book_link_resolution(context) -> MaterializeResult:
    sha = context.partition_key
    s3 = context.resources.minio.get_client()
    structure = json.loads(
        s3.get_object(Bucket=TRIAGE_BUCKET, Key=f"{sha}.structure.json")["Body"].read())
    book_id = structure["book_id"]

    new = context.resources.neo4j_new
    dep_rows, proved_rows = [], []
    unresolved: list[tuple[str, str]] = []  # (res_id, ref)
    with new.get_driver() as driver, driver.session(database=new.database) as s:
        nodes = [dict(rec) for rec in s.run(FETCH_BOOK_RESULTS, book_id=book_id)]
        idx = build_label_index(nodes)
        label_to_id = {n["name"]: n["id"] for n in nodes if n["name"]}

        for ch in structure["chapters"]:
            key = ch["key"]
            try:
                payload = json.loads(s3.get_object(
                    Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json")["Body"].read())
            except Exception:  # noqa: BLE001 — chapter skipped by role: no payload
                continue
            owner = payload["chapter_id"]
            for sec in payload.get("sections", []):
                for r in sec.get("results", []):
                    rid = result_id(owner, r["kind"], r["statement"])
                    for ref in r.get("depends_on", []):
                        dep = idx.resolve(ref)
                        if dep and dep != rid:
                            dep_rows.append({"res_id": rid, "dep_id": dep})
                        elif not dep:
                            unresolved.append((rid, ref))
                for pr in sec.get("proof_chunks", []):
                    kind, norm = pr["result_key"]
                    proved_rows.append({
                        "result_id": f"{owner}:{kind}:" + result_id(owner, kind, "X").rsplit(":", 1)[-1]
                    })  # placeholder — replaced below

        # PROVED_IN rows need the same content-hash id the write path used. result_key is
        # (kind, normalized statement); result_id hashes the RAW statement, so recompute the
        # id by matching the section's merged results on the normalized key instead:
        proved_rows = []
        for ch in structure["chapters"]:
            key = ch["key"]
            try:
                payload = json.loads(s3.get_object(
                    Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json")["Body"].read())
            except Exception:  # noqa: BLE001
                continue
            owner = payload["chapter_id"]
            for sec in payload.get("sections", []):
                from pipeline.text_norm import normalize_statement
                by_key = {(r["kind"], normalize_statement(r["statement"])):
                          result_id(owner, r["kind"], r["statement"])
                          for r in sec.get("results", [])}
                for pr in sec.get("proof_chunks", []):
                    rid = by_key.get(tuple(pr["result_key"]))
                    if rid:
                        proved_rows.append({"result_id": rid,
                                            "section_id": sec["section_id"],
                                            "position": pr["position"]})

        # fuzzy residue — one batched call
        ar = context.resources.anthropic
        fuzzy = _fuzzy_resolve(ar.get_client(), ar.summary_model,
                               sorted({ref for _, ref in unresolved}),
                               label_to_id, timeout=ar.request_timeout)
        still_dropped = 0
        for rid, ref in unresolved:
            dep = fuzzy.get(ref)
            if dep and dep != rid:
                dep_rows.append({"res_id": rid, "dep_id": dep})
            else:
                still_dropped += 1
                context.log.info(f"link dropped: {ref!r} (no unique match)")

        s.run(WRITE_RESULT_DEPENDS, rows=dep_rows)
        s.run(WRITE_PROVED_IN, rows=proved_rows)

    return MaterializeResult(metadata={
        "depends_on_edges": MetadataValue.int(len(dep_rows)),
        "proved_in_edges": MetadataValue.int(len(proved_rows)),
        "dropped_refs": MetadataValue.int(still_dropped),
    })
```

**Clean-up requirement for the implementer:** the first `proved_rows` loop with the placeholder comment must NOT survive — write the function with the single correct second loop only (it is shown twice above purely to document why the id must be recomputed from raw statements; final code has one loop that builds `by_key` and appends). Move the `normalize_statement` import to the top of the file.

- [ ] **Step 6: Wire job + sensor + registration**

`pipeline/runtime/jobs.py` — append:

```python
from pipeline.assets import book_link_resolution  # noqa: E402

resolve_book_links = define_asset_job(
    name="resolve_book_links",
    selection=AssetSelection.assets(book_link_resolution.book_link_resolution),
    description="Post-extraction linking pass: DEPENDS_ON / PROVED_IN edges from free-text refs.",
)
```

`pipeline/runtime/sensors.py` — append:

```python
@sensor(job_name="resolve_book_links", minimum_interval_seconds=120)
def book_links_sensor(context: SensorEvaluationContext):
    instance = context.instance
    done_chapters = set(
        instance.get_materialized_partitions(AssetKey("book_chapter_graph_write")))
    linked = set(instance.get_materialized_partitions(AssetKey("book_link_resolution")))
    all_chapter_keys = instance.get_dynamic_partitions(BOOK_CHAPTERS_PARTITION)
    by_book: dict[str, set[str]] = {}
    for ck in all_chapter_keys:
        by_book.setdefault(ck.rpartition(":ch")[0], set()).add(ck)
    requests = []
    for sha, cks in by_book.items():
        if sha not in linked and cks and cks <= done_chapters:
            # run_key includes chapter count so a re-ingest with different chapters re-links
            requests.append(RunRequest(partition_key=sha, run_key=f"link:{sha}:{len(cks)}"))
    if not requests:
        return SkipReason("no books awaiting link resolution")
    return SensorResult(run_requests=requests)
```

`pipeline/definitions.py` — add `book_link_resolution` to the assets import + list, `resolve_book_links` to jobs import + list, `book_links_sensor` to sensors import + list. `pipeline/assets/__init__.py` — export `book_link_resolution` the same way sibling modules are exported.

- [ ] **Step 7: Definitions load test**

Run: `uv run pytest tests/test_definitions.py -v` (this test loads `Definitions`; it must pick up the new asset/job/sensor without error). Also: `uv run python -c "from pipeline.definitions import defs; print(len(defs.get_all_asset_specs()))"`
Expected: PASS / prints count including the new asset

- [ ] **Step 8: Run everything**

Run: `uv run pytest -q`
Expected: all PASS

- [ ] **Step 9: Commit**

```bash
git add pipeline/books/labels.py pipeline/assets/book_link_resolution.py pipeline/runtime/jobs.py pipeline/runtime/sensors.py pipeline/definitions.py pipeline/assets/__init__.py tests/test_book_labels.py
git commit -m "feat(books): post-extraction link resolution — DEPENDS_ON/PROVED_IN via label normalization"
```

---

### Task 8: Wipe script + migration runbook

**Files:**
- Create: `scripts/wipe_book.py`
- Create: `docs/runbooks/extraction-v2-migration.md`
- Test: `tests/test_wipe_book.py` (Cypher string sanity only — no live DB in unit tests)

**Interfaces:**
- Consumes: nothing from other tasks (pure ops).
- Produces: `scripts/wipe_book.py --book-id <id> [--dry-run]` CLI; runbook executed AFTER merge, immediately before the v2 re-run (per Osian: wipe current Williams data before the new run).

- [ ] **Step 1: Write the failing test**

Create `tests/test_wipe_book.py`:

```python
from scripts.wipe_book import DELETE_SUBTREE, DELETE_SCOPED_STATEMENTS, DELETE_ORPHAN_CONCEPTS


def test_wipe_cypher_is_scoped_to_book():
    assert "$book_id" in DELETE_SUBTREE
    assert "STARTS WITH $prefix" in DELETE_SCOPED_STATEMENTS
    # orphan cleanup must require zero remaining relationships — never touch shared concepts
    assert "NOT (c)--()" in DELETE_ORPHAN_CONCEPTS
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_wipe_book.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Implement `scripts/wipe_book.py`**

```python
"""Wipe one book's entire subtree from the Aura graph. Scoped deletes only:
- the Book node + Document, Chapters, Sections, Chunks (subtree)
- Definitions / Results / Proofs / Notations whose id carries the book prefix
- Concepts left with NO remaining relationships (shared concepts survive)

Usage:
    uv run python scripts/wipe_book.py --book-id "title:probability with martingales" [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from neo4j import GraphDatabase

DELETE_SUBTREE = """
MATCH (b:Book {id: $book_id})
OPTIONAL MATCH (b)-[:HAS_CHAPTER]->(ch:Chapter)
OPTIONAL MATCH (ch)-[:HAS_SECTION]->(s:Section)
OPTIONAL MATCH (c:Chunk)-[:PART_OF]->(s)
OPTIONAL MATCH (b)-[:HAS_DOCUMENT]->(d:Document)
DETACH DELETE c, s, ch, d, b
"""

DELETE_SCOPED_STATEMENTS = """
MATCH (n)
WHERE (n:Definition OR n:Result OR n:Proof OR n:Notation)
  AND n.id STARTS WITH $prefix
DETACH DELETE n
"""

DELETE_ORPHAN_CONCEPTS = """
MATCH (c:Concept) WHERE NOT (c)--() DELETE c
"""

COUNT_SUBTREE = """
MATCH (b:Book {id: $book_id})
OPTIONAL MATCH (b)-[:HAS_CHAPTER]->(ch:Chapter)
OPTIONAL MATCH (ch)-[:HAS_SECTION]->(s:Section)
OPTIONAL MATCH (c:Chunk)-[:PART_OF]->(s)
RETURN count(DISTINCT b) AS books, count(DISTINCT ch) AS chapters,
       count(DISTINCT s) AS sections, count(DISTINCT c) AS chunks
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--book-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(
        os.environ["NEO4J_NEW_URI"],
        auth=(os.environ["NEO4J_NEW_USERNAME"], os.environ["NEO4J_NEW_PASSWORD"]))
    db = os.environ.get("NEO4J_NEW_DATABASE", "neo4j")
    prefix = args.book_id + ":"
    with driver.session(database=db) as s:
        counts = s.run(COUNT_SUBTREE, book_id=args.book_id).single()
        print(f"target: {dict(counts)}  scoped-statement prefix: {prefix!r}")
        if args.dry_run:
            print("dry-run: nothing deleted")
            return
        if counts["books"] == 0:
            print("book not found — nothing to do")
            return
        s.run(DELETE_SCOPED_STATEMENTS, prefix=prefix)
        s.run(DELETE_SUBTREE, book_id=args.book_id)
        orphans = s.run(DELETE_ORPHAN_CONCEPTS).consume().counters.nodes_deleted
        print(f"deleted book subtree + scoped statements; {orphans} orphan concepts removed")
    driver.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test + a live dry-run**

Run: `uv run pytest tests/test_wipe_book.py -v` → PASS
Run: `uv run python scripts/wipe_book.py --book-id "title:probability with martingales" --dry-run`
Expected: prints counts matching the known graph (1 book, 14 chapters, 36 sections, 118 chunks), deletes nothing.

- [ ] **Step 5: Write the runbook `docs/runbooks/extraction-v2-migration.md`**

```markdown
# Extraction v2 migration — Williams wipe + re-ingest

Run AFTER feat/extraction-v2 is merged to main. Order matters.

1. **Merge + restart** so containers pick up the new code:
   `docker compose restart dagster_webserver dagster_daemon` (containers only load code on restart).
2. **Constraints:** `uv run python scripts/init_neo4j.py` (idempotent; adds notation_id, proof_id).
3. **Wipe graph data** (both books):
   - `uv run python scripts/wipe_book.py --book-id "title:probability with martingales"`
   - `uv run python scripts/wipe_book.py --book-id "isbn:9783161484100"`  # smoke-test fixture
4. **Clear Dagster partitions** so sensors treat the PDF as new (inside the webserver container):
   ```
   docker exec kr_dagster_webserver sh -c 'cd /opt/code && uv run python -c "
   from dagster import AssetKey, DagsterInstance
   from pipeline.runtime.partitions import BOOKS_PARTITION, BOOK_CHAPTERS_PARTITION
   inst = DagsterInstance.get()
   SHA = \"<williams sha — the 5eca1849... key from dynamic partitions>\"
   for ck in [k for k in inst.get_dynamic_partitions(BOOK_CHAPTERS_PARTITION) if k.startswith(SHA)]:
       inst.delete_dynamic_partition(BOOK_CHAPTERS_PARTITION, ck)
   inst.delete_dynamic_partition(BOOKS_PARTITION, SHA)
   for key in [\"book_raw_blob\",\"book_parsed\",\"book_metadata\",\"book_structure\",
               \"book_chunks\",\"book_structure_write\",\"book_chapter_extraction\",
               \"book_chapter_resolved\",\"book_chapter_graph_write\",\"book_link_resolution\"]:
       inst.wipe_asset_partitions(AssetKey(key), None) if False else None
   "'
   ```
   Asset materialization wipes: use the UI (Assets → select the 10 book assets → Wipe materializations) — the CLI `wipe_asset_partitions` API differs across Dagster versions; the UI path is version-proof. Sensor run_keys: `books_sensor` uses `run_key=<sha>` which Dagster remembers per sensor — clear via UI: Sensors → books_sensor → reset cursor.
5. **Re-ingest:** the PDF is already in `BOOKS_SOURCE_DIR`; `books_sensor` re-registers it within 5 minutes of the cursor reset. Watch localhost:3000.
6. **Verify** (success criteria from the spec):
   ```
   // notation exists, typed correctly
   MATCH (n:Notation)-[:INTRODUCED_IN]->()<-[:HAS_SECTION]-()<-[:HAS_CHAPTER]-(b:Book {id:"title:probability with martingales"}) RETURN count(n);
   // zero glossary junk definitions
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(d:Definition) WHERE d.term IN ["a.e.:", "CF:characteristic function", "DF: distribution function"] RETURN count(d);  // expect 0
   // dependency edges > 50
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(:Result)-[e:DEPENDS_ON]->() RETURN count(e);
   // proofs
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(r:Result) RETURN count(r), count{ (r)-[:PROVED_IN]->() }, count{ (r)-[:HAS_PROOF]->() };
   // zero heading-echo statements
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(r:Result) WHERE r.statement = r.name RETURN count(r);  // expect 0
   // front matter skipped
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->(ch {role:"front_matter"})-[:HAS_SECTION]->()-[:STATES]->(x) RETURN count(x);  // expect 0
   ```
7. **Paper pipeline still green:** `uv run pytest -q` and confirm the next paper ingestion run succeeds.
```

- [ ] **Step 6: Commit**

```bash
git add scripts/wipe_book.py docs/runbooks/extraction-v2-migration.md tests/test_wipe_book.py
git commit -m "feat(ops): scoped book wipe script + extraction-v2 migration runbook"
```

---

## Self-Review Notes (done at plan time)

- **Spec coverage:** §1→Task 1/2, §2→Task 3, §3→Task 5, §4→Task 6, §5→Task 7, §6→tests inside every task, §7→Task 8. Covered.
- **Type consistency:** `attach_pages` 3-tuple/3-return used identically in Tasks 4 and 7 (`proof_chunks` rows `{result_key, label, position}`); `EXTRACT_ROLES` defined Task 5, consumed Task 5 asset; `notation_node_id` defined Task 6, used only there; `result_id(owner, kind, statement)` signature matches `pipeline/assets/graph_write.py:26`.
- **Known judgment calls for the implementer:** (a) `test_cypher.py` and `test_book_outline.py` may pin old signatures — update call sites, keep assertions' intent; (b) Dagster's asset-wipe API varies by version → runbook routes through the UI; (c) the duplicated `proved_rows` loop in Task 7 Step 5 is documentation — final code contains only the corrected loop.
