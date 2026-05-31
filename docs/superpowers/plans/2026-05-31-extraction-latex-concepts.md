# Extraction Quality (Concept Names + LaTeX) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the LLM extraction stage stop minting concepts from bare math notation, and render all math as LaTeX, with a conservative code backstop for notation-only concept names.

**Architecture:** All changes live in `pipeline/extraction.py` (shared by both the OpenAI and Anthropic `.parse()` paths). Pydantic `Field(description=...)` strings and `SYSTEM_PROMPT` are the model-facing instructions; a pure `_is_notation_only` helper applied inside `merge_results` is the code backstop. No schema-shape, DAG, or downstream changes.

**Tech Stack:** Python 3.12, Pydantic v2, pytest, `uv` (run tests with `uv run pytest`), ruff.

**Reference:** spec at `docs/superpowers/specs/2026-05-31-extraction-latex-concepts-design.md`.

---

## Task 1: `_is_notation_only` backstop helper

A pure predicate: a name is notation-only iff it carries a *math signal* (markup/operator/digit) **and** has no run of ≥3 consecutive letters. Keeps clean acronyms (`OT`, `SB`), drops notation (`W_t`, `Π*`).

**Files:**
- Modify: `pipeline/extraction.py`
- Test: `tests/test_extraction.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_extraction.py` (add a module-level `import pytest` at the top if not already present; the tests import the helper function-locally, matching the existing test style):

```python
import pytest


@pytest.mark.parametrize("name", [
    "W_t", "X_t", "Π*", "ũ(x,t)", "p_σ(x̃)", r"$\Pi^*$", "∇ρ",
])
def test_is_notation_only_drops_bare_notation(name):
    from pipeline.extraction import _is_notation_only
    assert _is_notation_only(name) is True


@pytest.mark.parametrize("name", [
    "Brownian motion", "Schrödinger bridge", "Markovian projection",
    "OT", "SB", "ELBO", "SDE", "BSDE", "WWR",
    "σ-algebra", "L² space", "k-NN", "GPT-4", "2-Wasserstein distance",
])
def test_is_notation_only_keeps_real_concepts(name):
    from pipeline.extraction import _is_notation_only
    assert _is_notation_only(name) is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_extraction.py -k is_notation_only -q`
Expected: FAIL with `ImportError: cannot import name '_is_notation_only'` (or `AttributeError`).

- [ ] **Step 3: Implement the helper**

Add to `pipeline/extraction.py` (near the other module-level helpers, e.g. just above `merge_results`):

```python
# Structural math markup / operators that mark a string as notation (NOT letters, hyphen, space).
_MATH_SIGNAL_CHARS = set("_^*\\(){}|")


def _has_three_letter_run(s: str) -> bool:
    """True if s has a run of >=3 consecutive Unicode-alphabetic letters (a word-like token)."""
    run = 0
    for ch in s:
        if ch.isalpha():
            run += 1
            if run >= 3:
                return True
        else:
            run = 0
    return False


def _has_math_signal(s: str) -> bool:
    """True if s contains math markup: structural chars, a digit, or a non-letter symbol.
    Greek/accented LETTERS (σ, Π, ũ) are letters, not signals; hyphen and whitespace are not signals."""
    for ch in s:
        if ch in _MATH_SIGNAL_CHARS or ch.isdigit():
            return True
        if not ch.isascii() and not ch.isalpha() and not ch.isspace():
            return True
    return False


def _is_notation_only(name: str) -> bool:
    """Conservative backstop: a concept name is notation-only (and should not be a Concept) iff it
    carries a math signal AND has no >=3-letter word. Errs toward keeping (real concept > stray symbol)."""
    s = name.replace("$", "")
    return _has_math_signal(s) and not _has_three_letter_run(s)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_extraction.py -k is_notation_only -q`
Expected: PASS (all parametrized cases).

- [ ] **Step 5: Commit**

```bash
git add pipeline/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): add _is_notation_only backstop predicate"
```

---

## Task 2: Drop notation-only concepts in `merge_results`

Wire the backstop into the concept-collection loop so notation-only names never become concepts, while the existing case-insensitive dedup and definition/result dedup are preserved.

**Files:**
- Modify: `pipeline/extraction.py` (the `merge_results` concepts loop)
- Test: `tests/test_extraction.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_extraction.py`:

```python
def test_merge_results_drops_notation_only_concepts_keeps_real():
    from pipeline.extraction import merge_results, ExtractionResult, Concept
    part = ExtractionResult(concepts=[
        Concept(name="Brownian motion", kind="concept"),
        Concept(name="W_t", kind="concept"),          # notation -> dropped
        Concept(name="brownian motion", kind="concept"),  # case dup -> deduped
        Concept(name="Π*", kind="concept"),            # notation -> dropped
        Concept(name="OT", kind="concept"),            # acronym -> kept
    ])
    merged = merge_results([part])
    names = [c.name for c in merged.concepts]
    assert names == ["Brownian motion", "OT"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_extraction.py::test_merge_results_drops_notation_only_concepts_keeps_real -q`
Expected: FAIL — `W_t` and `Π*` are still present (assert on `names` mismatches).

- [ ] **Step 3: Implement the filter in `merge_results`**

In `pipeline/extraction.py`, the concepts loop currently reads:

```python
    seen_c, concepts = set(), []
    for p in parts:
        for c in p.concepts:
            if c.name.lower() not in seen_c:
                seen_c.add(c.name.lower())
                concepts.append(c)
```

Change it to skip notation-only names:

```python
    seen_c, concepts = set(), []
    for p in parts:
        for c in p.concepts:
            if _is_notation_only(c.name):
                continue  # bare notation is never a concept (backstop; primary fix is the prompt)
            if c.name.lower() not in seen_c:
                seen_c.add(c.name.lower())
                concepts.append(c)
```

- [ ] **Step 4: Run the new test and the full extraction suite**

Run: `uv run pytest tests/test_extraction.py -q`
Expected: PASS, including the pre-existing `test_merge_results_dedupes_definitions_and_results_across_overlapping_chunks` (its `"WWR"` concept is a 3-letter acronym and is kept).

- [ ] **Step 5: Commit**

```bash
git add pipeline/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): drop notation-only concepts in merge_results"
```

---

## Task 3: Prompt + field-description changes (concept rule + LaTeX) with a regression guard

Rewrite the model-facing instructions so the model (a) treats concepts as named ideas and (b) renders all math as LaTeX, with two few-shot exemplars. Add a cheap tripwire test so a future edit can't silently drop these rules.

**Files:**
- Modify: `pipeline/extraction.py` (`Concept.name`, `Definition.statement`, `Definition.term`, `Result.statement` descriptions; `SYSTEM_PROMPT`)
- Test: `tests/test_extraction.py`

- [ ] **Step 1: Write the failing guard tests**

Add to `tests/test_extraction.py`:

```python
def test_statement_field_descriptions_require_latex():
    from pipeline.extraction import Definition, Result
    for model in (Definition, Result):
        desc = model.model_fields["statement"].description
        assert "$" in desc
        assert "LaTeX" in desc


def test_concept_name_description_forbids_bare_notation():
    from pipeline.extraction import Concept
    desc = Concept.model_fields["name"].description
    assert "notation" in desc.lower()


def test_system_prompt_states_both_rules():
    from pipeline.extraction import SYSTEM_PROMPT
    assert "LaTeX" in SYSTEM_PROMPT
    assert "Brownian motion" in SYSTEM_PROMPT  # the concept-vs-notation few-shot
```

- [ ] **Step 2: Run the guard tests to verify they fail**

Run: `uv run pytest tests/test_extraction.py -k "description or system_prompt" -q`
Expected: FAIL — current `Concept.name` description has no "notation"; `SYSTEM_PROMPT` has no "LaTeX"/"Brownian motion".

- [ ] **Step 3: Rewrite the field descriptions**

In `pipeline/extraction.py`, replace the `Concept.name` field:

```python
    name: str = Field(
        description="The name of a *named* idea, object, framework, or algorithm/technique, as it "
        "would head a glossary entry — no surrounding prose. It must be a real concept name, never "
        "bare mathematical notation: a symbol like 'W_t', 'Π*', or 'ũ(x,t)' is NOT a concept, it is "
        "notation that denotes one. If the named concept is present in the text, use its name (e.g. "
        "'Brownian motion', not 'W_t'); if a symbol has no named concept behind it, emit no concept for it."
    )
```

Replace the `Definition.statement` field:

```python
    statement: str = Field(
        description="The full formal definition as stated in the text. Render ALL mathematical "
        "notation as LaTeX: inline math in $...$, display equations in $$...$$. Convert any Unicode "
        "or plaintext math to LaTeX (e.g. σ -> \\sigma, ∇ -> \\nabla, sub/superscripts and fractions); "
        "never leave raw Unicode math."
    )
```

Replace the `Definition.term` field:

```python
    term: str = Field(
        description="The exact term being defined. If it contains mathematical notation, render it "
        "as LaTeX in $...$."
    )
```

Replace the `Result.statement` field:

```python
    statement: str = Field(
        description="The full statement of the result, excluding any proof. Render ALL mathematical "
        "notation as LaTeX: inline math in $...$, display equations in $$...$$. Convert any Unicode or "
        "plaintext math to LaTeX; never leave raw Unicode math."
    )
```

- [ ] **Step 4: Extend `SYSTEM_PROMPT` with the two rules + few-shots**

In `pipeline/extraction.py`, the current `SYSTEM_PROMPT` ends with `...if unsure, leave the list empty."""`. Append the two rules before the closing `"""` so the final value reads:

```python
SYSTEM_PROMPT = """You are an information-extraction assistant for STEM research papers \
(most often rooted in mathematics, statistics, or AI / machine learning, but spanning the \
sciences and engineering broadly). From the chunk, populate the concepts, definitions, and \
results of the response schema, following each field's description. Emit nothing not asserted \
by the text. When filling a definition's `defines`, a result's `uses`, or a result's \
`depends_on`, reference ONLY names you have already produced in this same response; if \
unsure, leave the list empty.

Two rules govern every field:
1. CONCEPTS are named ideas/objects/frameworks/algorithms (glossary headwords). Bare mathematical \
notation is never a concept: from "Let W_t be a standard Brownian motion", the concept is \
"Brownian motion", NOT "W_t". If a symbol has no named concept behind it, emit no concept for it.
2. Render ALL mathematical notation as LaTeX — inline in $...$, display in $$...$$ — actively \
converting Unicode or plaintext math. For example, source text "ũ(x,t) = (σ²/2) ∇ ln ρ̃(x,t)" must \
be written as $\\tilde u(x,t) = \\tfrac{\\sigma^2}{2}\\,\\nabla \\ln \\tilde\\rho(x,t)$. Never leave \
raw Unicode math in any field."""
```

- [ ] **Step 5: Run the guard tests and the full extraction suite**

Run: `uv run pytest tests/test_extraction.py tests/test_extraction_anthropic.py -q`
Expected: PASS (guard tests green; no existing extraction test broken).

- [ ] **Step 6: Lint**

Run: `uv run ruff check pipeline/extraction.py tests/test_extraction.py`
Expected: `All checks passed!`
Note: the descriptions/prompt contain Unicode math (σ, ∇, ũ) as intentional examples. If ruff's `RUF001`/`RUF003` (ambiguous-character) fires on these lines, add a targeted `# noqa: RUF001` to the offending line(s) — do not remove the example characters, they are the point.

- [ ] **Step 7: Commit**

```bash
git add pipeline/extraction.py tests/test_extraction.py
git commit -m "feat(extraction): concept=named-idea rule + LaTeX math in prompt/schema"
```

---

## Task 4: Full-suite green check

**Files:** none (verification only).

- [ ] **Step 1: Run the whole suite**

Run: `uv run pytest -q`
Expected: all pass, 0 failed (new tests added; nothing else changed).

- [ ] **Step 2: Lint the touched files**

Run: `uv run ruff check pipeline/extraction.py tests/test_extraction.py`
Expected: `All checks passed!`

- [ ] **Step 3: (No commit needed if clean.)** If Step 1 or 2 surfaced a fix, commit it:

```bash
git add -A
git commit -m "test(extraction): suite green after extraction-quality changes"
```

---

## Notes for the implementer

- **No live LLM calls are tested.** The behavioral guarantee for the prompt changes (concepts as named ideas, math as LaTeX) is validated by re-materializing the three papers and inspecting the graph/vault — that is an operational step the user runs, not part of this plan.
- **Both providers are covered** because `SYSTEM_PROMPT` and the Pydantic models are shared by the OpenAI and Anthropic paths; there is no second prompt to edit.
- **After merge**, the user re-runs `extracted_graph → resolved_entities → graph_write` for the 3 papers (graph already wiped) to pick up the new extraction behavior. No code in this plan triggers that.
