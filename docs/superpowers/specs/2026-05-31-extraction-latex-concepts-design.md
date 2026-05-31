# Extraction quality: concept names as named ideas + LaTeX math

**Date:** 2026-05-31
**Status:** Approved (design)
**Scope:** `pipeline/extraction.py` (+ tests). No schema-shape changes, no downstream/DAG changes.

## Problem

Two quality defects in the LLM extraction stage (`extracted_graph`), observed after a live run:

1. **Notation extracted as concepts.** The model mints `Concept` nodes whose `name` is bare
   mathematical notation — e.g. `W_t`, `Π*`, `ũ(x,t)`, `p_σ(x̃)`. These are not concepts; they are
   *notation that denotes* a concept. `W_t` is not a concept, "Brownian motion" is.
2. **Math is not consistently LaTeX.** Definition/result statements arrive as Unicode math
   (`ũ(x,t) = (σ²/2) ∇ ln ρ̃(x,t)`) rather than LaTeX. The current field descriptions say
   "preserve LaTeX / math notation **verbatim**", which faithfully preserves the *non*-LaTeX
   Unicode produced by the `pypdfium2` parser. Nothing instructs the model to *produce* LaTeX.

### Root-cause note (out of scope here)

Math arrives as Unicode because the active parser is `pypdfium2` (text-layer only, no equation
reconstruction — a deliberate speed tradeoff over Docling's GPU-bound VLM pipeline; see
`pipeline/parsing.py`). Fixing math *a priori* at the parsing layer (arXiv `.tex` source, a hosted
VLM page-image path, or Mathpix) is the more faithful long-term fix but is a separate, larger
parsing project with its own spec. This spec addresses extraction only — converting at extraction
time — which is needed regardless of any future parsing upgrade and improves output immediately.

## Goals

- A `Concept.name` is always a *named* idea/object/framework/method; bare notation is never minted
  as a concept (and is not "extracted then dropped" — the model is taught not to create it).
- All mathematical notation in `Definition.statement`, `Definition.term`, and `Result.statement`
  is rendered as LaTeX: inline in `$…$`, display in `$$…$$`, actively converting Unicode/plaintext.
- A conservative code backstop removes notation-only concept names that slip past the prompt,
  without ever failing extraction or dropping legitimate concepts.

## Non-goals

- No parsing-layer changes (a-priori LaTeX) — separate future project.
- No programmatic validation of LaTeX *correctness* (not reliably checkable; false flags would be
  worse than the occasional miss). LaTeX fidelity is prompt-driven only.
- No schema-shape changes (`ExtractionResult` field types unchanged), no DAG/downstream changes.

## Design

All changes are in `pipeline/extraction.py`. Both provider paths (OpenAI active, Anthropic) share
`SYSTEM_PROMPT` and the Pydantic models via `.parse()`, whose `Field(description=...)` strings are
the per-field instructions the model sees — so editing the shared file covers both providers.

### 1. `Concept.name` field description — define what qualifies

Rewrite the description to state that a concept is a *named* idea/object/framework or
algorithm/technique that could head a glossary entry, and that a bare mathematical symbol or piece
of notation (`W_t`, `Π*`, `ũ(x,t)`) is **not** a concept — it is notation denoting one. If the
named concept is present in the text, use its name ("Brownian motion"); if there is no named
concept behind the symbol, emit nothing. Retain the existing "no surrounding prose" guidance.

### 2. `statement` / `term` field descriptions — render math as LaTeX

Change `Definition.statement`, `Result.statement`, and `Definition.term` from "preserve LaTeX
verbatim" to: render **all** mathematical notation as LaTeX — inline math in `$…$`, display
equations in `$$…$$` — actively converting Unicode or plaintext math (e.g. `σ`→`\sigma`,
`∇`→`\nabla`, subscripts/superscripts/fractions); never leave raw Unicode math.

### 3. `SYSTEM_PROMPT` — global LaTeX rule + few-shot exemplars

Add to the shared `SYSTEM_PROMPT`:
- A one-line global rule restating the LaTeX requirement (reinforces the per-field descriptions).
- A one-line restatement of the concept rule (notation is never a concept).
- **Two compact worked examples** (currently there are none; the Anthropic path already notes
  few-shots belong here and cache-controls the system block):
  - From a snippet like "Let `W_t` be a standard Brownian motion…", the concept is
    **Brownian motion**, not `W_t`.
  - A definition whose source Unicode math (`ũ(x,t) = (σ²/2) ∇ ln ρ̃(x,t)`) is shown rendered as
    `$\tilde u(x,t) = \tfrac{\sigma^2}{2}\,\nabla \ln \tilde\rho(x,t)$`.

### 4. Backstop filter — `_is_notation_only(name)` in `merge_results`

A conservative safety net for the one common, detectable failure (notation-as-concept). Applied
when `merge_results` builds the deduped concept list: a concept whose name is notation-only is
skipped (silently — never raises, never fails the partition).

Letter-count alone cannot separate a concept acronym (`OT` = optimal transport, `SB` = Schrödinger
bridge) from notation (`Wt`) — both are two letters. The distinguishing signal is that *notation
carries math markup* (subscripts, operators, parentheses, symbols) while acronyms are clean letters.
So the rule keys on the presence of a math signal, not just length.

**Rule:** after removing `$` characters, a name is notation-only iff it **both** (a) contains a
*math signal* — any of `_ ^ * \ ( ) { } |`, an ASCII digit, or a non-ASCII symbol that is not a
letter (e.g. `∇ ∫ ∑ ± × ·`; Greek/accented *letters* like `σ`, `Π`, `ũ` are letters, not signals)
— **and** (b) contains **no run of ≥3 consecutive Unicode-alphabetic letters**. Hyphen and
whitespace are not signals.

- Dropped (signal + no ≥3-letter word): `W_t`, `X_t`, `Π*`, `ũ(x,t)`, `p_σ(x̃)`, `$\Pi^*$`, `∇ρ`.
- Kept:
  - `Brownian motion`, `Schrödinger bridge`, `Markovian projection` — ≥3-letter words.
  - `OT`, `SB` — clean acronyms, no math signal → not notation.
  - `ELBO`, `SDE`, `BSDE` — ≥3-letter acronyms (no signal anyway).
  - `σ-algebra` (has "algebra"), `L² space` (has "space"), `k-NN` (hyphen is not a signal; no
    signal present) — survive via a word and/or absence of any signal.

The rule errs toward keeping: a stray notation concept slipping through is cheaper than deleting a
real concept. The prompt changes (1–3) are the primary mechanism; this filter only catches slips.

## Data flow / blast radius

DAG unchanged. The effect is cleaner *content* flowing through the existing
`extracted_graph → resolved_entities → graph_write` chain: fewer junk `Concept` nodes (hence fewer
junk embeddings and resolution decisions downstream), and LaTeX-formatted definitions/results that
render in the Obsidian vault. The graph is already wiped, so a re-materialize of the three papers
picks up all changes with no extra cleanup.

## Testing (TDD)

- `_is_notation_only`: parametrized over the full keep/drop table above (every example listed in §4).
- `merge_results`: drops notation-only concepts while keeping real ones, and preserves the existing
  case-insensitive concept dedup and definition/result statement-key dedup.
- A light guard asserting the `Definition.statement` / `Result.statement` field descriptions
  reference LaTeX / `$` (cheap regression tripwire against a future edit silently dropping the rule).

## Acceptance criteria

- New unit tests pass; full suite stays green; `ruff` clean.
- After a re-materialize of the three papers: no `Concept` node whose name is bare notation; spot-checked
  definitions/results render math as `$…$`/`$$…$$` in the vault.
