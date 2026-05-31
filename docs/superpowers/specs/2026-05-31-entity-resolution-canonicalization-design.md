# Entity Resolution: Deterministic Canonicalization + Reconciled LLM Adjudicator

**Date:** 2026-05-31
**Status:** Design approved, pending spec review
**Branch:** `feat/llm-adjudicator`
**Supersedes/extends:** §7 of `docs/superpowers/specs/2026-05-27-document-graph-builder-design.md`

---

## 1. Problem

The graph over-splits `Concept` nodes. A diagnostic over the first test run (3 papers, 544 distinct
concept surfaces in `resolution_decisions`) found **~70 duplicate clusters covering ~150 surfaces**
— roughly a quarter of all concepts are *obvious* duplicates that were recorded as distinct nodes.

The duplicates fall into three tiers:

- **Tier A — obviously the same.** Differ only by casing, punctuation, a parenthetical acronym, a
  unicode dash variant, or a curly-vs-straight quote. E.g. `Bridge Matching` / `Bridge Matching (BM)`,
  `Brownian Bridge` / `Brownian bridge`, `Fokker-Planck` / `Fokker–Planck`.
- **Tier B — genuinely ambiguous.** Need judgement: "is this the same concept as that?" — not
  answerable by string rules.
- **Tier C — clearly different.** Should stay separate.

Two root causes were confirmed:

1. **Cosine-only resolution treats Tier A as Tier B.** An acronym/casing variant scores *below* the
   HIGH merge threshold (measured: `Bridge Matching (BM)` vs `Bridge Matching` = 0.845 < HIGH 0.90),
   so it lands in the ambiguous band and is split. The bulk problem is Tier A, but the machinery only
   has Tier-B tools.
2. **Intra-paper blind spot.** `resolved_entities` is decide-only and does not upsert embeddings
   (the single-writer rule reserves that for `graph_write`), so while resolving one paper, concept N
   cannot see concepts 1..N-1 of the *same* paper via `nearest()`. `extraction.merge_results` only
   collapses exact case-insensitive duplicates within a paper. So same-paper near-variants both get
   created regardless of how good the matcher is.

The prior change on this branch (an LLM adjudicator that auto-merged the whole ambiguous band) was
reviewed as "needs rework": it auto-merged uncertain pairs with no review flag (contradicting §7's
"create-new + flag, because wrong merges are hard to unwind"), and the per-concept LLM call was
unguarded (one API error/timeout/refusal aborted the whole partition).

## 2. Goals / Non-goals

**Goals**
- Catch Tier-A duplicates **deterministically and cheaply** (no LLM, no human), with ~zero false merges.
- Fix the intra-paper blind spot.
- Keep an LLM for the Tier-B band, but only **auto-merge when the LLM is confident**; route the
  LLM's own low-confidence cases to **human review** (split + flag), satisfying §7's caution.
- Make every non-deterministic merge **cacheable and reversible** via `alias_map`.
- Preserve the single-writer rule (graph stores untouched by `resolved_entities`).

**Non-goals (deferred, unchanged from §7 phase 2)**
- Building the human-review CLI/UI. This pass only *emits* the flags (recorded, with the LLM's reason)
  to feed that future tool.
- Tuning HIGH/LOW thresholds or the embedding model. (An `adjudication_model` config knob is added,
  but choosing a non-default model is out of scope.)

## 3. The resolution ladder

For each candidate `Concept`, evaluate cheapest tier first; stop at the first that decides.

1. **Canonical key (rules).** Compute `key = canonical_key(name)` (§4). Used for matching only — the
   node name remains the original surface form.
2. **Intra-paper grouping.** Within the partition, group the paper's concepts by `key`. Resolve one
   representative per key; the rest of the group merge to the representative. Action: `merge_local`.
   *(Fixes the intra-paper blind spot.)*
3. **Address-book lookup.** Look the representative's `key` up in `alias_map`. If it maps to an
   existing canonical (written by a past rule, LLM, or human decision) → merge. Action: `merge_alias`.
4. **Cosine fallback.** Otherwise `nearest()` + `decide()`:
   - similarity ≥ HIGH (0.90) → merge. Action: `merge`.
   - similarity < LOW (0.60) → create. Action: `create`.
   - LOW ≤ similarity < HIGH → **LLM adjudication** (§5), guarded:
     - `SAME` → merge; write `key → canonical` to `alias_map` with `source = 'llm'`. Action: `merge_llm`.
     - `DIFFERENT` → create. Action: `create_llm`.
     - `UNSURE`, or the call raises, or the response fails to parse → **create new (split) + flag for
       human review**, storing the LLM's reason. Action: `create_flagged`.
5. **Register new canonicals.** Any `create*` of a brand-new canonical writes `key → canonical` to
   `alias_map` with `source = 'rule'`, so future variants resolve deterministically in step 3.

The alias map is consulted before any cosine/LLM work (§7), so accumulated decisions make future runs
cheaper and stable.

## 4. `canonical_key` — the deterministic normalizer

New pure module `pipeline/canonicalize.py`, single public function `canonical_key(name: str) -> str`.
Steps, in order:

1. Unicode **NFKC** normalization.
2. **Dash unification** — `–` (en), `—` (em), `−` (minus), `‐` (hyphen) → `-`. **Quote unification** —
   curly `‘ ’ “ ”` → straight `' "`.
3. **Casefold.**
4. **Acronym strip (guarded).** If the name ends in a single trailing `(...)` whose contents, reduced
   to letters and upper-cased, equal the initials of the preceding words, drop the parenthetical.
   - `Bridge Matching (BM)` → `bridge matching` (BM = **B**ridge **M**atching). Stripped.
   - `Corrector algorithm (VE SDE)` → initials are "CA" ≠ "VESDE". **Not** stripped (it is a qualifier).
   - `G(t, c^2)` → contents not an initialism of "G". **Not** stripped.
5. **Whitespace collapse** and trim.

**Guards that guarantee ~zero false merges** (derived from observed false-positive clusters):
- **No plural/suffix stripping** (`Schrödinger Bridge` ≠ `Schrödinger Bridges` is an accepted *miss*,
  not a wrong merge).
- **No removal of `+`, `*`, or other symbols** (`DDPM` ≠ `DDPM++`, `sθ` ≠ `sθ*`, `DSBM-IMF` ≠ `DSBM-IMF+`).
- **Min-length guard:** if the resulting key is shorter than 3 characters, return the casefolded
  original instead (protects single-letter math symbols like `G`, `m`).

The function deliberately accepts misses (variants it does not collapse). Misses fall through to the
cosine/LLM/flag tiers; they never produce an incorrect merge. This conservatism is the core invariant.

## 5. LLM adjudication (reconciled)

`pipeline/resolver.py` keeps `adjudicate()` but its structured output becomes a **3-way verdict**:

```
Verdict:
  decision: "SAME" | "DIFFERENT" | "UNSURE"
  reason: str   # required, especially for UNSURE — shown to the human reviewer
```

- Called **only** for the cosine ambiguous band (a small set after normalization removes Tier A).
- The call is **guarded** in `resolved_entities`: any exception, timeout, unparseable response, or
  `UNSURE` verdict resolves to the safe fallback (create + flag), so a single failure can never abort
  the partition. Decisions for all other concepts in the partition still commit.
- Model is read from a new `adjudication_model` field on `OpenAILLMResource` (default: the current
  `extraction_model`, `gpt-5-nano`). Swapping models is a config change, not code.

## 6. Data model

`alias_map` (extend): add `source text` — one of `rule` | `llm` | `human` — recording how the alias was
established. (Existing columns: `alias`, `canonical`, `label`.) The `alias` column now also stores
canonical keys from §4, not only raw surface strings; `label` continues to scope by node type.

`resolution_decisions` (extend): add `note text` (nullable) — holds the LLM's `reason` for
`create_flagged` (and optionally `merge_llm`) rows, so the deferred review UI has context.
(Existing columns: `id`, `candidate`, `matched_to`, `label`, `score`, `action`, `run_id`, `ts`.)

`scripts/init_postgres.py` adds these columns idempotently (`ADD COLUMN IF NOT EXISTS`).

**Action vocabulary** recorded in `resolution_decisions.action`:
`merge_local`, `merge_alias`, `merge`, `merge_llm`, `create`, `create_llm`, `create_flagged`.
`graph_write` never reads `action` (verified — it keys on `name`/`surface`/`kind`/`embedding`), so the
expanded vocabulary is behaviourally invisible to the writer. The "human-review queue" is simply the
set of `create_flagged` rows.

## 7. Single-writer compliance

`resolved_entities` remains **decide-only with respect to the graph stores**: it writes only the
decision trail — `resolution_decisions` **and** `alias_map` — both of which §7 assigns to the
resolution step (the alias map is explicitly "the seam future decisions write back to"). It writes
**neither** the Neo4j `Concept` node **nor** the pgvector `entity_embeddings` table. `graph_write`
stays the sole writer of both, unchanged. `max_concurrent_runs = 1` remains the documented invariant
that makes the cross-partition address-book reads/writes safe.

## 8. Components & changes

- **`pipeline/canonicalize.py`** (new, pure): `canonical_key()` + private helpers.
- **`pipeline/resolver.py`**: `adjudicate()` returns 3-way `Verdict`; add `lookup_by_key(cur, label, key)`
  and `register_alias(cur, label, key, canonical, source)`; `decide()` thresholds unchanged
  (band → escalate, not auto-merge).
- **`pipeline/assets/resolved_entities.py`**: extract the per-partition logic into a pure
  `resolve_concepts(...)` implementing the §3 ladder (so it is unit-testable with a mock cursor and a
  fake `nearest`/`adjudicate`); the asset becomes thin glue (embed, open conn, call, write
  `resolved.json`). Guarded LLM call. Dynamic `counts` over the action vocabulary in metadata.
- **`pipeline/resources.py`**: add `adjudication_model` to `OpenAILLMResource`.
- **`scripts/init_postgres.py`**: `alias_map.source`, `resolution_decisions.note`.

## 9. Error handling

- LLM call failure / timeout / unparseable / `UNSURE` → fallback create + flag (never raises out of the
  loop). The partition completes and commits all other decisions.
- Embedding/DB errors retain current behaviour (asset fails, partition re-runnable — content-hash
  identity keeps re-runs idempotent).

## 10. Testing

- **`tests/test_canonicalize.py`**: Tier-A slam-dunks collapse (case, dash, quote, matching acronym);
  false-positives stay distinct (`DDPM`/`DDPM++`, `sθ`/`sθ*`, `Corrector (VE SDE)`/`(VP SDE)`,
  `G(1,c^2)`/`G(t,c^2)`); min-length guard; no plural strip.
- **`tests/test_resolver.py`**: `lookup_by_key`/`register_alias` (mock cursor); `adjudicate()` returns
  each of the 3 verdicts; `decide()` thresholds.
- **`resolve_concepts(...)` unit tests**: every ladder branch — intra-paper `merge_local`, `merge_alias`
  hit, cosine `merge`/`create`, LLM `merge_llm`/`create_llm`/`create_flagged`, and the guarded-error
  fallback — asserting action, canonical, and that a failure on one concept still commits the rest.
- Existing idempotency/integration tests continue to hold.

## 11. Backfill

The current 3-paper graph already contains the duplicate nodes from run 1; re-materializing alone
routes *new* resolutions to canonicals but does not delete the stale nodes. So:

1. **Scoped pre-clean** (NOT `reset_graph.py`): delete `Concept` nodes and their
   `DISCUSSES`/`DERIVED_FROM`/`DEFINES`/`USES` edges in Neo4j; truncate `entity_embeddings`,
   `alias_map`, and `resolution_decisions`. **Keep** Papers, Chunks, Definitions, Results, citations.
2. **Re-materialize** `resolved_entities` → `graph_write` for the 3 partitions.

Result: a clean graph with the new resolution logic, no full wipe.

## 12. Acceptance

- Re-running the diagnostic after backfill shows Tier-A clusters collapsed (target: the ~70 clusters
  reduced to the genuinely-distinct residue), with **no** false merges of the known false-positive
  pairs.
- The LLM is invoked only on the post-normalization band (materially fewer calls than 181).
- A simulated LLM failure/`UNSURE` produces a flagged split, not a partition abort.
