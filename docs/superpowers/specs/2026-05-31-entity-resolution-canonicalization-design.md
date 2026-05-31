# Entity Resolution: Deterministic Canonicalization + Reconciled LLM Adjudicator

**Date:** 2026-05-31 (rev 2 — incorporates peer-review findings)
**Status:** Design approved; rev 2 pending re-review
**Branch:** `feat/llm-adjudicator`
**Supersedes/extends:** §7 of `docs/superpowers/specs/2026-05-27-document-graph-builder-design.md`
(a superseded-pointer is added to that doc's §7; the band behaviour below replaces "create-new (split), not merge").

> **Rev 2 change log** (from a 3-reviewer pass): added a **cosine guard** on alias hits (D1);
> defined a precise **alias-write policy** that never caches uncertain decisions (D2); made `alias_map`
> a **single key-space** (canonical keys only) and **deletes `lookup_alias`** (D3); **moved alias writes
> into `graph_write`** to remove a crash-ordering hazard (D4); fully specified the **§3 control flow**
> (representative selection, per-group resolution, canonical propagation, top-1 LLM target); pinned the
> **acronym tokenisation** with test vectors; gave the exact **`Verdict`** class + guard contract;
> specified **score/null/note conventions**; and flagged the **action-string rename** as a test update.

---

## 1. Problem

The graph over-splits `Concept` nodes. A diagnostic over the first test run (3 papers, 544 distinct
concept surfaces in `resolution_decisions`) found **~70 duplicate clusters covering ~150 surfaces**
— roughly a quarter of all concepts are *obvious* duplicates recorded as distinct nodes.

Three tiers of duplicate:
- **Tier A — obviously the same.** Differ only by casing, punctuation, a parenthetical acronym, a
  unicode dash, or a curly-vs-straight quote (`Bridge Matching` / `Bridge Matching (BM)`,
  `Brownian Bridge` / `Brownian bridge`, `Fokker-Planck` / `Fokker–Planck`). A *normalization* problem.
- **Tier B — genuinely ambiguous.** Needs judgement.
- **Tier C — clearly different.** Stay separate.

Two confirmed root causes:
1. **Cosine-only resolution treats Tier A as Tier B.** Acronym/casing variants score *below* HIGH
   (measured: `Bridge Matching (BM)` vs `Bridge Matching` = 0.845 < 0.90), so they land in the
   ambiguous band and split.
2. **Intra-paper blind spot.** `resolved_entities` is decide-only and does not upsert embeddings
   (single-writer reserves that for `graph_write`), so while resolving one paper, concept N cannot see
   concepts 1..N-1 of the *same* paper via `nearest()`. `extraction.merge_results` only collapses exact
   case-insensitive duplicates within a paper.

The prior change on this branch (an LLM that auto-merged the whole ambiguous band) was reviewed
"needs rework": it auto-merged uncertain pairs with no review flag (contradicting §7's "split + flag,
because wrong merges are hard to unwind") and the per-concept call was unguarded.

## 2. Goals / Non-goals

**Goals**
- Catch Tier-A duplicates **deterministically and cheaply**, with **zero false merges** as a hard invariant.
- Fix the intra-paper blind spot.
- Keep an LLM for the Tier-B band, but **auto-merge only when the LLM is confident**; route the LLM's
  own low-confidence cases to **human review** (split + flag) — satisfying §7's caution.
- Make confident, non-deterministic merges **cacheable and reversible** via `alias_map`, while **never
  caching an uncertain decision**.
- Preserve the single-writer rule: the graph stores (Neo4j + `entity_embeddings`) are written only by
  `graph_write`; this revision also moves `alias_map` writes there (see D4).

**Non-goals (deferred, unchanged from §7 phase 2)**
- Building the human-review CLI/UI. This pass only *emits* flags (recorded, with the LLM's reason).
- Tuning HIGH/LOW thresholds or the embedding model (an `adjudication_model` knob is added; choosing a
  non-default model is out of scope).

## 3. The resolution ladder

Worked **per key-group within a partition**. Definitions first, then the per-group algorithm.

**Canonical key.** `key = canonical_key(surface)` (§4) — a match key only; the node name is always an
actual surface form.

**Representative (D-rep).** Within a partition, group the paper's concepts by `key`. The **representative**
of a group is the **first concept in extraction order** (the order `extraction.merge_results` already
preserves — stable for fixed input). Only the representative runs the ladder below; the others are
recorded as `merge_local` pointing at whatever canonical the representative resolves to.

**Per-representative ladder** (stop at the first tier that decides):

1. **Address-book lookup.** `lookup_by_key(cur, "Concept", key)` → if the key maps to an existing
   canonical, this is an **alias hit**. Apply the **cosine guard (D1)**:
   - `source = 'human'` → trust unconditionally → **merge** (`merge_alias`).
   - `source in ('rule','llm')` → compute cosine of the candidate to that canonical's embedding; if
     **≥ LOW** → **merge** (`merge_alias`); if **< LOW** → treat the alias as a suspected key-collision:
     **do not merge**, record a `collision` note, and fall through to step 2.
2. **Cosine NN.** `hit = nearest(cur, "Concept", embedding)`.
   - `hit is None` (empty store / first of its label) → **create** (score 0.0).
   - similarity ≥ HIGH (0.90) → **merge**.
   - similarity < LOW (0.60) → **create**.
   - LOW ≤ similarity < HIGH → **LLM adjudication** on the **single top-1 neighbour** (§5), guarded:
     - `SAME` → **merge** (`merge_llm`).
     - `DIFFERENT` → **create** (`create_llm`).
     - `UNSURE`, exception, timeout, refusal, `parsed is None`, or out-of-enum decision → **create new
       (split) + flag for human review** (`create_flagged`); store the LLM reason (or error) in `note`.
3. **Propagate.** Assign the representative's resulting canonical to **every member** of the key-group.
   Non-representatives are recorded as `merge_local` → that canonical.

**Output shape (D-rows).** `resolved.json` retains **one row per original surface** (not one per key),
each carrying `{surface, name=canonical, kind, embedding, action}`. This is required so `graph_write`'s
`surface_to_canon` (keyed on `surface.lower()`) can still attach `DEFINES`/`USES` edges for every
surface; collapsing to one row per key would silently drop those edges.

## 4. `canonical_key` — the deterministic normalizer

New pure module `pipeline/canonicalize.py`, single public function `canonical_key(name: str) -> str`,
steps in order:

1. Unicode **NFKC** normalization.
2. **Dash unification** — `–`(en) `—`(em) `−`(minus) `‐`(hyphen) → ASCII `-`. **Quote unification** —
   curly `‘ ’ “ ”` → straight `' "`. (Primes `′ ″` and backticks are **intentionally not** unified.)
3. **Casefold.**
4. **Acronym strip (guarded), precisely defined:**
   - Consider only the **single trailing balanced `(...)` group** at end-of-string (after trim). No
     recursion, no handling of multiple/nested trailing groups (leave those intact).
   - Let `acr = ` the group's contents reduced to **letters only**, uppercased.
   - **Tokenise the preceding text on whitespace AND hyphens**; `initials = ` the uppercased first
     letter of each token. **No stop-word dropping.**
   - Strip the parenthetical **iff `acr == initials`** (exact, including count).
   - Worked vectors (add as tests): `Bridge Matching (BM)`→strip; `Fokker-Planck (FP)`→strip (F,P);
     `Method of Moments (MoM)`→strip (M,O,M); `Score-Based Generative Model (SGM)`→**no** strip
     (S,B,G,M≠SGM — safe miss); `Corrector algorithm (VE SDE)`→**no** strip (C,A≠VESDE);
     `G(t, c^2)`→**no** strip (contents not an initialism of "G").
5. **Whitespace collapse** and trim.

**Guards — the zero-false-merge invariant:**
- **No plural/suffix stripping** (`Schrödinger Bridge` ≠ `Schrödinger Bridges` — an accepted *miss*).
- **No removal of `+ * /` or other symbols** (`DDPM` ≠ `DDPM++`, `sθ` ≠ `sθ*`, `DSBM-IMF` ≠ `DSBM-IMF+`).
- **Min-length guard:** if the post-normalization key is shorter than 3 characters, return the casefolded
  **original** (protects single/short math symbols like `G`, `m`). NB this means Tier-A collapsing is
  **disabled for names whose key would be <3 chars** (accepted miss, never a false merge).
- The function deliberately accepts misses; misses fall through to the cosine/LLM/flag tiers and never
  produce an incorrect merge. **This conservatism is the core invariant** and is asserted by tests that
  the known false-positive pairs above stay distinct.

## 5. LLM adjudication (reconciled)

`pipeline/resolver.py` keeps the adjudication call but the structured output becomes a **3-way verdict**.
Rename `SameConceptJudgment` → `Verdict`:

```python
class Verdict(BaseModel):
    decision: Literal["SAME", "DIFFERENT", "UNSURE"]
    reason: str  # required; for UNSURE it explains the doubt (shown to the human reviewer)

def adjudicate(client, model, candidate: str, canonical: str, timeout=...) -> Verdict: ...
```

- Called **once per ambiguous representative, on the single top-1 `nearest()` neighbour** (matches the
  existing `nearest()` `LIMIT 1` contract). Call site uses `cfg.adjudication_model` (see §8).
- **System prompt rewrite:** must genuinely offer all three options. The current prompt's "if not
  confident, answer they are not the same" collapses UNSURE into DIFFERENT and would make flagging dead
  code — replace it with explicit instructions to return `UNSURE` when genuinely uncertain.
- **Guard contract (D-guard):** the call site wraps both the call AND the result access. Any of —
  exception, timeout, `resp.choices[0].message.refusal` present, `.parsed is None`, or a `decision` not
  in the enum — is treated as **UNSURE → `create_flagged`**, so a single bad response can never raise out
  of the loop. All other concepts in the partition still commit.
- Because §4 removes Tier A first, the LLM is invoked only on the small post-normalization band.

## 6. Data model & alias-write policy

**`alias_map` is a single key-space (D3).** Its `alias` column now stores **canonical keys only** (the
output of §4), never raw surface forms. The legacy raw-name lookup `lookup_alias` is **deleted** and
replaced by `lookup_by_key`. Phase-2 human review will also write keyed by canonical key. Add column `source text` ∈ `{rule, cosine, llm, human}`. (Existing columns: `alias`, `canonical`, `label`.) The §11 backfill
truncates `alias_map`, so no legacy raw-surface rows survive into the new scheme.

**`resolution_decisions`**: add `note text` (nullable) — holds the LLM `reason` for `create_flagged`, the
LLM `reason` for `merge_llm`, and a `collision` marker when D1's cosine guard rejects an alias hit.
(Existing: `id, candidate, matched_to, label, score, action, run_id, ts`.)

**Migrations** in `scripts/init_postgres.py` as idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
entries appended to the existing DDL list (`alias_map.source`, `resolution_decisions.note`). Also remove
the pre-existing duplicated `load_dotenv()` import/call while touching the file.

**Alias-write policy (D2) — who registers `key → canonical`, and where (D4: written by `graph_write`).**
`resolved_entities` only *decides* and emits the intended registrations in `resolved.json`;
**`graph_write` performs the `alias_map` INSERTs**, in the same transaction as the Concept node +
`entity_embeddings` upsert (so an alias can never reference a not-yet-created node — fixes the crash
window). Registration rules:

| Action | New node? | Register alias? | source |
|---|---|---|---|
| `merge` (cosine ≥ HIGH) | no | yes, if candidate key ≠ canonical's key (caches the variant) | `cosine` |
| `merge_alias` | no | no (already present) | — |
| `merge_llm` (LLM SAME) | no | **yes** (cacheable, but D1-guarded on future reads) | `llm` |
| `create` (cosine < LOW / no NN) | yes | **yes** (first-seen registration) | `rule` |
| `create_llm` (LLM DIFFERENT) | yes | yes (a confident new canonical) | `llm` |
| `create_flagged` (UNSURE/error) | yes | **NO — never cache an uncertain decision** | — |
| `merge_local` (intra-paper) | no | no (the representative's row handles registration) | — |

Rationale for the two judgement calls:
- **D1 cosine guard on alias hits** closes the key-collision force-merge hole: a `canonical_key`
  collision between two genuinely different concepts can no longer auto-merge, because a low embedding
  similarity routes it back through the cosine/LLM ladder.
- **D2 never-cache-uncertain** ensures a `create_flagged` (human-review) item is *not* turned into a
  deterministic alias; its key stays unregistered so future identical surfaces re-enter the ladder rather
  than silently inheriting an unreviewed decision.

**Action vocabulary** (`resolution_decisions.action`): `merge`, `merge_alias`, `merge_llm`,
`merge_local`, `create`, `create_llm`, `create_flagged`. `graph_write` does **not** read `action`
(verified: it keys on `name`/`surface`/`kind`/`embedding`), so the vocabulary is invisible to the writer;
the only consumer is the future review UI (`create_flagged` rows = the review queue). **The rename from
the current `merge_adjudicated`/`create_adjudicated` strings means existing tests that assert those
strings must be updated (§10), not merely supplemented.**

**Score / matched_to conventions.** `matched_to` = canonical for any merge action, else `NULL`. `score`
is the cosine similarity used for the decision; `1.0` for a human-trusted `merge_alias` and for `merge_local` (the D1 cosine-guarded `merge_alias` records the guard similarity instead); `0.0` for a
`create` with no NN; the real band score for `merge_llm`/`create_llm`/`create_flagged`.

## 7. Single-writer compliance

`resolved_entities` remains **decide-only**: it READS `alias_map` (via `lookup_by_key`) and the cosine
store, and WRITES only `resolution_decisions`. It does **not** write Neo4j, `entity_embeddings`, or (as
of D4) `alias_map`. **`graph_write` is the sole writer** of the Neo4j `Concept` node, its
`entity_embeddings` row, **and** the `alias_map` registration — the three written together per the
"as one unit" rule (§5.9/§7 of the 2026-05-27 doc). `max_concurrent_runs = 1` remains the documented
invariant making cross-partition address-book reads/writes safe.

**Determinism note.** Per-key results are deterministic; the chosen canonical **surface** for a key still
depends on partition materialization order (whichever partition first registers the key wins). This is
acceptable: the *key* is stable, only the display surface varies. (If reproducible re-runs are later
required, add a tie-break — e.g. lexicographically smallest surface for a key — but that is out of scope.)

## 8. Components & changes

- **`pipeline/canonicalize.py`** (new, pure, no deps): `canonical_key()` + private helpers.
- **`pipeline/resolver.py`**: `SameConceptJudgment` → `Verdict` (3-way); `decide()` thresholds unchanged
  (band → escalate); **delete `lookup_alias`**; add `lookup_by_key(cur, label, key)`. (Alias INSERT
  helper moves to graph_write's path — see below.)
- **`pipeline/assets/resolved_entities.py`**: extract the per-partition logic into a **pure
  `resolve_concepts(concepts, embeddings, *, lookup_by_key, nearest, adjudicate, run_id)`** implementing
  §3 (grouping, ladder, propagation, alias-registration *intents*); the asset is thin glue (embed, open
  conn, call, write `resolved.json`). Guarded LLM call. Dynamic `counts` over the action vocabulary.
- **`pipeline/assets/graph_write.py`**: in the existing Concept+embedding transaction, also UPSERT the
  `alias_map` rows carried in `resolved.json` (per the §6 policy).
- **`pipeline/resources.py`**: add `adjudication_model` to `OpenAILLMResource`, **defaulting to `None`
  with a live fallback to `extraction_model`** at the call site (so the two don't silently diverge).
- **`scripts/init_postgres.py`**: the two `ADD COLUMN IF NOT EXISTS` migrations; dedupe `load_dotenv`.

## 9. Error handling

- LLM failure / timeout / refusal / `parsed is None` / out-of-enum / `UNSURE` → `create_flagged`
  (never raises out of the loop); the partition completes and commits all other decisions.
- D1 collision (alias hit with cosine < LOW) → fall through to cosine/LLM; record `collision` in `note`.
- Embedding/DB errors retain current behaviour (asset fails, partition re-runnable; content-hash
  identity keeps re-runs idempotent).

## 10. Testing

- **`tests/test_canonicalize.py`**: Tier-A slam-dunks collapse (case, dash, quote, matching acronym incl.
  the `Fokker-Planck (FP)` / `Method of Moments (MoM)` vectors); false-positives stay distinct
  (`DDPM`/`DDPM++`, `sθ`/`sθ*`, `Corrector (VE SDE)`/`(VP SDE)`, `G(1,c^2)`/`G(t,c^2)`,
  `Score-Based Generative Model (SGM)`); min-length guard; no plural strip.
- **`tests/test_resolver.py`**: `lookup_by_key` (mock cursor); `adjudicate()` returns each of the 3
  verdicts; `decide()` thresholds. **Update/remove** assertions referencing the old
  `merge_adjudicated`/`create_adjudicated` strings.
- **`resolve_concepts(...)` unit tests** (pure, mock cursor + fake `nearest`/`adjudicate`): every branch
  — `merge_local` propagation (incl. one-row-per-surface output); `merge_alias` hit; D1 collision
  fall-through; cosine `merge`/`create`; LLM `merge_llm`/`create_llm`/`create_flagged`; the guarded-error
  fallback (assert one bad concept still commits the rest); and the §6 alias-registration intents emitted
  (and **absent** for `create_flagged`).
- **`tests/test_resolved_entities.py`** / **graph_write**: alias UPSERT happens in graph_write and only
  for the policy-permitted actions.
- Existing idempotency/integration tests continue to hold (after the action-string updates above).

## 11. Backfill

The current 3-paper graph contains run-1 duplicates; re-materializing routes new resolutions to
canonicals but does not delete stale nodes. So:

1. **Scoped pre-clean** (NOT `reset_graph.py`): delete `Concept` nodes and their
   `DISCUSSES`/`DERIVED_FROM`/`DEFINES`/`USES` edges in Neo4j; truncate `entity_embeddings`,
   `alias_map`, `resolution_decisions`. **Keep** Papers, Chunks, Definitions, Results, citations.
   (`alias_map` currently has no `source='human'` rows; if any existed they would be preserved instead.)
2. **Re-materialize** `resolved_entities` → `graph_write` for the 3 partitions. `graph_write` re-`MERGE`s
   all four edge types on re-run (all idempotent `MERGE`), so the scoped delete leaves no dangling
   Definitions/Results.

## 12. Acceptance

- After backfill, re-running the §1 diagnostic: the Tier-A clusters (the surfaces sharing a
  `canonical_key`) collapse to one node each; **all known false-positive pairs in §4/§10 remain
  distinct** (automated assertion). Residual multi-surface clusters are manually confirmed to be Tier-B/C
  (eyeball check — not an automated count).
- **Cross-paper alias key-collisions are measured**, not just the known pairs: count `collision`-noted
  rows; expect ~0 on the current corpus.
- The LLM is invoked only on the post-normalization band (materially fewer calls than the prior 181).
- A simulated LLM failure / `UNSURE` produces a flagged split (`create_flagged`) with no alias
  registered, and does **not** abort the partition. `UNSURE` is actually exercised (not only simulated).
