# Entity Resolution: Canonicalization + Reconciled LLM Adjudicator — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the graph over-splitting `Concept` nodes by adding a deterministic `canonical_key` normalizer + intra-paper grouping in front of cosine NN, and reconciling the LLM adjudicator into a guarded, confident-only, 3-way verdict with `alias_map`-backed caching.

**Architecture:** A pure `canonical_key()` collapses obvious (Tier-A) duplicates; a pure `resolve_concepts()` runs the per-key-group ladder (alias lookup with cosine guard → cosine NN → guarded LLM 3-way verdict → flag). `resolved_entities` stays decide-only (writes `resolution_decisions`, reads `alias_map`); `graph_write` becomes the sole writer of `alias_map` (alongside the Concept node + embedding). Reference spec: `docs/superpowers/specs/2026-05-31-entity-resolution-canonicalization-design.md` (rev 2).

**Tech Stack:** Python 3, Dagster, Postgres + pgvector (psycopg), Pydantic, OpenAI structured outputs (`chat.completions.parse`), pytest. Run tests with `uv run --extra dev pytest`.

---

## File structure

| File | Responsibility | Action |
|---|---|---|
| `pipeline/canonicalize.py` | Pure `canonical_key(name)->str` (NFKC, dash/quote unify, casefold, guarded acronym strip, guards) | Create |
| `pipeline/resolver.py` | `Verdict` (3-way) + `adjudicate`; `decide`; DB helpers `lookup_by_key`/`similarity_to`/`upsert_alias`/`nearest`/`record_decision`(+note); pure `resolve_concepts` ladder; **delete** `lookup_alias`, `SameConceptJudgment` | Modify |
| `pipeline/assets/resolved_entities.py` | Thin glue: embed → bind cursor closures → `resolve_concepts` → write `resolution_decisions` + `resolved.json` (with `alias_registrations`) | Modify |
| `pipeline/assets/graph_write.py` | Sole writer of `alias_map`: upsert `alias_registrations` in the Concept+embedding txn | Modify |
| `pipeline/resources.py` | `OpenAILLMResource.adjudication_model` + `effective_adjudication_model` | Modify |
| `scripts/init_postgres.py` | Migrations: `alias_map.source`, `resolution_decisions.note`; dedupe `load_dotenv` | Modify |
| `tests/test_canonicalize.py` | canonical_key vectors (collapse + false-positive-stay-distinct) | Create |
| `tests/test_resolver.py` | `Verdict`, `decide`, `lookup_by_key`/`similarity_to`/`upsert_alias`, `record_decision` note | Modify |
| `tests/test_resolve_concepts.py` | Every ladder branch + guarded-error fallback + alias-registration intents | Create |
| `tests/test_resolved_entities.py` | `resolved_concept_row` unchanged; asset emits `alias_registrations` | Modify |

Data contract — `resolved.json` gains a top-level key:
```json
{"concepts": [ {"surface","name","kind","action","embedding"} , ...],
 "alias_registrations": [ {"key","canonical","source"} , ... ]}
```

---

## Task 1: `canonical_key` normalizer

**Files:**
- Create: `pipeline/canonicalize.py`
- Test: `tests/test_canonicalize.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_canonicalize.py
import pytest
from pipeline.canonicalize import canonical_key


@pytest.mark.parametrize("a,b", [
    ("Bridge Matching", "Bridge Matching (BM)"),          # acronym = initials
    ("Brownian Bridge", "Brownian bridge"),               # case
    ("Fokker-Planck equation", "Fokker–Planck equation"),  # hyphen vs en-dash
    ("Fokker-Planck (FP)", "Fokker-Planck"),              # hyphenated head initials F,P
    ("Method of Moments (MoM)", "Method of Moments"),     # stop-word counted: M,O,M
    ("Girsanov’s theorem", "Girsanov's theorem"),    # curly vs straight quote
])
def test_obvious_duplicates_share_key(a, b):
    assert canonical_key(a) == canonical_key(b)


@pytest.mark.parametrize("a,b", [
    ("DDPM", "DDPM++"),                                   # symbol kept
    ("DSBM-IMF", "DSBM-IMF+"),
    ("Corrector algorithm (VE SDE)", "Corrector algorithm (VP SDE)"),  # paren not initials -> kept
    ("Score-Based Generative Model (SGM)", "Score-Based Generative Model"),  # S,B,G,M != SGM -> not stripped
    ("G(1, c^2)", "G(t, c^2)"),                           # contents not initialism; also min-len guard
    ("Schrodinger Bridge", "Schrodinger Bridges"),        # no plural stripping
])
def test_distinct_concepts_keep_distinct_keys(a, b):
    assert canonical_key(a) != canonical_key(b)


def test_min_length_guard_returns_casefolded_original():
    # 'G' normalizes to <3 chars -> guard returns casefolded original, never empty
    assert canonical_key("G") == "g"
    assert canonical_key("SB") == "sb"


def test_idempotent():
    k = canonical_key("Schrödinger Bridge (SB)")
    assert canonical_key(k) == k or canonical_key(k) == canonical_key("Schrödinger Bridge")
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev pytest tests/test_canonicalize.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.canonicalize'`.

- [ ] **Step 3: Implement `pipeline/canonicalize.py`**

```python
"""Deterministic concept-name canonicalization (match key only; node names keep their surface form).

canonical_key collapses Tier-A duplicates (case / unicode dash / curly quote / parenthetical acronym)
while NEVER producing a false merge: no plural/suffix stripping, no symbol removal, and a min-length
guard. Misses (variants it does not collapse) are intentional and fall through to the cosine/LLM tiers.
"""
from __future__ import annotations

import re
import unicodedata

_DASHES = {"–", "—", "−", "‐"}  # en, em, minus, hyphen
_QUOTE_MAP = {"‘": "'", "’": "'", "“": '"', "”": '"'}
_TRAILING_PAREN = re.compile(r"\s*\(([^()]*)\)\s*$")
_MIN_KEY_LEN = 3


def _unify_dashes_quotes(s: str) -> str:
    out = []
    for ch in s:
        if ch in _DASHES:
            out.append("-")
        else:
            out.append(_QUOTE_MAP.get(ch, ch))
    return "".join(out)


def _strip_acronym(s: str) -> str:
    """Drop a single trailing (ACR) iff ACR (letters-only, upper) == initials of preceding tokens.

    Tokens split on whitespace AND hyphens; every token contributes its first letter; no stop-word
    dropping. Returns s unchanged if the rule does not fire.
    """
    m = _TRAILING_PAREN.search(s)
    if not m:
        return s
    head = s[: m.start()]
    acr = "".join(ch for ch in m.group(1) if ch.isalpha()).upper()
    if not acr:
        return s
    tokens = [t for t in re.split(r"[\s-]+", head) if t]
    initials = "".join(t[0] for t in tokens if t[:1].isalpha()).upper()
    return head if acr == initials else s


def canonical_key(name: str) -> str:
    casefolded_original = name.strip().casefold()
    s = unicodedata.normalize("NFKC", name)
    s = _unify_dashes_quotes(s)
    s = s.casefold()
    s = _strip_acronym(s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) < _MIN_KEY_LEN:
        return casefolded_original
    return s
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --extra dev pytest tests/test_canonicalize.py -q`
Expected: PASS (all parametrized cases).

- [ ] **Step 5: Commit**

```bash
git add pipeline/canonicalize.py tests/test_canonicalize.py
git commit -m "feat(resolver): deterministic canonical_key normalizer for Tier-A dedup"
```

---

## Task 2: 3-way `Verdict` + adjudication prompt rewrite

**Files:**
- Modify: `pipeline/resolver.py` (replace `SameConceptJudgment`/`_ADJUDICATE_SYSTEM`/`adjudicate`)
- Modify: `tests/test_resolver.py`

- [ ] **Step 1: Update the failing tests in `tests/test_resolver.py`**

First **edit the existing import block** at the top of `tests/test_resolver.py`: remove `SameConceptJudgment` and `lookup_alias` from the `from pipeline.resolver import (...)` line (both are deleted in Tasks 2-3). Then **delete** the now-obsolete tests `test_lookup_alias_hit_and_miss`, `test_adjudicate_returns_parsed_judgment`, and `test_adjudicate_passes_names_model_and_schema` (they use the removed symbols). Then add:

```python
from pipeline.resolver import Verdict, adjudicate


class _FakeParsed:
    def __init__(self, verdict): self.parsed = verdict
class _FakeChoice:
    def __init__(self, verdict): self.message = _FakeParsed(verdict); self.message.refusal = None
class _FakeResp:
    def __init__(self, verdict): self.choices = [_FakeChoice(verdict)]
class _FakeClient:
    def __init__(self, verdict): self._v = verdict
        # chat.completions.parse(...)
    @property
    def chat(self):
        outer = self
        class _C:
            class completions:
                @staticmethod
                def parse(**kw): return _FakeResp(outer._v)
        return _C()


def test_adjudicate_returns_three_way_verdict():
    v = Verdict(decision="SAME", reason="acronym of the same term")
    client = _FakeClient(v)
    out = adjudicate(client, "gpt-5-nano", "Bridge Matching", "Bridge Matching (BM)")
    assert out.decision == "SAME"


def test_verdict_rejects_bad_decision():
    import pydantic, pytest
    with pytest.raises(pydantic.ValidationError):
        Verdict(decision="MAYBE", reason="x")
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev pytest tests/test_resolver.py -q`
Expected: FAIL — `ImportError: cannot import name 'Verdict'`.

- [ ] **Step 3: Edit `pipeline/resolver.py`**

Replace the `SameConceptJudgment` class, `_ADJUDICATE_SYSTEM`, and `adjudicate` with:

```python
from typing import Literal


class Verdict(BaseModel):
    """LLM 3-way verdict on whether two concept names denote the same concept."""
    decision: Literal["SAME", "DIFFERENT", "UNSURE"]
    reason: str


_ADJUDICATE_SYSTEM = (
    "You judge whether two technical concept names, extracted from research papers, refer to the "
    "SAME underlying concept. Answer with exactly one decision:\n"
    "- SAME: they denote the same concept (acronym/expansion, pluralisation, or minor notational/"
    "spelling variant of one idea, e.g. 'Bridge Matching (BM)' vs 'Bridge Matching').\n"
    "- DIFFERENT: they are genuinely different ideas, even if closely related (e.g. 'Bridge Matching' "
    "vs 'Flow Matching').\n"
    "- UNSURE: you cannot tell from the names alone whether they are the same.\n"
    "Prefer UNSURE over guessing; do NOT collapse UNSURE into DIFFERENT. Always give a brief reason."
)


def adjudicate(client, model: str, candidate: str, canonical: str,
               timeout: float | None = None) -> Verdict:
    """LLM 3-way: do `candidate` and `canonical` name the same concept? Called only for the
    ambiguous cosine band on the single top-1 neighbour. The caller guards exceptions/None."""
    resp = client.chat.completions.parse(
        model=model,
        timeout=timeout,
        messages=[
            {"role": "system", "content": _ADJUDICATE_SYSTEM},
            {"role": "user",
             "content": f"Concept A: {candidate!r}\nConcept B: {canonical!r}\n\n"
                        "Do A and B refer to the same concept? Answer SAME, DIFFERENT, or UNSURE."},
        ],
        response_format=Verdict,
    )
    return resp.choices[0].message.parsed
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --extra dev pytest tests/test_resolver.py -q`
Expected: PASS. IMPORTANT: until Task 7 rewrites `resolved_entities.py` (which still imports `lookup_alias`), the full package will not import — run ONLY `tests/test_resolver.py` and `tests/test_canonicalize.py` for now, not the whole suite.

- [ ] **Step 5: Commit**

```bash
git add pipeline/resolver.py tests/test_resolver.py
git commit -m "feat(resolver): 3-way Verdict (SAME/DIFFERENT/UNSURE) with UNSURE-preserving prompt"
```

---

## Task 3: resolver DB helpers (key lookup, similarity, alias upsert, decision note)

**Files:**
- Modify: `pipeline/resolver.py` (delete `lookup_alias`; add `lookup_by_key`, `similarity_to`, `upsert_alias`; extend `record_decision`)
- Modify: `tests/test_resolver.py`

- [ ] **Step 1: Write failing tests** (append to `tests/test_resolver.py`)

```python
from unittest.mock import MagicMock
from pipeline.resolver import lookup_by_key, similarity_to, upsert_alias, record_decision


def test_lookup_by_key_returns_canonical_and_source():
    cur = MagicMock()
    cur.fetchone.return_value = ("Bridge Matching", "rule")
    assert lookup_by_key(cur, "Concept", "bridge matching") == ("Bridge Matching", "rule")


def test_lookup_by_key_miss_returns_none():
    cur = MagicMock()
    cur.fetchone.return_value = None
    assert lookup_by_key(cur, "Concept", "x") is None


def test_similarity_to_returns_float_or_none():
    cur = MagicMock()
    cur.fetchone.return_value = (0.84,)
    assert similarity_to(cur, "Concept", "Bridge Matching", [0.0] * 1536) == 0.84
    cur.fetchone.return_value = None
    assert similarity_to(cur, "Concept", "X", [0.0] * 1536) is None


def test_upsert_alias_uses_on_conflict_do_nothing():
    cur = MagicMock()
    upsert_alias(cur, "Concept", "bridge matching", "Bridge Matching", "rule")
    sql = cur.execute.call_args[0][0]
    assert "ON CONFLICT" in sql and "DO NOTHING" in sql


def test_record_decision_writes_note():
    cur = MagicMock()
    record_decision(cur, "BM", "Bridge Matching", "Concept", 0.84, "merge_llm", "run1", note="same")
    params = cur.execute.call_args[0][1]
    assert "same" in params
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev pytest tests/test_resolver.py -q`
Expected: FAIL — `ImportError: cannot import name 'lookup_by_key'`.

- [ ] **Step 3: Edit `pipeline/resolver.py`**

Delete `lookup_alias` entirely (its test `test_lookup_alias_hit_and_miss` was removed in Task 2). Add:

```python
def lookup_by_key(cur, label: str, key: str) -> tuple[str, str] | None:
    """Return (canonical, source) the canonical_key maps to in alias_map, or None.
    `alias` column stores canonical keys only (spec rev 2 §6)."""
    cur.execute(
        "SELECT canonical, source FROM alias_map WHERE label = %s AND alias = %s",
        (label, key),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def similarity_to(cur, label: str, canonical: str, embedding: list[float]) -> float | None:
    """Cosine similarity of `embedding` to a specific canonical's stored embedding, or None if absent.
    Used by the alias cosine-guard (spec §3 step 1)."""
    if len(embedding) != EMBEDDING_DIM:
        raise ValueError(f"embedding has {len(embedding)} dims, expected {EMBEDDING_DIM}")
    cur.execute(
        "SELECT 1 - (embedding <=> %s::vector) FROM entity_embeddings "
        "WHERE label = %s AND canonical = %s",
        (embedding, label, canonical),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_alias(cur, label: str, key: str, canonical: str, source: str) -> None:
    """Register canonical_key -> canonical (first-seen wins). Sole writer is graph_write (spec §7)."""
    cur.execute(
        "INSERT INTO alias_map (alias, label, canonical, source) VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (alias, label) DO NOTHING",
        (key, label, canonical, source),
    )
```

Change `record_decision` to accept and write `note`:

```python
def record_decision(cur, candidate: str, matched_to: str | None, label: str,
                    score: float, action: str, run_id: str, note: str | None = None) -> None:
    cur.execute(
        "INSERT INTO resolution_decisions "
        "(candidate, matched_to, label, score, action, run_id, note) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (candidate, matched_to, label, score, action, run_id, note),
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --extra dev pytest tests/test_resolver.py -q`
Expected: PASS for the new helper tests.

- [ ] **Step 5: Commit**

```bash
git add pipeline/resolver.py tests/test_resolver.py
git commit -m "feat(resolver): key lookup + cosine-guard helper + alias upsert + decision note"
```

---

## Task 4: pure `resolve_concepts` ladder

**Files:**
- Modify: `pipeline/resolver.py` (add dataclasses + `resolve_concepts`)
- Test: `tests/test_resolve_concepts.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_resolve_concepts.py
from pipeline.resolver import resolve_concepts

V = [0.0] * 1536


def _calls(alias=None, nn=None, sim=None, verdict=None, raises=False):
    """Build injected closures. alias: (canonical,source)|None; nn: (canonical,score)|None;
    sim: float|None; verdict: object with .decision/.reason."""
    def lookup_by_key(label, key): return alias
    def nearest(label, emb): return nn
    def similarity_to(label, canonical, emb): return sim
    def adjudicate(cand, canon):
        if raises: raise RuntimeError("boom")
        return verdict
    return dict(lookup_by_key=lookup_by_key, nearest=nearest,
                similarity_to=similarity_to, adjudicate=adjudicate)


class _V:
    def __init__(self, d, r="r"): self.decision = d; self.reason = r


def _one(name="Bridge Matching", kind="concept"):
    return [{"name": name, "kind": kind}], [V]


def test_create_when_no_neighbour():
    res, aliases = resolve_concepts(*_one(), **_calls(nn=None))
    assert res[0].action == "create" and res[0].canonical == "Bridge Matching"
    assert aliases and aliases[0].source == "rule"


def test_merge_high_cosine():
    res, aliases = resolve_concepts(*_one(), **_calls(nn=("Bridge Matching", 0.95)))
    assert res[0].action == "merge" and res[0].canonical == "Bridge Matching"


def test_create_low_cosine():
    res, _ = resolve_concepts(*_one(), **_calls(nn=("Flow Matching", 0.3)))
    assert res[0].action == "create"


def test_band_llm_same_merges_and_registers_llm_alias():
    res, aliases = resolve_concepts(*_one(), **_calls(nn=("Bridge Matching", 0.8), verdict=_V("SAME")))
    assert res[0].action == "merge_llm" and res[0].canonical == "Bridge Matching"
    assert any(a.source == "llm" for a in aliases)


def test_band_llm_different_creates():
    res, _ = resolve_concepts(*_one(), **_calls(nn=("Flow Matching", 0.8), verdict=_V("DIFFERENT")))
    assert res[0].action == "create_llm"


def test_band_llm_unsure_flags_and_registers_no_alias():
    res, aliases = resolve_concepts(*_one(), **_calls(nn=("Flow Matching", 0.8), verdict=_V("UNSURE")))
    assert res[0].action == "create_flagged"
    assert aliases == []  # never cache an uncertain decision


def test_band_llm_error_falls_back_to_flagged():
    res, aliases = resolve_concepts(*_one(), **_calls(nn=("X", 0.8), raises=True))
    assert res[0].action == "create_flagged" and aliases == []


def test_alias_hit_human_merges_unconditionally():
    res, _ = resolve_concepts(*_one(), **_calls(alias=("Bridge Matching", "human")))
    assert res[0].action == "merge_alias"


def test_alias_hit_rule_with_low_sim_is_collision_falls_through():
    # alias says canonicalX, but embedding similarity is low -> suspected collision -> cosine ladder
    res, _ = resolve_concepts(*_one(), **_calls(alias=("Wrong", "rule"), sim=0.1, nn=("Flow", 0.3)))
    assert res[0].action == "create"
    assert "collision" in (res[0].note or "")


def test_intra_paper_grouping_merges_local():
    concepts = [{"name": "Bridge Matching", "kind": "concept"},
                {"name": "Bridge Matching (BM)", "kind": "concept"}]
    res, _ = resolve_concepts(concepts, [V, V], **_calls(nn=None))
    assert res[0].action == "create"
    assert res[1].action == "merge_local" and res[1].canonical == res[0].canonical


def test_one_row_per_surface_preserved():
    concepts = [{"name": "Bridge Matching", "kind": "concept"},
                {"name": "Bridge Matching (BM)", "kind": "concept"}]
    res, _ = resolve_concepts(concepts, [V, V], **_calls(nn=None))
    assert [r.surface for r in res] == ["Bridge Matching", "Bridge Matching (BM)"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev pytest tests/test_resolve_concepts.py -q`
Expected: FAIL — `ImportError: cannot import name 'resolve_concepts'`.

- [ ] **Step 3: Implement in `pipeline/resolver.py`**

Add near the top:
```python
from dataclasses import dataclass

from pipeline.canonicalize import canonical_key
```

Add at the end:
```python
@dataclass
class ConceptResolution:
    surface: str
    canonical: str
    kind: str
    action: str
    score: float
    matched_to: str | None
    note: str | None
    embedding: list[float]


@dataclass
class AliasRegistration:
    key: str
    canonical: str
    source: str  # "rule" | "llm"


_VALID_DECISIONS = {"SAME", "DIFFERENT", "UNSURE"}


def _resolve_one(name, key, vec, *, lookup_by_key, nearest, similarity_to, adjudicate, high, low):
    """Resolve a single representative. Returns (action, canonical, score, matched_to, note, alias|None)."""
    note = None
    hit = lookup_by_key("Concept", key)
    if hit is not None:
        canonical, source = hit
        if source == "human":
            return "merge_alias", canonical, 1.0, canonical, None, None
        sim = similarity_to("Concept", canonical, vec)
        if sim is not None and sim >= low:
            return "merge_alias", canonical, sim, canonical, None, None
        note = f"alias key collision: key={key!r} -> {canonical!r} sim={sim}"

    nn = nearest("Concept", vec)
    if nn is None:
        return "create", name, 0.0, None, note, AliasRegistration(key, name, "rule")
    matched, score = nn
    if score >= high:
        reg = AliasRegistration(key, matched, "cosine") if canonical_key(matched) != key else None
        return "merge", matched, score, matched, note, reg
    if score < low:
        return "create", name, score, None, note, AliasRegistration(key, name, "rule")

    # ambiguous band -> guarded LLM 3-way
    try:
        v = adjudicate(name, matched)
        decision = v.decision if (v is not None and v.decision in _VALID_DECISIONS) else "UNSURE"
        reason = v.reason if v is not None else "no verdict"
    except Exception as e:  # noqa: BLE001 - any failure becomes UNSURE per spec §5/§9
        decision, reason = "UNSURE", f"adjudicate error: {e}"
    note = "; ".join(p for p in (note, reason) if p)
    if decision == "SAME":
        return "merge_llm", matched, score, matched, note, AliasRegistration(key, matched, "llm")
    if decision == "DIFFERENT":
        return "create_llm", name, score, None, note, AliasRegistration(key, name, "llm")
    return "create_flagged", name, score, None, note, None  # UNSURE: never cache


def resolve_concepts(concepts, embeddings, *, lookup_by_key, nearest, similarity_to, adjudicate,
                     high: float = 0.90, low: float = 0.60):
    """Pure per-partition resolution ladder (spec §3). `concepts` is a list of {name,kind};
    `embeddings` aligns 1:1. Returns (list[ConceptResolution] one-per-surface, list[AliasRegistration])."""
    groups: dict[str, list[int]] = {}
    order: list[str] = []
    keys: list[str] = []
    for i, c in enumerate(concepts):
        k = canonical_key(c["name"])
        keys.append(k)
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(i)

    res: list = [None] * len(concepts)
    aliases: list = []
    for k in order:
        idxs = groups[k]
        rep = idxs[0]
        action, canonical, score, matched_to, note, reg = _resolve_one(
            concepts[rep]["name"], k, embeddings[rep],
            lookup_by_key=lookup_by_key, nearest=nearest, similarity_to=similarity_to,
            adjudicate=adjudicate, high=high, low=low)
        res[rep] = ConceptResolution(concepts[rep]["name"], canonical, concepts[rep]["kind"],
                                     action, score, matched_to, note, embeddings[rep])
        if reg is not None:
            aliases.append(reg)
        for j in idxs[1:]:
            res[j] = ConceptResolution(concepts[j]["name"], canonical, concepts[j]["kind"],
                                       "merge_local", 1.0, canonical, None, embeddings[j])
    return res, aliases
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --extra dev pytest tests/test_resolve_concepts.py -q`
Expected: PASS (all branches).

- [ ] **Step 5: Commit**

```bash
git add pipeline/resolver.py tests/test_resolve_concepts.py
git commit -m "feat(resolver): pure resolve_concepts ladder (grouping, cosine-guard, 3-way, flag)"
```

---

## Task 5: `adjudication_model` knob

**Files:**
- Modify: `pipeline/resources.py`
- Modify: `tests/test_resolver.py` (or a small resources test)

- [ ] **Step 1: Write the failing test** (append to `tests/test_resolver.py`)

```python
def test_effective_adjudication_model_falls_back_to_extraction():
    from pipeline.resources import OpenAILLMResource
    r = OpenAILLMResource(api_key="x")
    assert r.effective_adjudication_model == r.extraction_model
    r2 = OpenAILLMResource(api_key="x", adjudication_model="gpt-5")
    assert r2.effective_adjudication_model == "gpt-5"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev pytest tests/test_resolver.py::test_effective_adjudication_model_falls_back_to_extraction -q`
Expected: FAIL — `AttributeError: ... has no attribute 'effective_adjudication_model'`.

- [ ] **Step 3: Edit `pipeline/resources.py`**

In `OpenAILLMResource`, add the field and a resolver property:
```python
    adjudication_model: str | None = None  # None -> fall back to extraction_model

    @property
    def effective_adjudication_model(self) -> str:
        return self.adjudication_model or self.extraction_model
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --extra dev pytest tests/test_resolver.py::test_effective_adjudication_model_falls_back_to_extraction -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/resources.py tests/test_resolver.py
git commit -m "feat(resources): adjudication_model knob with live fallback to extraction_model"
```

---

## Task 6: Postgres migrations

**Files:**
- Modify: `scripts/init_postgres.py`

- [ ] **Step 1: Edit `scripts/init_postgres.py`**

Fix the duplicated import/call at the top so it reads exactly:
```python
"""Create the pgvector extension + resolver tables."""
from __future__ import annotations

import os

import psycopg
from dotenv import load_dotenv

load_dotenv()
```

Append two migration statements to the end of the `DDL` list (before the closing `]`):
```python
    "ALTER TABLE alias_map ADD COLUMN IF NOT EXISTS source text",
    "ALTER TABLE resolution_decisions ADD COLUMN IF NOT EXISTS note text",
```

- [ ] **Step 2: Apply against the running local Postgres**

Run:
```bash
docker exec kr_postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "ALTER TABLE alias_map ADD COLUMN IF NOT EXISTS source text; ALTER TABLE resolution_decisions ADD COLUMN IF NOT EXISTS note text;"'
```
Expected: `ALTER TABLE` printed twice (idempotent; safe to re-run).

- [ ] **Step 3: Verify columns exist**

Run:
```bash
docker exec kr_postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d alias_map" -c "\d resolution_decisions"'
```
Expected: `alias_map` shows `source`, `resolution_decisions` shows `note`.

- [ ] **Step 4: Commit**

```bash
git add scripts/init_postgres.py
git commit -m "chore(db): alias_map.source + resolution_decisions.note migrations; dedupe load_dotenv"
```

---

## Task 7: rewrite `resolved_entities` as glue

**Files:**
- Modify: `pipeline/assets/resolved_entities.py`
- Modify: `tests/test_resolved_entities.py`

- [ ] **Step 1: Add a failing test** (append to `tests/test_resolved_entities.py`)

```python
def test_resolved_concept_row_shape_unchanged():
    from pipeline.assets.resolved_entities import resolved_concept_row
    row = resolved_concept_row("BM", "Bridge Matching", "concept", "merge_local", [0.1])
    assert set(row) == {"surface", "name", "kind", "action", "embedding"}
```

(The full asset path needs MinIO/Postgres, so it is exercised by the materialize check in Task 9; the pure ladder is already covered in Task 4.)

- [ ] **Step 2: Run to verify it passes the unchanged-shape test and that imports still resolve**

Run: `uv run --extra dev pytest tests/test_resolved_entities.py -q`
Expected: PASS (shape test). If it errors on import, that signals the asset rewrite below is needed.

- [ ] **Step 3: Replace the body of `pipeline/assets/resolved_entities.py`**

```python
"""resolved_entities: DECIDE ONLY. Runs the canonicalization+cosine+LLM ladder (pipeline.resolver.
resolve_concepts), records decision rows, and emits resolved concepts + alias registrations for
graph_write. Writes no Neo4j, no embeddings, no alias_map (graph_write owns those — spec rev 2 §7)."""
from __future__ import annotations

import json

from dagster import MaterializeResult, MetadataValue, asset

from pipeline.embedding import embed_texts
from pipeline.partitions import documents_partitions_def
from pipeline.resolver import (
    adjudicate,
    lookup_by_key,
    nearest,
    record_decision,
    resolve_concepts,
    similarity_to,
)
from pipeline.storage import EXTRACTED_BUCKET


def resolved_concept_row(surface: str, canonical: str, kind: str, action: str,
                         embedding: list[float]) -> dict:
    """One resolved-concept record (one per original surface). `surface` is the extracted name used by
    graph_write to attach defines/uses edges; `name` is the canonical node key; `embedding` is upserted
    by graph_write keyed on the canonical name."""
    return {"surface": surface, "name": canonical, "kind": kind,
            "action": action, "embedding": embedding}


@asset(partitions_def=documents_partitions_def(), deps=["extracted_graph"],
       required_resource_keys={"minio", "openai", "postgres"})
def resolved_entities(context) -> MaterializeResult:
    key = context.partition_key
    s3 = context.resources.minio.get_client()
    payload = json.loads(s3.get_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.json")["Body"].read())

    cfg = context.resources.openai
    client = cfg.get_client()
    concepts = payload.get("concepts", [])
    names = [c["name"] for c in concepts]
    vecs = embed_texts(client, names, model=cfg.embedding_model, timeout=cfg.request_timeout)

    counts: dict[str, int] = {}
    with context.resources.postgres.connect() as conn:
        with conn.cursor() as cur:
            resolutions, aliases = resolve_concepts(
                concepts, vecs,
                lookup_by_key=lambda label, k: lookup_by_key(cur, label, k),
                nearest=lambda label, emb: nearest(cur, label, emb),
                similarity_to=lambda label, canon, emb: similarity_to(cur, label, canon, emb),
                adjudicate=lambda cand, canon: adjudicate(
                    client, cfg.effective_adjudication_model, cand, canon, timeout=cfg.request_timeout),
            )
            for r in resolutions:
                counts[r.action] = counts.get(r.action, 0) + 1
                record_decision(cur, r.surface, r.matched_to, "Concept", r.score,
                                r.action, context.run_id, note=r.note)
        conn.commit()  # decision rows ONLY — no Neo4j, no embeddings, no alias_map (graph_write owns).

    payload["concepts"] = [
        resolved_concept_row(r.surface, r.canonical, r.kind, r.action, r.embedding)
        for r in resolutions
    ]
    payload["alias_registrations"] = [
        {"key": a.key, "canonical": a.canonical, "source": a.source} for a in aliases
    ]
    s3.put_object(Bucket=EXTRACTED_BUCKET, Key=f"{key}.resolved.json",
                  Body=json.dumps(payload).encode("utf-8"))
    return MaterializeResult(metadata={k: MetadataValue.int(v) for k, v in counts.items()})
```

- [ ] **Step 4: Run to verify import + shape tests pass**

Run: `uv run --extra dev pytest tests/test_resolved_entities.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/assets/resolved_entities.py tests/test_resolved_entities.py
git commit -m "feat(resolved_entities): thin glue over resolve_concepts; emit alias_registrations"
```

---

## Task 8: `graph_write` becomes the alias_map writer

**Files:**
- Modify: `pipeline/assets/graph_write.py`

- [ ] **Step 1: Add the import**

At the top of `pipeline/assets/graph_write.py`, change the resolver import line to also import `upsert_alias`:
```python
from pipeline.resolver import upsert_embedding, upsert_alias
```

- [ ] **Step 2: Upsert aliases inside the existing Postgres transaction**

In the `with context.resources.postgres.connect() as conn:` / `with conn.cursor() as cur:` block in
`graph_write` (the one that currently upserts embeddings and handles citations), add, right after the
embedding-upsert loop over `concepts`:
```python
                # Sole writer of alias_map (spec rev 2 §7): register canonical_key -> canonical,
                # co-located with the Concept node + embedding so an alias never precedes its node.
                for reg in resolved.get("alias_registrations", []):
                    upsert_alias(cur, "Concept", reg["key"], reg["canonical"], reg["source"])
```

- [ ] **Step 3: Sanity-check the module imports**

Run: `uv run python -c "import pipeline.assets.graph_write as m; print('ok', bool(m.graph_write))"`
Expected: `ok True`.

- [ ] **Step 4: Commit**

```bash
git add pipeline/assets/graph_write.py
git commit -m "feat(graph_write): sole writer of alias_map, co-located with Concept node + embedding"
```

---

## Task 9: Full test suite + single-partition materialize check

**Files:** none (verification)

- [ ] **Step 1: Run the whole unit suite**

Run: `uv run --extra dev pytest -q`
(Run on the HOST: the Dagster container mounts only ./pipeline and ./scripts, not tests/.)
Expected: all green. If any test still references the removed `lookup_alias`, `SameConceptJudgment`, or
the old `merge_adjudicated`/`create_adjudicated` strings, update it to the new names and re-run.

- [ ] **Step 2: Re-materialize ONE existing partition end-to-end** (smoke test)

In the Dagster UI (`http://localhost:3000`) materialize `resolved_entities` then `graph_write` for a
single existing partition key. Expected: both succeed; `resolved_entities` metadata shows the new action
vocabulary (`merge`/`merge_local`/`create`/… possibly `merge_llm`/`create_flagged`).

- [ ] **Step 3: Verify alias_map + decisions populated**

Run:
```bash
docker exec kr_postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT source,count(*) FROM alias_map GROUP BY source;" -c "SELECT action,count(*) FROM resolution_decisions GROUP BY action ORDER BY 2 DESC;"'
```
Expected: `alias_map` has `rule` (and maybe `llm`) rows; `resolution_decisions` shows the new actions.

- [ ] **Step 4: Commit (if any test fixups were needed)**

```bash
git add -A && git commit -m "test: align suite with new resolver action vocabulary"
```

---

## Task 10: Backfill the test graph (operational, run once)

**Files:** none (operational; spec §11)

- [ ] **Step 1: Scoped pre-clean in Neo4j (Aura) — Concept nodes + their edges only**

Run a Cypher session (via the project's Neo4j helper or `cypher-shell`) with:
Run on the host (with `.env` loaded) a guarded one-off via the project's Neo4j resource. Do NOT use `scripts/reset_graph.py` (verify its scope first — it may wipe the whole graph):

```bash
uv run python - <<'PYEOF'
from dotenv import load_dotenv; load_dotenv()
from pipeline.resources import new_neo4j_from_env
r = new_neo4j_from_env()
with r.get_driver() as d, d.session(database=r.database) as s:
    n = s.run('MATCH (c:Concept) RETURN count(c) AS n').single()['n']
    print(f'About to DETACH DELETE {n} Concept nodes from {r.uri}')
    s.run('MATCH (c:Concept) DETACH DELETE c')
    print('deleted')
PYEOF
```
(`DETACH DELETE` removes the `DISCUSSES`/`DERIVED_FROM`/`DEFINES`/`USES` edges with the nodes; Papers,
Chunks, Definitions, Results, citations remain.)

- [ ] **Step 2: Truncate resolution state in Postgres**

Run:
```bash
docker exec kr_postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE entity_embeddings, resolution_decisions;" -c "DELETE FROM alias_map WHERE source IS DISTINCT FROM '\human'\;"'
```
(If any `alias_map.source = '"'"'human'"'"'` rows ever exist, export them first and re-insert after — none exist on the current corpus.)

- [ ] **Step 3: Re-materialize all 3 partitions**

In the Dagster UI, materialize `resolved_entities` → `graph_write` for all 3 existing partition keys
(graph_write re-`MERGE`s all four edge types, so Definitions/Results re-link cleanly).

- [ ] **Step 4: Re-run the Tier-A diagnostic and confirm collapse**

Run the clustering diagnostic over `resolution_decisions.candidate` (the same script used pre-build).
Expected: the ~70 Tier-A clusters collapse to one canonical each; zero `collision`-noted rows; LLM-action
rows (`merge_llm`/`create_llm`/`create_flagged`) number far fewer than the prior 181 band cases.

---

## Self-review

- **Spec coverage:** §3 ladder → Task 4; §4 canonical_key → Task 1; §5 Verdict/guard → Tasks 2 & 4; §6 data model + alias policy → Tasks 3, 6, 7, 8; §7 single-writer (alias in graph_write) → Task 8; §8 components → Tasks 1-8; §9 error handling → Task 4 (guarded fallback test); §10 tests → Tasks 1-8 tests; §11 backfill → Task 10; §12 acceptance → Tasks 9 & 10 steps. No uncovered section.
- **Placeholder scan:** none — every code/step is concrete.
- **Type consistency:** `Verdict.decision`/`reason`; `resolve_concepts(...)->(list[ConceptResolution], list[AliasRegistration])`; `ConceptResolution(surface,canonical,kind,action,score,matched_to,note,embedding)`; `AliasRegistration(key,canonical,source)`; helpers `lookup_by_key(cur,label,key)->(canonical,source)|None`, `similarity_to(cur,label,canonical,emb)->float|None`, `upsert_alias(cur,label,key,canonical,source)`, `record_decision(...,note=None)`; resolved.json keys `concepts`/`alias_registrations`; `OpenAILLMResource.effective_adjudication_model`. Names match across tasks.
