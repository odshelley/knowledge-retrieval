from unittest.mock import MagicMock

import pytest

from pipeline.resolver import (
    EMBEDDING_DIM,
    Decision,
    Verdict,
    adjudicate,
    decide,
    lookup_by_key,
    nearest,
    record_decision,
    similarity_to,
    upsert_alias,
    upsert_embedding,
)

_VEC = [0.0] * EMBEDDING_DIM


def test_decide_merges_above_high():
    assert decide(0.95, high=0.9, low=0.6) == Decision.MERGE


def test_decide_creates_below_low():
    assert decide(0.4, high=0.9, low=0.6) == Decision.CREATE


def test_decide_ambiguous_band_escalates_to_llm():
    assert decide(0.75, high=0.9, low=0.6) == Decision.ADJUDICATE


def test_decide_at_high_threshold():
    assert decide(0.9, high=0.9, low=0.6) == Decision.MERGE


def test_decide_at_low_threshold():
    assert decide(0.6, high=0.9, low=0.6) == Decision.ADJUDICATE


# --- DB function tests (mock cursor) ---


def test_nearest_returns_none_when_no_row():
    cur = MagicMock()
    cur.fetchone.return_value = None
    assert nearest(cur, "Concept", _VEC) is None


def test_nearest_returns_name_and_similarity():
    cur = MagicMock()
    cur.fetchone.return_value = ("Wrong-Way Risk", 0.92)
    assert nearest(cur, "Concept", _VEC) == ("Wrong-Way Risk", 0.92)
    assert cur.execute.called


def test_nearest_rejects_wrong_dim():
    cur = MagicMock()
    with pytest.raises(ValueError):
        nearest(cur, "Concept", [0.1, 0.2])


def test_upsert_embedding_uses_on_conflict():
    cur = MagicMock()
    upsert_embedding(cur, "WWR", "Concept", _VEC)
    sql = cur.execute.call_args[0][0]
    assert "ON CONFLICT" in sql


def test_upsert_embedding_rejects_wrong_dim():
    cur = MagicMock()
    with pytest.raises(ValueError):
        upsert_embedding(cur, "WWR", "Concept", [0.1])


def test_record_decision_executes_insert():
    cur = MagicMock()
    record_decision(cur, "cand", None, "Concept", 0.4, "create", "run1")
    assert cur.execute.called


# --- LLM adjudicator tests (mock OpenAI client) ---


class _FakeMessage:
    def __init__(self, verdict):
        self.parsed = verdict
        self.refusal = None


class _FakeResp:
    def __init__(self, verdict):
        self.choices = [type("C", (), {"message": _FakeMessage(verdict)})()]


class _FakeClient:
    def __init__(self, verdict):
        self._v = verdict
        self.chat = type("Chat", (), {"completions": self})()

    def parse(self, **kwargs):
        return _FakeResp(self._v)


def test_adjudicate_returns_three_way_verdict():
    v = Verdict(decision="SAME", reason="acronym of the same term")
    client = _FakeClient(v)
    out = adjudicate(client, "gpt-5-nano", "Bridge Matching", "Bridge Matching (BM)")
    assert out.decision == "SAME"


def test_verdict_rejects_bad_decision():
    import pydantic
    with pytest.raises(pydantic.ValidationError):
        Verdict(decision="MAYBE", reason="x")




# --- new DB helpers (Task 3) ---


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


def test_effective_adjudication_model_falls_back_to_extraction():
    from pipeline.runtime.resources import OpenAILLMResource
    r = OpenAILLMResource(api_key="x")
    assert r.effective_adjudication_model == r.extraction_model
    r2 = OpenAILLMResource(api_key="x", adjudication_model="gpt-5")
    assert r2.effective_adjudication_model == "gpt-5"
