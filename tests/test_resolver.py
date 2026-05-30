from unittest.mock import MagicMock

import pytest

from pipeline.resolver import (
    EMBEDDING_DIM,
    Decision,
    decide,
    lookup_alias,
    nearest,
    record_decision,
    upsert_embedding,
)

_VEC = [0.0] * EMBEDDING_DIM


def test_decide_merges_above_high():
    assert decide(0.95, high=0.9, low=0.6) == Decision.MERGE


def test_decide_creates_below_low():
    assert decide(0.4, high=0.9, low=0.6) == Decision.CREATE


def test_decide_ambiguous_band_creates_and_flags():
    assert decide(0.75, high=0.9, low=0.6) == Decision.CREATE_FLAGGED


def test_decide_at_high_threshold():
    assert decide(0.9, high=0.9, low=0.6) == Decision.MERGE


def test_decide_at_low_threshold():
    assert decide(0.6, high=0.9, low=0.6) == Decision.CREATE_FLAGGED


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


def test_lookup_alias_hit_and_miss():
    cur = MagicMock()
    cur.fetchone.return_value = ("Canonical Name",)
    assert lookup_alias(cur, "Concept", "alias") == "Canonical Name"
    cur.fetchone.return_value = None
    assert lookup_alias(cur, "Concept", "alias") is None
