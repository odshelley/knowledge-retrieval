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


def test_decide_above_threshold_returns_match():
    assert decide(0.9, 0.7) is True


def test_decide_below_threshold_returns_no_match():
    assert decide(0.5, 0.7) is False


def test_similarity_to_returns_float_or_none():
    vec = [0.1] * EMBEDDING_DIM
    result = similarity_to(vec, vec)
    assert result is not None


def test_nearest_returns_none_on_empty():
    assert nearest([0.1] * EMBEDDING_DIM, []) is None


def test_record_decision_persists_verdict():
    store = {}
    record_decision(store, "k1", Verdict.MATCH)
    assert store["k1"] == Verdict.MATCH


def test_upsert_embedding_adds_vector():
    store = {}
    upsert_embedding(store, "k1", [0.1] * EMBEDDING_DIM)
    assert "k1" in store


def test_lookup_by_key_hit_returns_value():
    store = {"k1": 42}
    assert lookup_by_key(store, "k1") == 42


def test_adjudicate_returns_decision():
    judge = MagicMock(return_value=Decision.MATCH)
    result = adjudicate(judge, "a", "b")
    assert result == Decision.MATCH


def test_lookup_by_key_miss_returns_none():
    store = {}
    assert lookup_by_key(store, "k1") is None
