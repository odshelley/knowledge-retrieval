from pipeline.resolver import resolve_concepts

V = [0.0] * 1536


def _calls(alias=None, nn=None, sim=None, verdict=None, raises=False):
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
    assert aliases == []


def test_band_llm_error_falls_back_to_flagged():
    res, aliases = resolve_concepts(*_one(), **_calls(nn=("X", 0.8), raises=True))
    assert res[0].action == "create_flagged" and aliases == []


def test_alias_hit_human_merges_unconditionally():
    res, _ = resolve_concepts(*_one(), **_calls(alias=("Bridge Matching", "human")))
    assert res[0].action == "merge_alias"


def test_alias_hit_rule_with_low_sim_is_collision_falls_through():
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
