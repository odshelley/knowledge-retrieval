from pipeline.resolver import resolve_concepts

V = [0.0] * 1536
A = [0.1] + [0.0] * 1535
B = [0.2] + [0.0] * 1535


def _calls(alias=None, nn=None, sim=None, verdict=None, raises=False):
    def lookup_by_key(label, key):
        return alias

    def nearest(label, emb):
        return nn

    def similarity_to(label, canonical, emb):
        return sim

    def adjudicate(cand, canon):
        if raises:
            raise RuntimeError("boom")
        return verdict
    return dict(lookup_by_key=lookup_by_key, nearest=nearest,
                similarity_to=similarity_to, adjudicate=adjudicate)


class _V:
    def __init__(self, d, r="r"):
        self.decision = d
        self.reason = r


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


def test_only_creating_row_carries_embedding_so_canonical_vector_is_deterministic():
    # Two surfaces group to one canonical (rep `create` + `merge_local`). graph_write upserts the
    # canonical embedding keyed by name, so at most ONE row may carry an embedding — otherwise the
    # stored vector is last-write-wins and nondeterministic. The surviving vector is the rep's.
    concepts = [{"name": "Bridge Matching", "kind": "concept"},
                {"name": "Bridge Matching (BM)", "kind": "concept"}]
    res, _ = resolve_concepts(concepts, [A, B], **_calls(nn=None))
    with_emb = [r for r in res if r.embedding is not None]
    assert len(with_emb) == 1
    assert with_emb[0].action == "create" and with_emb[0].embedding == A


def test_merge_to_existing_canonical_carries_no_embedding():
    # A cosine merge to a pre-existing canonical must NOT overwrite that canonical's established
    # embedding with this surface's vector — the merged row carries no embedding to write.
    res, _ = resolve_concepts(*_one(), **_calls(nn=("Bridge Matching", 0.95)))
    assert res[0].action == "merge" and res[0].embedding is None


def test_create_carries_representative_embedding():
    res, _ = resolve_concepts([{"name": "Bridge Matching", "kind": "concept"}], [A], **_calls(nn=None))
    assert res[0].action == "create" and res[0].embedding == A
