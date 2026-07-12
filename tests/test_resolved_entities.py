from pipeline.assets.resolved_entities import resolved_concept_row


def test_resolved_concept_row_carries_surface_and_canonical():
    row = resolved_concept_row(
        surface="BSDE",
        canonical="Backward Stochastic Differential Equation",
        kind="concept",
        action="merge",
        embedding=[0.1, 0.2],
    )
    assert row["surface"] == "BSDE"                                   # original, for link resolution
    assert row["name"] == "Backward Stochastic Differential Equation"  # canonical, the node key
    assert row["kind"] == "concept"
    assert row["action"] == "merge"
    assert row["embedding"] == [0.1, 0.2]


def test_resolved_concept_row_shape_unchanged():
    from pipeline.assets.resolved_entities import resolved_concept_row
    row = resolved_concept_row("BM", "Bridge Matching", "concept", "merge_local", [0.1])
    assert set(row) == {"surface", "name", "kind", "action", "embedding", "description"}
