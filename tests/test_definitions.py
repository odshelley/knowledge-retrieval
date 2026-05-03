def test_definitions_import():
    """Verify that pipeline.definitions can be imported and has the right shape."""
    from pipeline.definitions import defs

    assert defs is not None
    assert len(defs.assets) == 5, f"Expected 5 assets, got {len(defs.assets)}"
    assert len(defs.resources) == 5, f"Expected 5 resources, got {len(defs.resources)}"

    # Asset keys are AssetKey objects with a path tuple
    asset_keys = {a.key.path[-1] for a in defs.assets}
    expected_keys = {"pdf_blob", "v1_md_blob", "kg_extracted", "structural_overlay", "paper_summary"}
    assert asset_keys == expected_keys, f"Assets mismatch. Got {asset_keys}, expected {expected_keys}"

    resource_keys = set(defs.resources.keys())
    expected_resources = {"neo4j_new", "neo4j_legacy", "minio", "openai", "anthropic"}
    assert resource_keys == expected_resources, f"Resources mismatch. Got {resource_keys}, expected {expected_resources}"
