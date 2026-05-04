def test_definitions_import():
    """Verify pipeline.definitions imports cleanly with the expected asset/resource shape."""
    from pipeline.definitions import defs

    assert defs is not None

    asset_keys = {a.key.path[-1] for a in defs.assets}
    expected_keys = {
        "pdf_blob",
        "v1_md_blob",
        "kg_extracted",
        "structural_overlay",
        "paper_summary",
        "legacy_graph_mirror",
    }
    assert asset_keys == expected_keys, f"Assets mismatch. Got {asset_keys}, expected {expected_keys}"

    resource_keys = set(defs.resources.keys())
    expected_resources = {"neo4j_new", "neo4j_legacy", "minio", "openai", "anthropic"}
    assert resource_keys == expected_resources, f"Resources mismatch. Got {resource_keys}, expected {expected_resources}"
