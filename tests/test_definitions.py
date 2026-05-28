from pipeline.definitions import defs

def test_defs_has_raw_blob_only_so_far():
    names = {a.key.to_user_string() for a in defs.get_all_asset_specs()}
    assert "raw_blob" in names
    assert "legacy_graph_mirror" not in names
    assert "structural_overlay" not in names
