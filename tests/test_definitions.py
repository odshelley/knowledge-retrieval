from pipeline.definitions import defs

def test_full_asset_set_registered():
    names = {a.key.to_user_string() for a in defs.get_all_asset_specs()}
    assert {"raw_blob", "parsed_document", "triage_metadata", "chunks",
            "extracted_graph", "resolved_entities", "graph_write", "paper_analysis"} <= names
    assert {
        "legacy_graph_mirror",
        "structural_overlay",
        "pdf_blob",
        "kg_extracted",
        "paper_summary",
    }.isdisjoint(names)
