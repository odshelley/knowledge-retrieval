from server import queries


def test_render_schema_advertises_venue_properties():
    schema = queries.render_schema()
    for prop in ("venue", "journal_name", "publication_types"):
        assert prop in schema, f"{prop} invisible to the query-generating LLM"


def test_get_paper_projects_venue_properties():
    for prop in ("venue", "journal_name", "volume", "pages", "publication_types"):
        assert f".{prop}" in queries.GET_PAPER
