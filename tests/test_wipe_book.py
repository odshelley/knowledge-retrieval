from scripts.wipe_book import (
    DELETE_ORPHAN_CONCEPTS, DELETE_SCOPED_STATEMENTS, DELETE_SUBTREE,
)


def test_wipe_cypher_is_scoped_to_book():
    assert "$book_id" in DELETE_SUBTREE
    assert "STARTS WITH $prefix" in DELETE_SCOPED_STATEMENTS
    # orphan cleanup must require zero remaining relationships — never touch shared concepts
    assert "NOT (c)--()" in DELETE_ORPHAN_CONCEPTS
