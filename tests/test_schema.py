from pipeline import schema


def test_node_types_include_paper_and_topic():
    assert "Paper" in schema.NODE_TYPES
    assert "Topic" in schema.NODE_TYPES


def test_relationship_types_match_legacy_schema():
    # The new DB mirrors the legacy alethograph schema 1:1; verbs are subject-first
    # ("Author AUTHORED Paper", "Paper HAS_TOPIC Topic"), not the AUTHORED_BY / IN_TOPIC
    # variants we originally drafted.
    assert "AUTHORED" in schema.RELATIONSHIP_TYPES
    assert "HAS_TOPIC" in schema.RELATIONSHIP_TYPES
    assert "CITES" in schema.RELATIONSHIP_TYPES


def test_patterns_use_only_declared_types():
    for src, rel, tgt in schema.PATTERNS:
        assert src in schema.NODE_TYPES, f"unknown src: {src}"
        assert tgt in schema.NODE_TYPES, f"unknown tgt: {tgt}"
        assert rel in schema.RELATIONSHIP_TYPES, f"unknown rel: {rel}"


def test_init_cypher_includes_chunk_vector_index():
    assert "chunk_embedding" in schema.INIT_CYPHER
    assert "vector.dimensions" in schema.INIT_CYPHER
    assert "1536" in schema.INIT_CYPHER


def test_init_cypher_uses_idempotent_constraints():
    for stmt in schema.iter_init_statements():
        if stmt.upper().startswith(("CREATE CONSTRAINT", "CREATE VECTOR INDEX", "CREATE INDEX")):
            assert "IF NOT EXISTS" in stmt.upper(), f"non-idempotent: {stmt}"
