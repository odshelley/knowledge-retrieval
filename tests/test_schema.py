from pipeline.graph import schema
from pipeline.graph.schema import (
    NODE_TYPES, RELATIONSHIP_TYPES, PATTERNS, iter_init_statements,
)


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


def test_new_node_types_present():
    for label in ("Definition", "Result", "Summary"):
        assert label in NODE_TYPES


def test_new_relationship_types_present():
    for rel in ("STATES", "DEFINES", "USES", "DEPENDS_ON", "HAS_SUMMARY"):
        assert rel in RELATIONSHIP_TYPES


def test_new_patterns_present():
    expected = {
        ("Paper", "STATES", "Definition"),
        ("Paper", "STATES", "Result"),
        ("Definition", "DEFINES", "Concept"),
        ("Result", "USES", "Concept"),
        ("Result", "DEPENDS_ON", "Result"),
        ("Paper", "HAS_SUMMARY", "Summary"),
    }
    assert expected.issubset(set(PATTERNS))


def test_init_cypher_has_new_constraints():
    joined = " ".join(iter_init_statements())
    assert "definition_id" in joined
    assert "result_id" in joined
    assert "summary_id" in joined


def test_book_pipeline_node_types_present():
    from pipeline.graph.schema import NODE_TYPES
    for label in ("Chapter", "Section", "Document"):
        assert label in NODE_TYPES


def test_book_pipeline_relationship_types_present():
    from pipeline.graph.schema import RELATIONSHIP_TYPES
    for rel in ("HAS_DOCUMENT", "HAS_CHAPTER", "HAS_SECTION", "PART_OF"):
        assert rel in RELATIONSHIP_TYPES


def test_book_pipeline_patterns_present():
    from pipeline.graph.schema import PATTERNS
    expected = [
        ("Book", "HAS_DOCUMENT", "Document"),
        ("Paper", "HAS_DOCUMENT", "Document"),
        ("Book", "HAS_CHAPTER", "Chapter"),
        ("Chapter", "HAS_SECTION", "Section"),
        ("Chunk", "BELONGS_TO", "Document"),
        ("Chunk", "PART_OF", "Section"),
        ("Section", "STATES", "Definition"),
        ("Section", "STATES", "Result"),
    ]
    for triple in expected:
        assert triple in PATTERNS


def test_init_cypher_has_chapter_and_section_constraints():
    from pipeline.graph.schema import iter_init_statements
    joined = " ".join(" ".join(s.split()) for s in iter_init_statements())
    assert "CREATE CONSTRAINT chapter_id IF NOT EXISTS" in joined
    assert "CREATE CONSTRAINT section_id IF NOT EXISTS" in joined


def test_proof_pipeline_node_types_present():
    from pipeline.graph.schema import NODE_TYPES
    for label in ("Notation", "Proof"):
        assert label in NODE_TYPES


def test_proof_pipeline_relationship_types_present():
    from pipeline.graph.schema import RELATIONSHIP_TYPES
    for rel in ("INTRODUCED_IN", "DENOTES", "HAS_PROOF", "PROVED_IN"):
        assert rel in RELATIONSHIP_TYPES


def test_proof_pipeline_patterns_present():
    from pipeline.graph.schema import PATTERNS
    expected = [
        ("Notation", "INTRODUCED_IN", "Section"),
        ("Notation", "DENOTES", "Concept"),
        ("Result", "HAS_PROOF", "Proof"),
        ("Result", "PROVED_IN", "Chunk"),
    ]
    for triple in expected:
        assert triple in PATTERNS


def test_init_cypher_has_proof_notation_constraints():
    from pipeline.graph.schema import iter_init_statements
    joined = " ".join(" ".join(s.split()) for s in iter_init_statements())
    assert "CREATE CONSTRAINT notation_id IF NOT EXISTS" in joined
    assert "CREATE CONSTRAINT proof_id IF NOT EXISTS" in joined


def test_init_scripts_import_current_module_paths():
    # Regression: scripts still importing pipeline.schema / pipeline.cypher broke at c636533.
    import pathlib
    for script in ("scripts/init_neo4j.py", "scripts/reset_graph.py"):
        text = pathlib.Path(script).read_text()
        assert "pipeline.schema" not in text.replace("pipeline.graph.schema", "")
        assert "pipeline.cypher" not in text.replace("pipeline.graph.cypher", "")
