import os
from unittest.mock import patch
from pipeline.resources import (
    Neo4jResource,
    MinIOResource,
    OpenAILLMResource,
    AnthropicResource,
    new_neo4j_from_env,
    legacy_neo4j_from_env,
    minio_from_env,
)


def test_neo4j_resource_constructs():
    r = Neo4jResource(uri="bolt://x", username="u", password="p", database="d")
    assert r.uri == "bolt://x"
    assert r.database == "d"


def test_minio_resource_constructs():
    r = MinIOResource(endpoint_url="http://x", access_key="a", secret_key="s")
    assert r.endpoint_url == "http://x"


@patch.dict(os.environ, {
    "NEO4J_NEW_URI": "bolt://new",
    "NEO4J_NEW_USERNAME": "u1",
    "NEO4J_NEW_PASSWORD": "p1",
    "NEO4J_NEW_DATABASE": "n",
}, clear=False)
def test_new_neo4j_from_env_reads_env():
    r = new_neo4j_from_env()
    assert r.uri == "bolt://new"
    assert r.database == "n"


@patch.dict(os.environ, {
    "NEO4J_LEGACY_URI": "bolt://legacy",
    "NEO4J_LEGACY_USERNAME": "u2",
    "NEO4J_LEGACY_PASSWORD": "p2",
}, clear=False)
def test_legacy_neo4j_from_env_defaults_database():
    os.environ.pop("NEO4J_LEGACY_DATABASE", None)
    r = legacy_neo4j_from_env()
    assert r.database == "neo4j"


@patch.dict(os.environ, {
    "MINIO_ENDPOINT": "http://m",
    "MINIO_ACCESS_KEY": "a",
    "MINIO_SECRET_KEY": "s",
}, clear=False)
def test_minio_from_env():
    r = minio_from_env()
    assert r.endpoint_url == "http://m"
    assert r.access_key == "a"
