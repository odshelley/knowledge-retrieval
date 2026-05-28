# pipeline/resources.py
from __future__ import annotations

import os

import anthropic
import boto3
from dagster import ConfigurableResource
from neo4j import Driver, GraphDatabase
from pydantic import Field


class Neo4jResource(ConfigurableResource):
    """Wraps a Neo4j driver. Used in two flavors: new DB (read/write) and legacy DB (read-only)."""
    uri: str
    username: str
    password: str
    database: str = "neo4j"

    def get_driver(self) -> Driver:
        return GraphDatabase.driver(self.uri, auth=(self.username, self.password))


class MinIOResource(ConfigurableResource):
    """S3-compatible client targeting MinIO."""
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    def get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )


class OpenAILLMResource(ConfigurableResource):
    """OpenAI used for entity extraction (gpt-5-nano) and embeddings (text-embedding-3-small)."""
    api_key: str = Field(default_factory=lambda: os.environ.get("OPENAI_API_KEY", ""))
    extraction_model: str = "gpt-5-nano"
    embedding_model: str = "text-embedding-3-small"


class AnthropicResource(ConfigurableResource):
    """Claude used for paper summary generation."""
    api_key: str = Field(default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", ""))
    summary_model: str = "claude-sonnet-4-6"

    def get_client(self) -> anthropic.Anthropic:
        return anthropic.Anthropic(api_key=self.api_key)


def new_neo4j_from_env() -> Neo4jResource:
    return Neo4jResource(
        uri=os.environ["NEO4J_NEW_URI"],
        username=os.environ["NEO4J_NEW_USERNAME"],
        password=os.environ["NEO4J_NEW_PASSWORD"],
        database=os.environ.get("NEO4J_NEW_DATABASE", "neo4j"),
    )


def legacy_neo4j_from_env() -> Neo4jResource:
    return Neo4jResource(
        uri=os.environ["NEO4J_LEGACY_URI"],
        username=os.environ["NEO4J_LEGACY_USERNAME"],
        password=os.environ["NEO4J_LEGACY_PASSWORD"],
        database=os.environ.get("NEO4J_LEGACY_DATABASE", "neo4j"),
    )


def minio_from_env() -> MinIOResource:
    return MinIOResource(
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
    )


class PostgresResource(ConfigurableResource):
    """Postgres (shares the Dagster metadata instance) for pgvector entity resolution."""
    dsn: str

    def connect(self):
        import psycopg
        from pgvector.psycopg import register_vector

        conn = psycopg.connect(self.dsn)
        register_vector(conn)
        return conn


def postgres_from_env() -> "PostgresResource":
    return PostgresResource(dsn=os.environ["RESOLVER_POSTGRES_DSN"])
