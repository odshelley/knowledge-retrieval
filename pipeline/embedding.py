"""OpenAI embedding helper."""
from __future__ import annotations


def embed_texts(client, texts: list[str], model: str) -> list[list[float]]:
    if not texts:
        return []
    resp = client.embeddings.create(model=model, input=texts)
    return [d.embedding for d in resp.data]
