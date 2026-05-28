"""OpenAI embedding helper."""
from __future__ import annotations


def embed_texts(client, texts: list[str], model: str, timeout: float = 60.0) -> list[list[float]]:
    if not texts:
        return []
    resp = client.embeddings.create(model=model, input=texts, timeout=timeout)
    return [d.embedding for d in resp.data]
