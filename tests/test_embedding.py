from unittest.mock import MagicMock
from pipeline.embedding import embed_texts

def test_embed_texts_batches_and_returns_vectors():
    client = MagicMock()
    client.embeddings.create.return_value = MagicMock(
        data=[MagicMock(embedding=[0.1, 0.2]), MagicMock(embedding=[0.3, 0.4])]
    )
    out = embed_texts(client, ["a", "b"], model="text-embedding-3-small")
    assert out == [[0.1, 0.2], [0.3, 0.4]]
    client.embeddings.create.assert_called_once()
    _, kwargs = client.embeddings.create.call_args
    assert kwargs["model"] == "text-embedding-3-small"
    assert kwargs["input"] == ["a", "b"]
