from pipeline.graph.cypher import batched_detach_delete

def test_batched_detach_delete_uses_call_in_transactions():
    q = batched_detach_delete(batch_size=5000)
    assert "MATCH (n)" in q
    assert "DETACH DELETE n" in q
    assert "IN TRANSACTIONS OF 5000 ROWS" in q
