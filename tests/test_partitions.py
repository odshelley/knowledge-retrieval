from pipeline.runtime.partitions import documents_partitions_def, hash_bytes

def test_hash_bytes_is_stable_sha256_hex():
    h = hash_bytes(b"hello")
    assert h == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

def test_documents_partitions_def_named():
    d = documents_partitions_def()
    assert d.name == "documents"
