from pathlib import Path
from pipeline.source import list_pdf_files, file_partition_key

def test_list_pdf_files_returns_only_pdfs(tmp_path: Path):
    (tmp_path / "a.pdf").write_bytes(b"x")
    (tmp_path / "b.txt").write_text("y")
    (tmp_path / "c.PDF").write_bytes(b"z")
    found = sorted(p.name for p in list_pdf_files(tmp_path))
    assert found == ["a.pdf", "c.PDF"]

def test_file_partition_key_is_hash_of_contents(tmp_path: Path):
    f = tmp_path / "a.pdf"
    f.write_bytes(b"hello")
    assert file_partition_key(f) == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
