import json
from pathlib import Path

import pytest

from pipeline import partitions


@pytest.fixture
def fake_partitions(tmp_path, monkeypatch):
    f = tmp_path / "partitions.json"
    f.write_text(json.dumps([
        {"paper_id": "alpha_2020", "title": "Alpha", "pdf_path": "a.pdf", "md_path": "a.md",
         "arxiv_id": None, "doi": None, "year": 2020},
        {"paper_id": "beta_2021", "title": "Beta", "pdf_path": "b.pdf", "md_path": "b.md",
         "arxiv_id": "2101.0001", "doi": None, "year": 2021},
    ]))
    monkeypatch.setattr(partitions, "PARTITIONS_FILE", f)
    return f


def test_paper_ids_returns_list_in_file_order(fake_partitions):
    assert partitions.paper_ids() == ["alpha_2020", "beta_2021"]


def test_get_partition_returns_dict(fake_partitions):
    p = partitions.get_partition("beta_2021")
    assert p is not None
    assert p["arxiv_id"] == "2101.0001"


def test_get_partition_returns_none_for_missing(fake_partitions):
    assert partitions.get_partition("zzz") is None


def test_partitions_def_constructs(fake_partitions):
    pd = partitions.partitions_def()
    assert set(pd.get_partition_keys()) == {"alpha_2020", "beta_2021"}


def test_load_returns_empty_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(partitions, "PARTITIONS_FILE", tmp_path / "nope.json")
    assert partitions.load_partitions() == []
