from unittest.mock import MagicMock

import pytest

from pipeline.sensors import _list_new_keys, _key_to_partition


def test_key_to_partition_strips_pdf_extension():
    assert _key_to_partition("burnett_2023_hva.pdf") == "burnett_2023_hva"


def test_key_to_partition_rejects_non_pdf():
    with pytest.raises(ValueError):
        _key_to_partition("foo.txt")


def test_list_new_keys_returns_keys_after_cursor():
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "a.pdf", "LastModified": MagicMock(timestamp=lambda: 100)},
            {"Key": "b.pdf", "LastModified": MagicMock(timestamp=lambda: 200)},
            {"Key": "c.pdf", "LastModified": MagicMock(timestamp=lambda: 300)},
        ]
    }
    new_keys, latest_ts = _list_new_keys(s3, since_ts=150)
    assert new_keys == ["b.pdf", "c.pdf"]
    assert latest_ts == 300


def test_list_new_keys_empty_when_all_old():
    s3 = MagicMock()
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "a.pdf", "LastModified": MagicMock(timestamp=lambda: 100)},
        ]
    }
    new_keys, latest_ts = _list_new_keys(s3, since_ts=200)
    assert new_keys == []
    assert latest_ts == 200
