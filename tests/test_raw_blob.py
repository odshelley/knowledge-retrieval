from unittest.mock import MagicMock
from pipeline.assets.raw_blob import _upload_if_absent

def test_upload_if_absent_skips_when_present():
    s3 = MagicMock()
    s3.head_object.return_value = {}  # exists
    uploaded = _upload_if_absent(s3, "raw", "k.pdf", b"data")
    assert uploaded is False
    s3.put_object.assert_not_called()

def test_upload_if_absent_uploads_when_missing():
    import botocore.exceptions
    s3 = MagicMock()
    s3.head_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "404"}}, "HeadObject")
    uploaded = _upload_if_absent(s3, "raw", "k.pdf", b"data")
    assert uploaded is True
    s3.put_object.assert_called_once()
