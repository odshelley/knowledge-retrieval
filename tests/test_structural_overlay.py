from unittest.mock import MagicMock

from pipeline.assets.structural_overlay import build_overlay_payload


def test_build_overlay_payload_extracts_authors_and_topics():
    legacy_session = MagicMock()
    legacy_session.run.return_value = [
        {
            "authors": ["Alice Smith", "Bob Jones"],
            "topics": ["XVA", "deep BSDE"],
            "citations": ["foo_2018", "bar_2020"],
        }
    ]
    payload = build_overlay_payload(legacy_session, "burnett_2023_hva")
    assert payload["authors"] == ["Alice Smith", "Bob Jones"]
    assert "XVA" in payload["topics"]
    assert payload["citations"] == ["foo_2018", "bar_2020"]


def test_build_overlay_payload_handles_missing_paper():
    legacy_session = MagicMock()
    legacy_session.run.return_value = []
    payload = build_overlay_payload(legacy_session, "missing_id")
    assert payload == {"authors": [], "topics": [], "citations": []}
