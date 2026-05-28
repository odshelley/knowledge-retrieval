from pipeline.resolver import decide, Decision


def test_decide_merges_above_high():
    assert decide(0.95, high=0.9, low=0.6) == Decision.MERGE


def test_decide_creates_below_low():
    assert decide(0.4, high=0.9, low=0.6) == Decision.CREATE


def test_decide_ambiguous_band_creates_and_flags():
    assert decide(0.75, high=0.9, low=0.6) == Decision.CREATE_FLAGGED


def test_decide_at_high_threshold():
    assert decide(0.9, high=0.9, low=0.6) == Decision.MERGE


def test_decide_at_low_threshold():
    assert decide(0.6, high=0.9, low=0.6) == Decision.CREATE_FLAGGED
