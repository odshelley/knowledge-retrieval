import pytest
from pipeline.resolution.canonicalize import canonical_key


@pytest.mark.parametrize("a,b", [
    ("Bridge Matching", "Bridge Matching (BM)"),
    ("Brownian Bridge", "Brownian bridge"),
    ("Fokker-Planck equation", "Fokker–Planck equation"),
    ("Fokker-Planck (FP)", "Fokker-Planck"),
    ("Method of Moments (MoM)", "Method of Moments"),
    ("Girsanov's theorem", "Girsanov’s theorem"),
])
def test_obvious_duplicates_share_key(a, b):
    assert canonical_key(a) == canonical_key(b)


@pytest.mark.parametrize("a,b", [
    ("DDPM", "DDPM++"),
    ("DSBM-IMF", "DSBM-IMF+"),
    ("Corrector algorithm (VE SDE)", "Corrector algorithm (VP SDE)"),
    ("Score-Based Generative Model (SGM)", "Score-Based Generative Model"),
    ("G(1, c^2)", "G(t, c^2)"),
    ("Schrodinger Bridge", "Schrodinger Bridges"),
])
def test_distinct_concepts_keep_distinct_keys(a, b):
    assert canonical_key(a) != canonical_key(b)


def test_min_length_guard_returns_casefolded_original():
    assert canonical_key("G") == "g"
    assert canonical_key("SB") == "sb"


def test_idempotent():
    k = canonical_key("Schrödinger Bridge (SB)")
    assert canonical_key(k) == k or canonical_key(k) == canonical_key("Schrödinger Bridge")


def test_canonical_key_ignores_nul_bytes():
    # Defense in depth for the NUL-byte incident: keys derived from tainted names must
    # never carry \x00 into Postgres parameters.
    from pipeline.resolution.canonicalize import canonical_key
    assert "\x00" not in canonical_key("L\x00evy process\x00")
    assert canonical_key("L\x00evy process") == canonical_key("Levy process")
