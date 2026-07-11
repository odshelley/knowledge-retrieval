from server.auth import RateLimiter, hash_token, parse_tokens, verify_token


def test_parse_tokens_roundtrip():
    entries = parse_tokens("osian:ab12:deadbeef,ffion:cd34:cafebabe")
    assert entries == {"osian": ("ab12", "deadbeef"), "ffion": ("cd34", "cafebabe")}


def test_parse_tokens_empty_and_whitespace():
    assert parse_tokens("") == {}
    assert parse_tokens(" ") == {}


def test_verify_token_accepts_valid():
    token = "kg_osian_0123456789abcdef0123456789abcdef"
    entries = {"osian": ("s4lt", hash_token("s4lt", token))}
    assert verify_token(token, entries) == "osian"


def test_verify_token_rejects_invalid():
    entries = {"osian": ("s4lt", hash_token("s4lt", "kg_osian_right"))}
    assert verify_token("kg_osian_wrong", entries) is None
    assert verify_token("", entries) is None


def test_rate_limiter_blocks_after_limit():
    rl = RateLimiter(limit_per_min=3)
    assert all(rl.allow("osian", now=10.0 + i) for i in range(3))
    assert rl.allow("osian", now=13.0) is False
    assert rl.allow("ffion", now=13.0) is True      # per-token buckets
    assert rl.allow("osian", now=71.0) is True      # window expired


def test_mint_produces_verifiable_entry():
    from scripts.issue_token import mint
    from server.auth import parse_tokens, verify_token

    token, entry = mint("ffion")
    assert token.startswith("kg_ffion_") and len(token) == len("kg_ffion_") + 32
    assert verify_token(token, parse_tokens(entry)) == "ffion"
