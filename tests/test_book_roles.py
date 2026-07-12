from pipeline.books.roles import EXTRACT_ROLES, classify_roles, resolve_ambiguous

WILLIAMS = [
    {"number": 1, "title": "Title", "page_start": 1, "page_end": 1},
    {"number": 2, "title": "Copyright", "page_start": 2, "page_end": 2},
    {"number": 3, "title": "Contents", "page_start": 3, "page_end": 8},
    {"number": 4, "title": "Preface", "page_start": 9, "page_end": 10},
    {"number": 5, "title": "A Question of Terminology", "page_start": 11, "page_end": 11},
    {"number": 6, "title": "A Guide to Notation", "page_start": 12, "page_end": 14},
    {"number": 7, "title": "0 A Branching-Process Example", "page_start": 15, "page_end": 27},
    {"number": 8, "title": "Part A:  Foundations", "page_start": 28, "page_end": 96},
    {"number": 9, "title": "Part B: Martingale Theory", "page_start": 97, "page_end": 185},
    {"number": 10, "title": "Part C: Characteristic Functions", "page_start": 186, "page_end": 205},
    {"number": 11, "title": "Appendices", "page_start": 206, "page_end": 237},
    {"number": 12, "title": "E Exercises", "page_start": 238, "page_end": 256},
    {"number": 13, "title": "References", "page_start": 257, "page_end": 259},
    {"number": 14, "title": "Index", "page_start": 260, "page_end": 265},
]


def test_williams_roles():
    roles = classify_roles(WILLIAMS)
    assert roles[1] == "front_matter"       # Title
    assert roles[2] == "front_matter"       # Copyright
    assert roles[3] == "front_matter"       # Contents
    assert roles[4] == "front_matter"       # Preface
    assert roles[6] == "notation_guide"     # A Guide to Notation
    assert roles[8] == "content"            # Part A
    assert roles[11] == "content"           # Appendices hold real math
    assert roles[12] == "exercises"
    assert roles[13] == "back_matter"       # References
    assert roles[14] == "back_matter"       # Index


def test_unmatched_short_chapter_is_ambiguous():
    roles = classify_roles([{"number": 1, "title": "Interlude", "page_start": 1,
                             "page_end": 3}])
    # Unrecognized titles are ambiguous (None) — the asset resolves via LLM or
    # defaults to content, never silently skips.
    assert roles[1] is None


def test_extract_roles_set():
    assert EXTRACT_ROLES == frozenset({"content", "notation_guide", "exercises"})


class _Block:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class _Resp:
    def __init__(self, text):
        self.content = [_Block(text)]


class _StubMessages:
    def __init__(self, reply=None, raises=None):
        self._reply = reply
        self._raises = raises

    def create(self, **kwargs):
        if self._raises is not None:
            raise self._raises
        return _Resp(self._reply)


class _StubClient:
    def __init__(self, reply=None, raises=None):
        self.messages = _StubMessages(reply=reply, raises=raises)


CHAPTERS = [{"number": 1, "title": "Interlude", "page_start": 1, "page_end": 3}]


def test_resolve_ambiguous_parses_stub_response():
    client = _StubClient(reply='{"1": "exercises"}')
    resolved = resolve_ambiguous(client, "stub-model", CHAPTERS, [1], timeout=1.0)
    assert resolved == {1: "exercises"}


def test_resolve_ambiguous_unknown_role_defaults_to_content():
    client = _StubClient(reply='{"1": "not_a_real_role"}')
    resolved = resolve_ambiguous(client, "stub-model", CHAPTERS, [1], timeout=1.0)
    assert resolved == {1: "content"}


def test_resolve_ambiguous_never_raises_on_client_failure():
    client = _StubClient(raises=RuntimeError("network down"))
    resolved = resolve_ambiguous(client, "stub-model", CHAPTERS, [1], timeout=1.0)
    assert resolved == {1: "content"}


def test_resolve_ambiguous_missing_number_in_reply_defaults_to_content():
    client = _StubClient(reply='{"2": "exercises"}')  # reply omits pending chapter 1
    resolved = resolve_ambiguous(client, "stub-model", CHAPTERS, [1], timeout=1.0)
    assert resolved == {1: "content"}
