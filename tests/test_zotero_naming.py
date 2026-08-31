from pipeline.zotero.naming import attachment_filename, surname


def test_surname_takes_the_last_whitespace_token():
    assert surname("Bruno Bouchard") == "Bouchard"
    assert surname("Jean-Pierre Fouque") == "Fouque"
    assert surname("Plato") == "Plato"
    assert surname("  Ada  Lovelace  ") == "Lovelace"


def test_one_author():
    assert attachment_filename("Deep BSDE", ["Bruno Bouchard"], 2019) == \
        "Deep BSDE - Bouchard - 2019.pdf"


def test_two_authors_joined_with_and():
    assert attachment_filename("Deep BSDE", ["Bruno Bouchard", "Ada Lovelace"], 2019) == \
        "Deep BSDE - Bouchard and Lovelace - 2019.pdf"


def test_three_or_more_authors_use_et_al():
    names = ["Bruno Bouchard", "Ada Lovelace", "Alan Turing"]
    assert attachment_filename("Deep BSDE", names, 2019) == \
        "Deep BSDE - Bouchard et al. - 2019.pdf"
    assert attachment_filename("Deep BSDE", names + ["Grace Hopper"], 2019) == \
        "Deep BSDE - Bouchard et al. - 2019.pdf"


def test_path_hostile_characters_are_replaced():
    got = attachment_filename("A/B: A Study", ["Ada Lovelace"], 2020)
    assert "/" not in got and ":" not in got
    assert got == "A-B- A Study - Lovelace - 2020.pdf"


def test_missing_year_omits_the_segment_and_separator():
    assert attachment_filename("Deep BSDE", ["Bruno Bouchard"], None) == \
        "Deep BSDE - Bouchard.pdf"


def test_missing_authors_omits_the_segment():
    assert attachment_filename("Deep BSDE", [], 2019) == "Deep BSDE - 2019.pdf"


def test_missing_title_falls_back_to_untitled():
    assert attachment_filename(None, ["Bruno Bouchard"], 2019) == "Untitled - Bouchard - 2019.pdf"


def test_long_title_truncates_under_200_bytes():
    got = attachment_filename("Lorem ipsum dolor sit amet " * 20, ["Ada Lovelace"], 2020)
    assert len(got.encode("utf-8")) <= 200
    assert got.endswith(" - Lovelace - 2020.pdf"), "author/year must survive truncation"


def test_whitespace_runs_collapse_and_edges_strip():
    assert attachment_filename("  A   Study  ", ["Ada Lovelace"], 2020) == \
        "A Study - Lovelace - 2020.pdf"


def test_trailing_dots_are_stripped_from_the_title():
    assert attachment_filename("A Study...", ["Ada Lovelace"], 2020) == \
        "A Study - Lovelace - 2020.pdf"
