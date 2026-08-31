from pipeline.zotero.items import (
    attachment_item, book_item, match_existing, paper_item, publisher_doi, split_creator,
)

JOURNAL_PAPER = {
    "title": "A Multi-agent Targeted Trading Equilibrium with Transaction Costs",
    "year": 2023,
    "doi": "10.1137/22M1542982",
    "arxiv_id": "2306.08519",
    "venue": "SIAM Journal on Financial Mathematics",
    "journal_name": "SIAM J. Financial Math.",
    "volume": "15",
    "pages": "161-193",
    "publication_types": ["JournalArticle"],
    "abstract": "We study...",
    "tldr": "A short generated summary.",
}

# Real S2 record: journal.name is "ArXiv" for a paper published in MOR.
MISLEADING_JOURNAL_PAPER = {
    "title": "Bridging Bayesian and Minimax Mean Square Error Estimation",
    "year": 2021,
    "doi": "10.1287/moor.2021.1176",
    "arxiv_id": "1911.03539",
    "venue": "Mathematics of Operations Research",
    "journal_name": "ArXiv",
    "volume": "abs/1911.03539",
    "pages": None,
    "publication_types": ["JournalArticle"],
}


def test_split_creator_two_part_name():
    assert split_creator("Bruno Bouchard") == {
        "creatorType": "author", "firstName": "Bruno", "lastName": "Bouchard"}


def test_split_creator_three_part_name_splits_on_last_space():
    assert split_creator("Jean Pierre Fouque") == {
        "creatorType": "author", "firstName": "Jean Pierre", "lastName": "Fouque"}


def test_split_creator_single_token_has_no_fieldmode():
    """fieldMode is an internal Zotero client concept, not Web API v3 — verified against
    15 live single-field creators, none of which carry it."""
    got = split_creator("Plato")
    assert got == {"creatorType": "author", "name": "Plato"}
    assert "fieldMode" not in got


def test_journal_article_uses_venue_not_the_abbreviation():
    item = paper_item(JOURNAL_PAPER, ["Bruno Bouchard"], "COLL1")
    assert item["itemType"] == "journalArticle"
    assert item["publicationTitle"] == "SIAM Journal on Financial Mathematics"
    assert item["journalAbbreviation"] == "SIAM J. Financial Math."
    assert item["volume"] == "15"
    assert item["pages"] == "161-193"
    assert item["DOI"] == "10.1137/22M1542982"
    assert item["date"] == "2023"
    assert item["collections"] == ["COLL1"]
    assert item["abstractNote"] == "We study..."


def test_arxiv_masquerading_as_a_journal_is_discarded():
    """S2 sometimes reports journal.name="ArXiv" with an "abs/..." volume for a paper
    genuinely published elsewhere. That must never reach the user's library."""
    item = paper_item(MISLEADING_JOURNAL_PAPER, [], "COLL1")
    assert item["publicationTitle"] == "Mathematics of Operations Research"
    assert "journalAbbreviation" not in item
    assert "volume" not in item, "abs/1911.03539 is not a volume"


def test_journal_abbreviation_omitted_when_identical_to_venue():
    paper = dict(JOURNAL_PAPER, journal_name="SIAM Journal on Financial Mathematics")
    assert "journalAbbreviation" not in paper_item(paper, [], "C")


def test_tldr_is_never_written_to_zotero():
    item = paper_item(JOURNAL_PAPER, ["Bruno Bouchard"], "COLL1")
    assert "A short generated summary." not in str(item)


def test_conference_wins_over_journal_article():
    paper = dict(JOURNAL_PAPER, publication_types=["JournalArticle", "Conference"])
    item = paper_item(paper, [], "COLL1")
    assert item["itemType"] == "conferencePaper"
    assert item["proceedingsTitle"] == "SIAM Journal on Financial Mathematics"
    assert "publicationTitle" not in item


def test_publisher_doi_alone_implies_journal_article():
    paper = dict(JOURNAL_PAPER, publication_types=[])
    assert paper_item(paper, [], "C")["itemType"] == "journalArticle"


def test_venue_falls_back_to_journal_name_when_venue_absent():
    paper = dict(JOURNAL_PAPER, venue=None)
    assert paper_item(paper, [], "C")["publicationTitle"] == "SIAM J. Financial Math."


def test_arxiv_only_maps_to_preprint():
    paper = {"title": "A Preprint", "year": 2025, "arxiv_id": "2503.13804",
             "doi": None, "publication_types": []}
    item = paper_item(paper, [], "C")
    assert item["itemType"] == "preprint"
    assert item["repository"] == "arXiv"
    assert item["archiveID"] == "arXiv:2503.13804"
    assert item["url"] == "https://arxiv.org/abs/2503.13804"


def test_arxiv_doi_is_not_a_publisher_doi():
    paper = {"title": "T", "year": 2023, "arxiv_id": "2305.16261",
             "doi": "10.48550/arXiv.2305.16261", "publication_types": []}
    assert paper_item(paper, [], "C")["itemType"] == "preprint"
    assert publisher_doi("10.48550/arXiv.2305.16261") is None


def test_ssrn_doi_is_not_a_publisher_doi():
    assert publisher_doi("10.2139/ssrn.3594076") is None


def test_placeholder_doi_never_reaches_the_item():
    paper = {"title": "T", "year": 2017, "arxiv_id": None,
             "doi": "10.1145/NNNNNNN.NNNNNNN", "publication_types": []}
    item = paper_item(paper, [], "C")
    assert item["itemType"] == "preprint"
    assert "DOI" not in item


def test_identifierless_paper_maps_to_bare_preprint():
    paper = {"title": "Lecture Notes", "year": None, "arxiv_id": None, "doi": None,
             "publication_types": []}
    item = paper_item(paper, ["Ada Lovelace"], "C")
    assert item["itemType"] == "preprint"
    assert item["title"] == "Lecture Notes"
    assert "date" not in item
    assert "repository" not in item


def test_book_mapping():
    book = {"title": "Probability with Martingales", "year": 1991,
            "publisher": "Cambridge University Press", "edition": "1st",
            "isbn": "9780521406055"}
    item = book_item(book, ["David Williams"], "BOOKS")
    assert item["itemType"] == "book"
    assert item["publisher"] == "Cambridge University Press"
    assert item["edition"] == "1st"
    assert item["ISBN"] == "9780521406055"
    assert item["collections"] == ["BOOKS"]


# --- preprint hosts (SSRN, arXiv, CoRR) masquerading as journals -----------------

# Real corpus record: S2 tags an SSRN working paper as JournalArticle in a "journal"
# called Social Science Research Network.
SSRN_PAPER = {
    "title": "Continuous-time Equilibrium Returns in Markets with Price Impact and "
             "Transaction Costs",
    "year": 2024,
    "doi": "10.2139/ssrn.4839073",
    "arxiv_id": "2405.14418",
    "venue": "Social Science Research Network",
    "journal_name": "SSRN Electronic Journal",
    "volume": None,
    "pages": None,
    "publication_types": ["JournalArticle"],
}


def test_ssrn_masquerading_as_a_journal_maps_to_preprint():
    """S2 says JournalArticle for an SSRN listing, but that is S2 being wrong: an SSRN
    DOI is not a publisher DOI, so this must be filed as a preprint, not a journal
    article in a journal called "Social Science Research Network"."""
    item = paper_item(SSRN_PAPER, [], "COLL1")
    assert item["itemType"] == "preprint"
    assert "publicationTitle" not in item
    assert "journalAbbreviation" not in item
    assert "volume" not in item
    assert "pages" not in item
    assert item["repository"] == "arXiv"
    assert item["archiveID"] == "arXiv:2405.14418"


def test_ssrn_doi_without_journal_claim_stays_preprint():
    """Pin the pre-existing (already correct) behaviour for the second real corpus case,
    so a future change to the preprint-host logic can't regress it."""
    paper = {"title": "T", "year": 2020, "doi": "10.2139/ssrn.3594076",
             "arxiv_id": "2005.02633", "publication_types": []}
    item = paper_item(paper, [], "C")
    assert item["itemType"] == "preprint"
    assert item["archiveID"] == "arXiv:2005.02633"


def test_real_publisher_doi_wins_over_preprint_host_venue():
    """A genuine publisher DOI is the trustworthy signal that a paper was actually
    published — an odd/preprint-host-shaped venue string alone must not downgrade it."""
    paper = dict(JOURNAL_PAPER, venue="SSRN Electronic Journal")
    assert paper_item(paper, [], "C")["itemType"] == "journalArticle"


def test_arxiv_org_venue_is_recognized_as_a_preprint_host():
    paper = {"title": "T", "year": 2020, "doi": None, "arxiv_id": "2001.00001",
             "venue": "arxiv.org", "journal_name": None, "publication_types": ["JournalArticle"]}
    assert paper_item(paper, [], "C")["itemType"] == "preprint"


def test_corr_journal_name_is_recognized_as_a_preprint_host():
    paper = {"title": "T", "year": 2020, "doi": None, "arxiv_id": "2001.00002",
             "venue": None, "journal_name": "CoRR", "publication_types": ["JournalArticle"]}
    assert paper_item(paper, [], "C")["itemType"] == "preprint"


def test_attachment_item_includes_required_tags_and_relations():
    """The API docs list tags and relations as required on item creation."""
    att = attachment_item("PARENT1", "A Study - Lovelace - 2020.pdf")
    assert att == {
        "itemType": "attachment", "parentItem": "PARENT1", "linkMode": "imported_file",
        "title": "A Study - Lovelace - 2020.pdf",
        "filename": "A Study - Lovelace - 2020.pdf",
        "contentType": "application/pdf",
        "tags": [], "relations": {},
    }
    assert "collections" not in att, "child items cannot be collection members"


# --- dedup matching ---------------------------------------------------------------

CANDIDATES = [
    {"key": "K_DOI", "data": {"DOI": "10.1137/22M1542982", "title": "Something Else"}},
    {"key": "K_ARXIV", "data": {"archiveID": "arXiv:2306.08519", "title": "Other"}},
    {"key": "K_TITLE", "data": {"title": "A Multi-agent Targeted Trading Equilibrium"}},
]


def test_doi_match_wins():
    assert match_existing(CANDIDATES, "10.1137/22M1542982", "2306.08519",
                          "A Multi-agent Targeted Trading Equilibrium") == "K_DOI"


def test_arxiv_match_beats_title():
    assert match_existing(CANDIDATES, None, "2306.08519",
                          "A Multi-agent Targeted Trading Equilibrium") == "K_ARXIV"


def test_title_match_is_the_last_resort():
    assert match_existing(CANDIDATES, None, None,
                          "A Multi-agent Targeted Trading Equilibrium") == "K_TITLE"


def test_title_match_is_case_and_whitespace_insensitive():
    assert match_existing(CANDIDATES, None, None,
                          "  A MULTI-AGENT   targeted Trading Equilibrium ") == "K_TITLE"


def test_arxiv_match_ignores_version_suffix():
    assert match_existing(CANDIDATES, None, "2306.08519v3", None) == "K_ARXIV"


def test_arxiv_match_also_reads_the_url_field():
    cands = [{"key": "K_URL", "data": {"url": "https://arxiv.org/abs/2503.13804"}}]
    assert match_existing(cands, None, "2503.13804", None) == "K_URL"


def test_arxiv_match_requires_digit_boundaries():
    """Regression: plain substring matching made 1707.08464 match 11707.084640, filing a
    paper against an unrelated item."""
    cands = [{"key": "K", "data": {"url": "https://arxiv.org/abs/11707.084640"}}]
    assert match_existing(cands, None, "1707.08464", None) is None
    cands2 = [{"key": "K", "data": {"url": "https://example.com/x2503.138040"}}]
    assert match_existing(cands2, None, "2503.13804", None) is None


def test_isbn_match_wins_for_books():
    cands = [{"key": "K_ISBN", "data": {"ISBN": "978-0-521-40605-5", "title": "Other"}},
             {"key": "K_TITLE", "data": {"title": "Probability with Martingales"}}]
    assert match_existing(cands, None, None, "Probability with Martingales",
                          isbn="9780521406055") == "K_ISBN"


def test_placeholder_doi_is_not_used_for_matching():
    cands = [{"key": "K", "data": {"DOI": "10.1145/NNNNNNN.NNNNNNN"}}]
    assert match_existing(cands, "10.1145/NNNNNNN.NNNNNNN", None, None) is None


def test_no_match_returns_none():
    assert match_existing(CANDIDATES, "10.9999/nope", "0000.00000", "Unrelated") is None


def test_empty_candidates_returns_none():
    assert match_existing([], "10.1007/x", "1707.08464", "T") is None
