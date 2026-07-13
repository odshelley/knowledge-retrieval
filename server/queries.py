"""Cypher constants + pure validation/shaping for the kg MCP tools.

All queries are read-only. Provenance note: paper chunks carry `position` (int order
within the paper) — cite as (paper, chunk position). Book chunks additionally carry
`chapter`/`section` titles and `source_type='book'` — cite as (book, chapter, section).
"""
from __future__ import annotations

import re

from pipeline.graph.schema import NODE_TYPES, PATTERNS

VALID_EXPAND = ("none", "local", "concepts")
VALID_KINDS = ("theorem", "lemma", "proposition", "corollary")

_WRITE_CLAUSE = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|FOREACH|LOAD\s+CSV)\b", re.IGNORECASE)

# Procedures the READ_ACCESS session STILL permits (they are read-mode) but that fetch external
# URLs, touch the filesystem, run dynamic Cypher, or administer the DBMS. For these the driver's
# write-routing is not a backstop, so this guard is the only defense (e.g. SSRF via apoc.load.*).
_BANNED_PROC = re.compile(
    r"\b(apoc\.(load|import|export|cypher|periodic|trigger|refactor|create|merge|atomic|systemdb|do)"
    r"|dbms\.|db\.(create|drop|await))",
    re.IGNORECASE)
_IN_TRANSACTIONS = re.compile(r"\bIN\s+TRANSACTIONS\b", re.IGNORECASE)

_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"//[^\n]*")
_STRING_LIT = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"")


def _strip_noise(cypher: str) -> str:
    """Blank out comments and string literals before keyword/procedure scanning, so a write
    clause can't hide in a comment (LOAD/**/CSV) and a literal containing a keyword
    (CONTAINS 'level set') can't trip a false positive. Comments collapse to a space so
    surrounding tokens can't fuse; literals collapse to an empty quote."""
    s = _BLOCK_COMMENT.sub(" ", cypher)
    s = _LINE_COMMENT.sub(" ", s)
    return _STRING_LIT.sub("''", s)


def check_read_only(cypher: str) -> None:
    """Guard for run_cypher. The driver READ_ACCESS session blocks node/edge writes, but it does
    NOT block read-mode external fetch (LOAD CSV, apoc.load.*), dynamic Cypher, or admin
    procedures, so this guard is the real defense against those (SSRF/side effects). Comments and
    string literals are stripped first so the checks are neither bypassable nor false-tripping."""
    s = _strip_noise(cypher)
    m = _WRITE_CLAUSE.search(s)
    if m:
        raise ValueError(f"run_cypher is read-only; found write clause {m.group(1)!r}")
    m = _BANNED_PROC.search(s)
    if m:
        raise ValueError(
            f"run_cypher forbids procedure {m.group(0)!r} (external fetch / write / admin)")
    if _IN_TRANSACTIONS.search(s):
        raise ValueError("run_cypher forbids CALL ... IN TRANSACTIONS")


def render_schema() -> str:
    lines = ["Node labels: " + ", ".join(NODE_TYPES), "", "Relationships:"]
    lines += [f"(:{s})-[:{r}]->(:{e})" for s, r, e in PATTERNS]
    lines += ["", "Key properties: Paper{id,title,year,doi,arxiv_id,abstract,tldr,citation_count}, "
              "Concept{name,description,tags}, Definition{id,term,statement}, "
              "Result{id,kind,name,statement}, Chunk{id,text,position}, "
              "Notation{id,symbol_latex,meaning}, Proof{id,sketch,technique}, "
              "Book{id,title}, Chapter/Section{id,title}.",
              "Only Topic/Researcher/Idea are in the vocabulary but not yet populated."]
    return "\n".join(lines)


def validate_top_k(k: int | None) -> int:
    if k is None:
        return 8
    return max(1, min(25, int(k)))


def validate_expand(expand: str | None) -> str:
    if expand is None:
        return "local"
    if expand not in VALID_EXPAND:
        raise ValueError(f"expand must be one of {VALID_EXPAND}")
    return expand


def validate_depth(depth: int | None) -> int:
    if depth is None:
        return 3
    return max(1, min(5, int(depth)))


def validate_kind(kind: str | None) -> str | None:
    if kind is None:
        return None
    if kind not in VALID_KINDS:
        raise ValueError(f"kind must be one of {VALID_KINDS}")
    return kind


def merge_paper_hits(title_rows: list[dict], vector_rows: list[dict], top_k: int) -> list[dict]:
    """Union of exact-title and vector hits, title matches first, dedup by id."""
    seen, out = set(), []
    for row in list(title_rows) + list(vector_rows):
        if row["id"] in seen:
            continue
        seen.add(row["id"])
        out.append(row)
    return out[:top_k]


_LUCENE_SPECIAL = set('+-&|!(){}[]^"~*?:\\/')
_LUCENE_OPERATORS = {"AND", "OR", "NOT"}


def lucene_escape(q: str) -> str:
    """Turn a natural-language question into a literal term query. Escapes Lucene special
    characters AND neutralizes the bare boolean keywords AND/OR/NOT (lowercased), which are not
    special characters but are parsed as operators by the classic QueryParser — leaving them
    live turns the search into an unintended boolean query or, in an invalid position (leading
    OR, trailing AND), throws a ParseException that fails the whole fulltext call."""
    escaped = "".join("\\" + ch if ch in _LUCENE_SPECIAL else ch for ch in q)
    return " ".join(tok.lower() if tok in _LUCENE_OPERATORS else tok
                    for tok in escaped.split(" "))


_RRF_K = 60


def merge_chunk_hits(vector_rows: list[dict], fulltext_rows: list[dict],
                     top_k: int) -> list[dict]:
    """Hybrid merge via reciprocal-rank fusion. Vector scores (0..1 cosine) and Lucene scores
    (unbounded) are incomparable, and per-list max-normalization inflates a single weak hit to
    1.0 — a lone marginal keyword match would then outrank strong vector hits. RRF uses only each
    hit's RANK within its own list (1/(k+rank)), so a chunk is promoted by appearing high in one
    list and/or in both, never by a lone list's absolute score. Dedup by chunk_id summing the
    per-list contributions; the merged `score` is the RRF score."""
    fused: dict[str, dict] = {}
    for rows in (vector_rows, fulltext_rows):
        for rank, r in enumerate(rows):
            contrib = 1.0 / (_RRF_K + rank)
            cur = fused.get(r["chunk_id"])
            if cur is None:
                fused[r["chunk_id"]] = {**r, "score": contrib}
            else:
                cur["score"] += contrib
    return sorted(fused.values(), key=lambda r: -r["score"])[:top_k]


FULLTEXT_SEARCH = """
CALL db.index.fulltext.queryNodes('chunk_text', $q) YIELD node, score
MATCH (node)-[:BELONGS_TO]->(:Document)<-[:HAS_DOCUMENT]-(src)
WHERE (src:Paper OR src:Book) AND ($paper_id IS NULL OR src.id = $paper_id)
OPTIONAL MATCH (node)-[:PART_OF]->(sec:Section)<-[:HAS_SECTION]-(chp:Chapter)
RETURN node.id AS chunk_id, node.text AS text, node.position AS position, score,
       src.id AS paper_id, src.title AS paper_title, src.year AS year,
       CASE WHEN src:Book THEN 'book' ELSE 'paper' END AS source_type,
       chp.title AS chapter, sec.title AS section
ORDER BY score DESC
LIMIT $top_k
"""

VECTOR_SEARCH = """
CALL db.index.vector.queryNodes('chunk_embedding', $k, $embedding)
YIELD node, score
MATCH (node)-[:BELONGS_TO]->(:Document)<-[:HAS_DOCUMENT]-(src)
WHERE (src:Paper OR src:Book) AND ($paper_id IS NULL OR src.id = $paper_id)
OPTIONAL MATCH (node)-[:PART_OF]->(sec:Section)<-[:HAS_SECTION]-(chp:Chapter)
RETURN node.id AS chunk_id, node.text AS text, node.position AS position, score,
       src.id AS paper_id, src.title AS paper_title, src.year AS year,
       CASE WHEN src:Book THEN 'book' ELSE 'paper' END AS source_type,
       chp.title AS chapter, sec.title AS section
ORDER BY score DESC
LIMIT $top_k
"""

EXPAND_LOCAL = """
UNWIND $paper_ids AS pid
MATCH (p:Paper {id: pid})
OPTIONAL MATCH (p)-[:DISCUSSES]->(c:Concept)
WITH p, collect(DISTINCT c.name)[..10] AS concepts
OPTIONAL MATCH (p)-[:STATES]->(d:Definition)
WITH p, concepts, collect(DISTINCT {id: d.id, term: d.term})[..10] AS definitions
OPTIONAL MATCH (p)-[:STATES]->(r:Result)
WITH p, concepts, definitions,
     collect(DISTINCT {id: r.id, kind: r.kind, name: r.name})[..10] AS results
OPTIONAL MATCH (p)-[:CITES]->(o:Paper)
WITH p, concepts, definitions, results,
     collect(DISTINCT {id: o.id, title: o.title})[..5] AS cites
OPTIONAL MATCH (i:Paper)-[:CITES]->(p)
RETURN p.id AS paper_id, concepts, definitions, results, cites,
       collect(DISTINCT {id: i.id, title: i.title})[..5] AS cited_by
"""

TOP_CONCEPTS_FOR_PAPERS = """
UNWIND $paper_ids AS pid
MATCH (:Paper {id: pid})-[:DISCUSSES]->(c:Concept)
RETURN c.name AS name, count(*) AS freq
ORDER BY freq DESC LIMIT 5
"""

EXPAND_CONCEPTS = """
UNWIND $names AS cname
MATCH (c:Concept {name: cname})
OPTIONAL MATCH (d:Definition)-[:DEFINES]->(c)
OPTIONAL MATCH (dp:Paper)-[:STATES]->(d)
WITH c, collect(DISTINCT {id: d.id, term: d.term, statement: d.statement,
                          paper_id: dp.id, paper_title: dp.title})[..5] AS definitions
OPTIONAL MATCH (r:Result)-[:USES]->(c)
OPTIONAL MATCH (rp:Paper)-[:STATES]->(r)
WITH c, definitions,
     collect(DISTINCT {id: r.id, kind: r.kind, name: r.name,
                       paper_id: rp.id})[..5] AS results
OPTIONAL MATCH (p:Paper)-[:DISCUSSES]->(c)
RETURN c.name AS concept, definitions, results,
       collect(DISTINCT {id: p.id, title: p.title})[..5] AS papers
"""

GET_PAPER = """
MATCH (p:Paper)
WHERE p.id = $key OR p.doi = $key OR p.arxiv_id = $key
   OR toLower(p.title) = toLower($key)
OPTIONAL MATCH (a:Author)-[:AUTHORED]->(p)
OPTIONAL MATCH (p)-[:HAS_SUMMARY]->(sm:Summary)
RETURN p{.id, .title, .year, .doi, .arxiv_id, .s2_id, .abstract, .tldr,
         .citation_count, .influential_citation_count} AS paper,
       collect(DISTINCT a.name) AS authors, sm.json AS summary_json
LIMIT 1
"""

TITLE_MATCH = """
MATCH (p:Paper)
WHERE toLower(p.title) CONTAINS toLower($q)
RETURN p.id AS id, p.title AS title, p.year AS year, p.tldr AS tldr, 1.0 AS score
LIMIT $top_k
"""

PAPER_VECTOR_AGG = """
CALL db.index.vector.queryNodes('chunk_embedding', $k, $embedding)
YIELD node, score
MATCH (node)-[:BELONGS_TO]->(:Document)<-[:HAS_DOCUMENT]-(p:Paper)
WITH p, max(score) AS score
RETURN p.id AS id, p.title AS title, p.year AS year, p.tldr AS tldr, score
ORDER BY score DESC LIMIT $top_k
"""

GET_CONCEPT = """
MATCH (c:Concept)
WHERE toLower(c.name) = toLower($name)
OPTIONAL MATCH (d:Definition)-[:DEFINES]->(c)
OPTIONAL MATCH (dp:Paper)-[:STATES]->(d)
WITH c, collect(DISTINCT {id: d.id, term: d.term, statement: d.statement,
                          paper_id: dp.id, paper_title: dp.title})[..10] AS definitions
OPTIONAL MATCH (p:Paper)-[:DISCUSSES]->(c)
WITH c, definitions,
     collect(DISTINCT {id: p.id, title: p.title, year: p.year})[..15] AS papers
OPTIONAL MATCH (p2:Paper)-[:DISCUSSES]->(c)
OPTIONAL MATCH (p2)-[:DISCUSSES]->(other:Concept)
WHERE other.name <> c.name
WITH c, definitions, papers, other.name AS oname, count(DISTINCT p2) AS shared
ORDER BY shared DESC
WITH c, definitions, papers, collect(oname)[..10] AS related_concepts
OPTIONAL MATCH (ch:Chunk)-[:MENTIONS]->(c)
WITH c, definitions, papers, related_concepts, collect(DISTINCT ch) AS chs
WITH c, definitions, papers, related_concepts,
     [x IN chs | {chunk_id: x.id, position: x.position,
                  text: left(x.text, 600)}][..5] AS supporting_chunks
RETURN c.name AS name, c.tags AS tags, c.description AS description,
       definitions, papers, related_concepts, supporting_chunks
"""

SEARCH_CONCEPTS = """
CALL db.index.vector.queryNodes('concept_embedding', $k, $embedding)
YIELD node, score
RETURN node.name AS name, node.description AS description,
       node.tags AS tags, score
ORDER BY score DESC
LIMIT $top_k
"""

GET_RESULTS = """
MATCH (r:Result)
WHERE ($concept IS NULL OR EXISTS {
        MATCH (r)-[:USES]->(c:Concept) WHERE toLower(c.name) = toLower($concept) })
  AND ($paper_id IS NULL OR EXISTS {
        MATCH (:Paper {id: $paper_id})-[:STATES]->(r) })
  AND ($kind IS NULL OR r.kind = $kind)
MATCH (sp:Paper)-[:STATES]->(r)
RETURN r.id AS id, r.kind AS kind, r.name AS name, r.statement AS statement,
       sp.id AS paper_id, sp.title AS paper_title
LIMIT 25
"""


def dependency_chain_cypher(depth: int) -> str:
    """Variable-length hops can't be parameterized; interpolate a CLAMPED int only."""
    d = validate_depth(depth)
    return f"""
MATCH (r:Result {{id: $result_id}})
OPTIONAL MATCH (r)-[:DEPENDS_ON*1..{d}]->(dep:Result)
WITH r, collect(DISTINCT dep) AS deps
UNWIND ([r] + deps) AS node
MATCH (p:Paper)-[:STATES]->(node)
OPTIONAL MATCH (node)-[:USES]->(c:Concept)
OPTIONAL MATCH (node)-[:DEPENDS_ON]->(d2:Result)
RETURN node.id AS id, node.kind AS kind, node.name AS name, node.statement AS statement,
       p.id AS paper_id, p.title AS paper_title,
       collect(DISTINCT c.name) AS uses_concepts,
       collect(DISTINCT d2.id) AS depends_on
"""


GET_CITATIONS = """
MATCH (p:Paper {id: $paper_id})
CALL (p) {
  MATCH (p)-[:CITES]->(o:Paper) WHERE $direction = 'out' RETURN o
  UNION
  MATCH (o:Paper)-[:CITES]->(p) WHERE $direction = 'in' RETURN o
}
RETURN o.id AS id, o.title AS title, o.year AS year, o.doi AS doi,
       o.citation_count AS citation_count
ORDER BY coalesce(o.year, 0) DESC LIMIT 50
"""

OVERVIEW_COUNTS = """
CALL { MATCH (p:Paper) RETURN count(p) AS papers }
CALL { MATCH (c:Chunk) RETURN count(c) AS chunks }
CALL { MATCH (co:Concept) RETURN count(co) AS concepts }
CALL { MATCH (d:Definition) RETURN count(d) AS definitions }
CALL { MATCH (r:Result) RETURN count(r) AS results }
RETURN papers, chunks, concepts, definitions, results
"""

OVERVIEW_TOP_CONCEPTS = """
MATCH (p:Paper)-[:DISCUSSES]->(c:Concept)
RETURN c.name AS name, count(p) AS papers
ORDER BY papers DESC LIMIT 20
"""

OVERVIEW_RECENT = """
MATCH (p:Paper)
RETURN p.id AS id, p.title AS title, p.year AS year
ORDER BY coalesce(p.year, 0) DESC LIMIT 10
"""
