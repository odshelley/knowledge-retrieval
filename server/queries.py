"""Cypher constants + pure validation/shaping for the kg MCP tools.

All queries are read-only. Provenance note: paper chunks carry `position` (int order
within the paper), not a section — cite as (paper, chunk position).
"""
from __future__ import annotations

VALID_EXPAND = ("none", "local", "concepts")
VALID_KINDS = ("theorem", "lemma", "proposition", "corollary")


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


VECTOR_SEARCH = """
CALL db.index.vector.queryNodes('chunk_embedding', $k, $embedding)
YIELD node, score
MATCH (node)-[:BELONGS_TO]->(:Document)<-[:HAS_DOCUMENT]-(p:Paper)
WHERE $paper_id IS NULL OR p.id = $paper_id
RETURN node.id AS chunk_id, node.text AS text, node.position AS position, score,
       p.id AS paper_id, p.title AS paper_title, p.year AS year
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
RETURN p{.id, .title, .year, .doi, .arxiv_id, .abstract, .tldr,
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
RETURN c.name AS name, c.tags AS tags, definitions, papers,
       collect(oname)[..10] AS related_concepts
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
MATCH (p:Paper) WITH count(p) AS papers
MATCH (c:Chunk) WITH papers, count(c) AS chunks
MATCH (co:Concept) WITH papers, chunks, count(co) AS concepts
MATCH (d:Definition) WITH papers, chunks, concepts, count(d) AS definitions
MATCH (r:Result)
RETURN papers, chunks, concepts, definitions, count(r) AS results
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
