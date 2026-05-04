"""Mirror the curated knowledge graph from the legacy alethograph Neo4j DB into the new DB.

Copies all Paper / Book / Author / Concept / Topic / Researcher / Idea nodes and
every relationship between them, preserving labels, relationship types, and
properties. Idempotent via MERGE on each label's stable key (see ``LABEL_KEY``).

This is a non-partitioned asset: it materialises once per ``bulk_reingest`` run
and seeds the structural backbone that the per-partition LLM pipeline
(``kg_extracted`` → ``structural_overlay`` → ``paper_summary``) layers chunks,
embeddings, and summaries onto.

Skipped: ``Chunk``, ``Document``, and any embedding/vector data — those are
managed by the per-partition kg_extracted pipeline in the new DB.
"""
from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

LABEL_KEY: dict[str, str] = {
    "Paper":      "id",
    "Book":       "id",
    "Author":     "name",
    "Concept":    "name",
    "Topic":      "name",
    "Researcher": "name",
    "Idea":       "id",
}

MIRRORED_LABELS = set(LABEL_KEY)


def _mirror_nodes(legacy_session, new_session, label: str, key: str) -> int:
    rows = list(legacy_session.run(f"MATCH (n:`{label}`) RETURN properties(n) AS props"))
    payload = [r["props"] for r in rows if r["props"] and r["props"].get(key) is not None]
    if not payload:
        return 0
    new_session.run(
        f"""
        UNWIND $rows AS row
        MERGE (n:`{label}` {{`{key}`: row.`{key}`}})
        SET n += row
        """,
        rows=payload,
    )
    return len(payload)


def _discover_patterns(legacy_session) -> list[tuple[str, str, str]]:
    rows = list(legacy_session.run(
        """
        MATCH (a)-[r]->(b)
        WITH labels(a) AS la, type(r) AS t, labels(b) AS lb
        UNWIND la AS l1 UNWIND lb AS l2
        WITH DISTINCT l1, t, l2 WHERE l1 IN $labels AND l2 IN $labels
        RETURN l1, t, l2
        """,
        labels=list(MIRRORED_LABELS),
    ))
    return [(r["l1"], r["t"], r["l2"]) for r in rows]


def _mirror_relationship(legacy_session, new_session, sl: str, rt: str, el: str) -> int:
    sk, ek = LABEL_KEY[sl], LABEL_KEY[el]
    rows = list(legacy_session.run(
        f"""
        MATCH (a:`{sl}`)-[r:`{rt}`]->(b:`{el}`)
        RETURN a.`{sk}` AS sk, b.`{ek}` AS ek, properties(r) AS props
        """,
    ))
    payload = [
        {"sk": r["sk"], "ek": r["ek"], "props": r["props"] or {}}
        for r in rows
        if r["sk"] is not None and r["ek"] is not None
    ]
    if not payload:
        return 0
    new_session.run(
        f"""
        UNWIND $rows AS row
        MATCH (a:`{sl}` {{`{sk}`: row.sk}})
        MATCH (b:`{el}` {{`{ek}`: row.ek}})
        MERGE (a)-[r:`{rt}`]->(b)
        SET r += row.props
        """,
        rows=payload,
    )
    return len(payload)


@asset(
    required_resource_keys={"neo4j_legacy", "neo4j_new"},
    description="Mirror the curated graph (Books, Papers, Concepts, Topics, "
                "Researchers, Ideas, Authors + all relationships) from legacy → new DB.",
)
def legacy_graph_mirror(context) -> MaterializeResult:
    legacy = context.resources.neo4j_legacy
    new = context.resources.neo4j_new

    node_counts: dict[str, int] = {}
    rel_counts: dict[str, int] = {}

    legacy_driver = legacy.get_driver()
    new_driver = new.get_driver()
    try:
        with legacy_driver.session(database=legacy.database) as ls, \
             new_driver.session(database=new.database) as ns:
            for label, key in LABEL_KEY.items():
                n = _mirror_nodes(ls, ns, label, key)
                node_counts[label] = n
                context.log.info(f"mirrored {n} {label} nodes")

            patterns = _discover_patterns(ls)
            context.log.info(f"discovered {len(patterns)} relationship patterns to mirror")
            for sl, rt, el in patterns:
                k = f"({sl})-[:{rt}]->({el})"
                rel_counts[k] = _mirror_relationship(ls, ns, sl, rt, el)
                context.log.info(f"mirrored {rel_counts[k]} {k}")
    finally:
        legacy_driver.close()
        new_driver.close()

    return MaterializeResult(
        metadata={
            "nodes_by_label":     MetadataValue.json(node_counts),
            "relationships":      MetadataValue.json(rel_counts),
            "total_nodes":        MetadataValue.int(sum(node_counts.values())),
            "total_relationships": MetadataValue.int(sum(rel_counts.values())),
        },
    )
