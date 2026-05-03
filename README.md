# Quant Wiki GraphRAG — Architecture Spec

A hybrid Neo4j-backed GraphRAG system for natural-language search over an internal quant wiki and externally-stored model documentation.

**Grounding**: this spec is informed by six analyzed papers — Edge et al. 2024 (MS GraphRAG), Han et al. 2024 (GraphRAG survey), Guo et al. 2024 (LightRAG), Gutiérrez et al. 2024 (HippoRAG), Xiang et al. 2025 (GraphRAG-Bench), Zeng et al. 2025 (GraphRAG-FI). Concept references in this spec use Obsidian-style wikilinks pointing into `~/Projects/academic-research-system/vault/`.

## Design summary

- **One graph store** (Neo4j) for vector indexes, k-hop traversal, GDS algorithms, and idempotent updates — no separate vector DB.
- **Native wiki hyperlinks become graph edges directly** (high-precision, free); LLM extraction is reserved for typed entities and relations *inside* page content.
- **Domain-typed schema** (`:Model`, `:Methodology`, `:Product`, `:RiskFactor`, …) — never flatten to generic `:Entity`.
- **Cross-source alias resolution** is the load-bearing engineering problem: the same model has different names in the wiki, model docs, and code repos.
- **Lazy Leiden, not eager**: skip MS GraphRAG's pre-built community summaries. Wiki edits constantly; community summaries don't update incrementally. Run Leiden on demand for global queries, cache.
- **Query router** dispatches by query type to one of five retrieval patterns (vector, 1-hop, PPR, dual-level, lazy-Leiden map-reduce).
- **Filter + refuse fallback are mandatory**, not optional. A bank will not tolerate the ~17% broken-call rate of unfiltered GraphRAG.

## Spec contents

| File | Topic |
|---|---|
| [spec/01-schema.md](spec/01-schema.md) | Neo4j node/edge schema, indexes, constraints |
| [spec/02-ingestion.md](spec/02-ingestion.md) | Wiki + model-doc ingestion pipelines |
| [spec/03-extraction-prompts.md](spec/03-extraction-prompts.md) | Concrete LLM prompts for entity/relation extraction |
| [spec/04-alias-resolution.md](spec/04-alias-resolution.md) | Cross-source entity canonicalisation |
| [spec/05-query-router.md](spec/05-query-router.md) | Five retrieval patterns + dispatcher logic |
| [spec/06-filtering-fallback.md](spec/06-filtering-fallback.md) | Two-stage filter + asymmetric parametric-memory fallback |
| [spec/07-updates.md](spec/07-updates.md) | Incremental update semantics |
| [spec/08-mvp-plan.md](spec/08-mvp-plan.md) | Phased build order |
| [spec/09-abstractness-review.md](spec/09-abstractness-review.md) | Critique + refactor toward a domain-pluggable framework (alethograph + code-explorer) |

## Out of scope (engineering, not research)

- **Access control / entitlements** — see §2 of [spec/01-schema.md](spec/01-schema.md) for the `:VISIBLE_TO` overlay; full RBAC integration is a separate workstream.
- **Model-doc versioning** — sketched in [spec/01-schema.md](spec/01-schema.md), but quant-business decisions on versioning policy are out of scope.
- **Federated source ingest** beyond wiki + one model-doc store — extension pattern only, not specified.
- **LLM hosting / private inference** — assumed available; prompt cost and latency are noted but not optimised for.

---

## Knowledge pipeline substrate (2026-05-03)

The current iteration of this repo is the substrate described in:

- Spec: [docs/specs/2026-05-03-knowledge-pipeline-design.md](docs/specs/2026-05-03-knowledge-pipeline-design.md)
- Plan: [docs/superpowers/plans/2026-05-03-knowledge-pipeline-substrate.md](docs/superpowers/plans/2026-05-03-knowledge-pipeline-substrate.md)
- Operations: [docs/operations.md](docs/operations.md)

The legacy `spec/` directory is from an earlier (abandoned) design and is not part of the current build.
