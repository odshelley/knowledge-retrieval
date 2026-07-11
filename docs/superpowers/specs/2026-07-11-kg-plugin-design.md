# kg: shared researcher plugin + hosted MCP query server — design

**Date:** 2026-07-11
**Status:** approved (brainstorming session with Osian)
**Working name:** `kg` — planned rename to `alethograph` once it replaces the existing alethograph plugin (see Roadmap).

## Purpose

A shareable Claude Code plugin that lets Osian and fellow researchers use the knowledge-retrieval graph (the from-scratch builder graph in the alethograph Aura DB) as a grounded research assistant. Target capabilities, in priority order:

1. **v1 — Grounded Q&A**: answer questions about topics covered by the corpus, with verbatim quotes and precise citations.
2. **v2 — Referee support**: check a submitted paper against existing knowledge (prior art, novelty) via the CITES graph and concept overlap.
3. **v2/v3 — Paper-writing aid (high priority)**: strengthen arguments with citations and context, help prove statements via `Result DEPENDS_ON` chains, verify citation correctness. Adapts existing skills (citation-audit, proof-checker, latex) into self-contained form.
4. **v3 — Link discovery**: find connections between papers.

This design covers v1 in full; later versions reuse v1's retrieval protocol and server tools.

## Decisions made (with rationale)

| Decision | Choice | Why |
|---|---|---|
| Graph access for colleagues | Hosted query server (not shared DB creds, not per-user graphs) | Keys stay server-side; controlled query surface; one shared corpus |
| Protocol | Remote MCP server, streamable HTTP | Native Claude Code integration; cross-client standard; auth/distribution solved |
| Query surface | Typed tools only — no raw Cypher, no write tools | Predictable, safe, cacheable; schema knowledge stays server-side |
| Hosting | PaaS container (Fly.io or Railway) | Stateless service; always-on without Osian's Mac; trivially scalable; portable Docker image |
| v1 scope | Server + one skill (`ask`) | Q&A exercises every tool; other workflows are increments on a proven base |
| Repo layout | Server inside knowledge-retrieval (`server/` package); plugin in a new public repo | Server reuses `pipeline/graph/schema.py` and embedding code with zero drift; pipeline repo stays private; plugin repo ships nothing but skills + config |
| Server stack | Python + official MCP SDK (FastMCP), uv-managed | Same stack as pipeline; direct code reuse |
| Auth | Static per-researcher bearer tokens, salted hashes server-side | A handful of trusted colleagues; OAuth is an upgrade path, not a v1 need |

## Architecture

```
Colleague's machine                      PaaS (Fly.io/Railway)              State
┌──────────────────────────┐            ┌──────────────────────────┐
│ Claude Code              │  MCP over  │ kg MCP server            │  bolt+s   ┌────────────┐
│  └ kg plugin             │  HTTPS +   │  server/ package in      │──────────▶│ Neo4j Aura │
│     ├ skills/ask         │  bearer    │  knowledge-retrieval     │ read-only │ (graph)    │
│     ├ .mcp.json ─────────┼───────────▶│  8 typed read-only tools │   user    └────────────┘
│     └ bin/kg-setup       │   token    │  embeds queries with     │  HTTPS    ┌────────────┐
│       (stores KG_TOKEN)  │            │  server-side OpenAI key  │──────────▶│ OpenAI     │
└──────────────────────────┘            └──────────────────────────┘           │ embeddings │
                                                                                └────────────┘
```

Properties:

- **Stateless, read-only server.** Connects to Aura with a dedicated read-only Neo4j user, never the admin credentials. No Postgres or MinIO access; those remain pipeline-internal.
- **All secrets server-side** as PaaS secrets: `NEO4J_RO_URI/USER/PASSWORD`, `OPENAI_API_KEY`, `KG_TOKENS` (token-name → salted hash). Researchers hold only a personal bearer token.
- **Thin plugin**: skills + `.mcp.json`. No Python dependencies on the researcher's machine (an improvement over alethograph's httpx/neo4j requirement).
- **Versioned contract**: endpoint is `/v1/mcp`; breaking tool changes go to `/v2` so deployed plugins never silently break. `/healthz` reports server-up and graph-reachable separately.

## Component 1: MCP server (`server/` in knowledge-retrieval)

Python + FastMCP over streamable HTTP, one Docker image (`Dockerfile.server` + `fly.toml` in `docker/`, alongside the existing Dagster Dockerfile). Imports `pipeline/graph/schema.py`, Cypher helpers, and the embedding helper directly.

### Tool set (v1 — 8 read-only tools)

All tools return compact JSON with stable ids so results compose across calls.

| Tool | Essential parameters | Purpose |
|---|---|---|
| `search_chunks` | `query, top_k=8, expand="local", paper_id?` | Embed query server-side; vector search the chunk index; graph-expand per `expand` (below). The workhorse. |
| `get_paper` | `id \| doi \| arxiv \| title` | Paper metadata, authors, abstract/TLDR, Summary-node analysis if present. |
| `search_papers` | `query, top_k` | Papers by title/abstract relevance. |
| `get_concept` | `name_or_id` | Concept + its Definitions (with source paper/section) + related concepts. |
| `get_results` | `concept_id \| paper_id, kind?` | Results (theorem/lemma/proposition/corollary) that USE a concept or are STATED by a paper, with statement text. |
| `get_dependency_chain` | `result_id, depth=3` | Walk `Result DEPENDS_ON Result` with DEFINES context. Proof-scaffolding primitive. |
| `get_citations` | `paper_id, direction=in\|out, depth=1` | CITES neighbourhood; prior-art and influence queries. |
| `get_corpus_overview` | — | Counts, concept coverage, recent papers. Lets skills state what the corpus does not cover. |

### GraphRAG: the `expand` parameter of `search_chunks`

- `expand="none"` — plain vector similarity.
- `expand="local"` (default) — LightRAG/HippoRAG-style local expansion in one server-side Cypher pass: each hit returns with section + paper, the Concepts its paper DISCUSSES, Definitions/Results stated in the same section, and one hop of CITES neighbours.
- `expand="concepts"` — dual-level variant: vector hits identify relevant Concepts, then retrieval pivots to everything attached to those concepts (definitions, results, discussing papers). Catches same-idea-different-words matches that pure similarity misses.

Hybrid Cypher is lifted from the proven patterns in `notebooks/smoke_test.ipynb`, adapted to the current schema.

Deliberately absent from the tool set: write tools, raw Cypher, and any "answer the question" tool — synthesis is the skill's job; the server only retrieves.

### Auth, limits, observability

- `Authorization: Bearer <token>`; tokens issued by `scripts/issue_token.py`, verified against salted hashes in `KG_TOKENS`.
- Per-token rate limit; 429 with retry-after. Per-query timeout.
- Every call logged with token name (usage visibility per colleague).

## Component 2: plugin (new public repo, working name `kg`)

```
kg/
├── .claude-plugin/
│   ├── plugin.json          # name "kg", version, mcpServers ref
│   └── marketplace.json     # /plugin marketplace add odshelley/kg
├── .mcp.json                # → https://<host>/v1/mcp, Authorization: Bearer ${KG_TOKEN}
├── skills/
│   └── ask/SKILL.md
├── bin/kg-setup             # prompt for token → persist KG_TOKEN
└── README.md                # install steps + "ask Osian for a token"
```

Colleague setup: add marketplace → install plugin → run `kg-setup` with an issued token. The `.mcp.json` references `${KG_TOKEN}`; no secret ever lives in the repo.

### The `ask` skill (`/kg:ask`)

Triggers: "ask the graph", "what does the literature say about…", `/kg:ask`. Workflow:

1. **Scope check first.** Call `get_corpus_overview` (cached per session); if the corpus cannot support the question, say so up front. Anti-hallucination gate.
2. **Retrieve hybrid.** `search_chunks(query, expand="local")`; reformulate and re-search on weak hits; `expand="concepts"` for idea-level questions.
3. **Deepen deliberately.** `get_dependency_chain` for theorem lineage, `get_citations` for provenance, `get_concept` for exact definitions, as warranted.
4. **Synthesize with hard citation discipline.** Every substantive claim carries `[Author Year, §section]` from a retrieved chunk; verbatim quotes for definitions and theorem statements; Sources list (title, authors, year, DOI/arXiv) at the end. **Rule: a claim without a retrieved chunk behind it must be labelled as the model's own interpretation.**
5. **Answer shape scaled to the ask** — prose with inline citations for direct questions; structured note for briefings. LaTeX preserved from chunks.

The skill contains **no Cypher and no schema knowledge** — only the eight tool contracts. Steps 1–3 form the shared "retrieval protocol" that the referee/write/links skills will reuse verbatim.

## Error handling

| Failure | Behaviour |
|---|---|
| Server unreachable / 401 | Skill reports which it is ("server down" vs "token invalid — rerun `kg-setup` or ask Osian"); never silently answers from memory instead. |
| Aura down / query timeout | Structured tool error, never a partial result dressed as complete; `/healthz` separates server-up from graph-reachable. |
| Weak retrieval | Similarity floor + corpus-overview check → skill says the corpus is thin there rather than synthesizing from noise. |
| Rate limited | 429 + retry-after; skill waits or informs the user. |

## Testing

- **Server unit tests** (pytest, mocked driver): auth, tool input validation, Cypher builders.
- **Integration suite** gated behind `--run-integration` against real Aura read-only (same convention as the pipeline).
- **`scripts/smoke_server.py`**: calls every tool against a deployed server; the post-deploy check.
- **Skill eval checklist** in the plugin repo: 5–10 questions with known-good sources, including one deliberately uncovered topic to verify the "corpus is thin" behaviour. Manual for v1.

## Roadmap

- **v2 — `referee` skill**: novelty/prior-art checking via CITES + concept overlap; reuses the retrieval protocol unchanged.
- **v2/v3 — `write` skill family (HIGH PRIORITY per Osian)**: argument strengthening, proof help via `get_dependency_chain`, citation correctness. Adapt citation-audit/proof-checker/latex into self-contained form (no ARIS_REPO or external-reviewer dependencies) so colleagues can run them.
- **v3 — `links` skill**: cross-paper connections via shared Concepts, dependency chains, citation structure, embedding proximity.
- **Pipeline-side enablers**: topic inference (unlocks topic-subtree queries à la alethograph's topic-expert — the builder currently creates no Topic nodes, so this is a data dependency, not a server change); book ingestion landing (Book-backed chunks then appear in retrieval automatically).
- **Rename `kg` → `alethograph`** when it replaces the old plugin: marketplace entry, plugin name, server name in `.mcp.json`; colleagues reinstall once.

## Known gaps vs the current alethograph plugin

- No topic-DAG walking until topic inference lands (above).
- No Researcher/Idea nodes in the builder graph; alethograph's idea-seeding and researcher-linking workflows are out of scope here — this plugin is the consumption layer, alethograph's curation side is not being replaced by v1.

## Out of scope (v1)

Write access of any kind; raw Cypher; OAuth; a web UI; per-user corpora; automated paper writing (the write family assists, it does not draft unattended).
