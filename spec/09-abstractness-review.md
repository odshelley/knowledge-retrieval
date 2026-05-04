# 09 — Abstractness Review

**Question**: is the current spec ([01](01-schema.md)–[08](08-mvp-plan.md)) abstract enough to apply both to (a) the alethograph research-vault plugin (which already does GraphRAG-of-sorts and needs an upgrade) and (b) other bank knowledge-retrieval ventures such as exploring code bases linked to model docs?

**Short answer**: the *architecture* is reusable; the *current spec text* is 60-70 % core/abstract and 30-40 % bank-quant-specific. With a small refactor — pulling the domain-specific parts behind a "domain plugin" interface — the same machinery serves all three use cases.

This file (1) audits which parts of the spec are core vs domain-specific, (2) defines the domain-plugin contract, and (3) sketches three concrete domain plugins: `quant-wiki`, `research-vault` (alethograph), and `code-explorer`.

## 1. Audit — what's core, what's domain

| Spec file | Core (reusable) | Domain-specific (must vary) |
|---|---|---|
| [01-schema](01-schema.md) — Source nodes, Chunk, Page/Doc | `:Page`, `:Doc`, `:Chunk` patterns; vector + btree indexes; `:VISIBLE_TO` overlay; `:Alias` mechanism | The set of typed entity labels (`:Model`, `:Methodology`, …) and the typed predicate set |
| [02-ingestion](02-ingestion.md) — Pipelines | The pipeline shape (fetch → upsert → chunk → embed → extract → resolve → merge); idempotent MERGE; native-link GC; `content_hash`-based skip | What "native links" are (wiki hyperlinks vs paper bibrefs vs code imports); what counts as "model doc" vs other sources |
| [03-extraction-prompts](03-extraction-prompts.md) — Prompts | The output JSON schema; system-prompt structure; few-shot template; gleaning loop | The label/predicate vocabulary; the few-shot exemplars; canonical-name rules |
| [04-alias-resolution](04-alias-resolution.md) — Resolver | The whole resolution pipeline (exact → alias → vector+token fuzzy → LLM disambig → manual review); `:Alias` data model; audit log + reversibility | Source-trust hierarchy ordering; vector model; token-similarity normaliser specifics |
| [05-query-router](05-query-router.md) — Five patterns | The five patterns themselves (vector / 1-hop / PPR / dual-level / lazy-Leiden); classifier interface; uniform result shape | Classifier prompt examples; which typed predicates the 1-hop expansion considers; preferred starting label set for vector seeds |
| [06-filtering-fallback](06-filtering-fallback.md) — Filter + parametric fallback | The two-stage filter, asymmetric integration, refuse-mode | Wording of the refuse message ("compliance" framing is bank-specific); `tau_G` / `tau_L` defaults |
| [07-updates](07-updates.md) — Incremental updates | All of it — pipeline-shape rather than domain | Decay window (`P30D`, `P90D`) is a policy choice |
| [08-mvp-plan](08-mvp-plan.md) — Build order | Phase shape | Eval set construction details; "quant SME" terminology |

**Verdict**: the architectural backbone (files 02, 04, 05, 06, 07) is already domain-agnostic in *shape*. Files 01, 03, and parts of 05 carry the most domain-specific weight, and that weight is concentrated in two places: **the schema's type system** and **the extraction prompt's vocabulary**.

This is the right factoring. Per [[domain-specific-graphrag]] (Han et al. 2024 survey §2.2): "a unified GraphRAG is nearly impossible" because relations are domain-specific. The trick is not to *eliminate* the domain coupling but to *isolate* it behind a plugin boundary.

## 2. The domain-plugin contract

A domain plugin provides exactly the things that vary, and nothing else. Concretely a `DomainConfig` (as a Python dataclass / TS interface / YAML — pick one) is:

```python
@dataclass
class DomainConfig:
    # — Identity —
    name: str                                    # "quant-wiki", "research-vault", "code-explorer"
    description: str

    # — Schema —
    entity_labels: dict[str, EntityLabelSpec]    # {label_name: spec}
    relation_predicates: dict[str, RelationSpec] # {predicate_name: spec}

    # — Source ingestion —
    sources: list[SourceSpec]                    # how to fetch + parse each source
    native_link_rules: list[NativeLinkRule]      # how to lift source-side links into edges

    # — Extraction —
    extraction_system_prompt: str                # template; references the label / predicate sets
    few_shot_exemplars: list[Exemplar]           # 3–5 in-domain examples
    canonical_name_rules: str                    # plain-English rules embedded in the system prompt

    # — Alias resolution —
    source_trust_order: list[str]                # source names, most-trusted first
    fuzzy_thresholds: FuzzyThresholds            # (vector_auto, vector_ambig, token_min)

    # — Query routing —
    classifier_examples: list[ClassifierExample] # query → class examples per class
    expansion_predicates: list[str]              # which predicates Pattern B expands across
    seed_labels: list[str]                       # which typed labels Pattern D's low-level vector match targets

    # — Filter / fallback —
    refuse_message_template: str
    tau_g: float = 0.45
    tau_l: float = 1.0


@dataclass
class EntityLabelSpec:
    description: str                             # for the extraction prompt
    aliases: list[str]                           # synonymous names for the LLM to recognise
    canonical_attrs: list[str]                   # attribute names beyond name/description (e.g. for Model: type)


@dataclass
class RelationSpec:
    description: str
    src_labels: list[str]                        # allowed source-node labels
    dst_labels: list[str]                        # allowed destination-node labels
    inverse: str | None = None                   # optional inverse relation name
```

**The core implementation accepts a `DomainConfig` and is agnostic to its contents.** That's the test of whether the architecture is abstract enough: can `core` be unit-tested with a mock `DomainConfig` containing only `:Foo` and `:Bar` labels and a single `:RELATES_TO` predicate?

## 3. Repository layout under the refactor

```
knowledge-retrieval/
  core/                          # domain-agnostic implementation
    schema.py                    # applies a DomainConfig → Cypher constraints/indexes
    ingest/
      pipeline.py                # the stage shape from spec/02
      native_links.py            # GC and refresh
      chunker.py                 # generic chunking with config knobs
    extraction/
      prompt_builder.py          # builds system prompt from DomainConfig
      runner.py                  # LLM call + JSON parsing + validation
      gleaning.py                # self-reflection loop
    resolution/
      resolver.py                # the §2 pipeline from spec/04
      review_queue.py
    retrieval/
      router.py                  # query classifier + dispatch
      patterns/
        vector.py                # Pattern A
        one_hop.py               # Pattern B
        ppr.py                   # Pattern C
        dual_level.py            # Pattern D
        lazy_leiden.py           # Pattern E
    filter/
      stage1.py                  # cheap LLM scoring
      stage2.py                  # full LLM scoring
      integrator.py              # asymmetric fallback
    updates/
      handler.py                 # spec/07 logic
      decay.py
    observability/
      logging.py                 # the Query node + audit log
  domains/
    quant_wiki/
      config.py                  # DomainConfig for spec/01–08 as written
      exemplars/                 # the curated few-shots
    research_vault/
      config.py                  # alethograph
      exemplars/
    code_explorer/
      config.py
      exemplars/
  apps/
    quant_wiki_service/          # FastAPI service binding core + quant_wiki domain
    alethograph_plugin/          # the existing plugin, refactored to use core
    code_explorer_service/
```

## 4. Domain plugin sketches

### 4.1 `domains/research_vault/` — alethograph

This is the alethograph plugin's current job. Its existing graph already has these labels (see `~/Projects/academic-research-system/vault/knowledge-retrieval.md`-style topic notes and the Neo4j topic DAG), so the `DomainConfig` is mostly a transcription of what's already implicit:

```python
research_vault_config = DomainConfig(
    name="research-vault",
    description="Academic research papers, concepts, methods, ideas, and topics in an Obsidian vault.",
    entity_labels={
        "Paper":      EntityLabelSpec(description="An academic paper / preprint", aliases=["article","preprint"], canonical_attrs=["year","venue","arxiv_id","doi"]),
        "Concept":    EntityLabelSpec(description="A theoretical idea, mathematical object, or framework", aliases=[]),
        "Method":     EntityLabelSpec(description="An algorithm or implementable technique", aliases=["algorithm","technique"]),
        "Author":     EntityLabelSpec(description="A paper author / researcher", aliases=[]),
        "Topic":      EntityLabelSpec(description="A research domain or subfield", aliases=["domain","field"]),
        "Idea":       EntityLabelSpec(description="A user-authored speculative connection between concepts", aliases=[]),
        "Book":       EntityLabelSpec(description="An academic textbook", aliases=["textbook","monograph"]),
        "Venue":      EntityLabelSpec(description="A publication venue (journal, conference)", aliases=[]),
    },
    relation_predicates={
        "AUTHORED":     RelationSpec("Author wrote Paper",                 src_labels=["Author"],  dst_labels=["Paper","Book"]),
        "HAS_TOPIC":    RelationSpec("Paper/concept belongs to Topic",     src_labels=["Paper","Concept","Method","Idea","Book"], dst_labels=["Topic"]),
        "DERIVED_FROM": RelationSpec("Concept/method extracted from Paper",src_labels=["Concept","Method"], dst_labels=["Paper","Book"]),
        "ISA":          RelationSpec("Concept/method is-a-kind-of",        src_labels=["Concept","Method"], dst_labels=["Concept","Method"]),
        "CITES":        RelationSpec("Paper cites Paper",                  src_labels=["Paper"], dst_labels=["Paper","Book"]),
        "PUBLISHED_IN": RelationSpec("Paper published in Venue",           src_labels=["Paper"], dst_labels=["Venue"]),
        "BROADER_THAN": RelationSpec("Topic is broader than Topic",        src_labels=["Topic"], dst_labels=["Topic"]),
        "RELATED_TO":   RelationSpec("Concept/idea related to other",      src_labels=["Concept","Idea"], dst_labels=["Concept","Idea","Method"]),
    },
    sources=[
        SourceSpec(name="vault", kind="local-markdown", path="$ALETHOGRAPH_VAULT", parser="obsidian-md"),
    ],
    native_link_rules=[
        # Every [[wikilink]] in vault markdown is a native link
        NativeLinkRule(pattern=r"\[\[([^\]]+)\]\]", target_resolver="vault-filename-to-page"),
    ],
    expansion_predicates=["AUTHORED","HAS_TOPIC","DERIVED_FROM","ISA","CITES","BROADER_THAN","RELATED_TO"],
    seed_labels=["Paper","Concept","Method","Topic"],
    source_trust_order=["vault"],
    classifier_examples=[
        ClassifierExample(query="What is the Doob h-transform?",                                     cls="ENTITY_LOOKUP"),
        ClassifierExample(query="Which papers under stochastic-analysis cite the Cont-Tankov book?", cls="MULTI_HOP"),
        ClassifierExample(query="How do diffusion bridges relate to rectified flows?",               cls="COMPARATIVE"),
        ClassifierExample(query="Summarise the segmentation-unlearning subfield",                    cls="GLOBAL_SUMMARY"),
    ],
    refuse_message_template=(
        "I don't have enough confidence to answer from the vault. "
        "Closest notes: {closest_notes}. Run `/research <topic>` to ingest more papers."
    ),
)
```

Two observations on what alethograph currently does and what changes:

- **Already has**: `:Paper`, `:Concept`, `:Author`, `:Topic`, `:Idea`, `:Book`, `:Venue` nodes in Neo4j with `HAS_TOPIC`, `DERIVED_FROM`, `BROADER_THAN`, `CITES` edges. Its retrieval is currently dump-all-notes-for-topic-then-LLM (see the `db-get-researcher-notes` query in this skill's SKILL.md). That's effectively Pattern A scoped by topic — equivalent to the simplest case.
- **Upgrade buys**: adding the query router brings four new retrieval patterns, especially Pattern C (Personalized PageRank for "what concepts/ideas are connected to this paper through 2 hops?") and Pattern E (lazy Leiden global summaries for "what's the state of the field on X?"). The alias resolver is also valuable because alethograph has the same name-fragmentation issue (e.g., the "Doob h-transform" appears with three slightly different spellings in current ingests).

### 4.2 `domains/code_explorer/` — bank code bases linked to model docs

The use case: a quant has a model doc and asks "show me the implementation"; or has a function and asks "what models use this calibrator?". Cross-source linking from `:ModelDoc` nodes (already in the quant-wiki domain) to `:Function` / `:Class` nodes here is the key value-add.

```python
code_explorer_config = DomainConfig(
    name="code-explorer",
    description="Bank quant-libs code: repos, modules, classes, functions, tests, dependencies, API endpoints.",
    entity_labels={
        "Repository":  EntityLabelSpec(description="A git repo", canonical_attrs=["url","default_branch"]),
        "Module":      EntityLabelSpec(description="A module / package / namespace"),
        "Class":       EntityLabelSpec(description="A class / struct / record"),
        "Function":    EntityLabelSpec(description="A free function or method"),
        "Test":        EntityLabelSpec(description="A test case or test function"),
        "Dependency":  EntityLabelSpec(description="An external package the repo depends on"),
        "ApiEndpoint": EntityLabelSpec(description="An RPC / HTTP endpoint"),
        "ConfigKey":   EntityLabelSpec(description="A configuration key / env var read at runtime"),
    },
    relation_predicates={
        "IMPORTS":     RelationSpec("Module/file imports another", src_labels=["Module","Class","Function"], dst_labels=["Module","Class","Function","Dependency"]),
        "CALLS":       RelationSpec("Function/method calls another", src_labels=["Function"], dst_labels=["Function"]),
        "DEFINED_IN":  RelationSpec("Class/function defined in module", src_labels=["Class","Function"], dst_labels=["Module"]),
        "TESTS":       RelationSpec("Test exercises function/class", src_labels=["Test"], dst_labels=["Class","Function"]),
        "IMPLEMENTS":  RelationSpec("Class implements interface", src_labels=["Class"], dst_labels=["Class"]),
        "EXTENDS":     RelationSpec("Class extends another", src_labels=["Class"], dst_labels=["Class"]),
        "EXPOSES":     RelationSpec("Function exposes API endpoint", src_labels=["Function"], dst_labels=["ApiEndpoint"]),
        "READS_CONFIG":RelationSpec("Function reads config key", src_labels=["Function"], dst_labels=["ConfigKey"]),
        # — Cross-domain bridge —
        "IMPLEMENTS_MODEL": RelationSpec("Code implements a Model entity", src_labels=["Class","Function","Module"], dst_labels=["Model"]),
    },
    sources=[
        SourceSpec(name="git-quant-libs", kind="git", url="...", parser="tree-sitter-multilang"),
    ],
    native_link_rules=[
        # Imports parsed by tree-sitter become :IMPORTS edges directly
        NativeLinkRule(parser_emits="import-statement", predicate="IMPORTS"),
        NativeLinkRule(parser_emits="call-site",        predicate="CALLS"),
        NativeLinkRule(parser_emits="class-inheritance",predicate="EXTENDS"),
    ],
    expansion_predicates=["CALLS","IMPORTS","DEFINED_IN","TESTS","IMPLEMENTS","EXTENDS","IMPLEMENTS_MODEL"],
    seed_labels=["Function","Class","Module","Repository"],
    source_trust_order=["git-quant-libs"],
    classifier_examples=[
        ClassifierExample(query="Where is the SABR calibrator implemented?",                  cls="ENTITY_LOOKUP"),
        ClassifierExample(query="What functions does sabrCalibrator call transitively?",       cls="MULTI_HOP"),
        ClassifierExample(query="Which tests cover the Bermudan swaption pricer?",             cls="MULTI_HOP"),
        ClassifierExample(query="Summarise the IR exotics codebase architecture",              cls="GLOBAL_SUMMARY"),
    ],
)
```

Two notes on this domain:

- **Native links are even more powerful here than in the wiki**: a tree-sitter / language-server parse extracts `:IMPORTS`, `:CALLS`, `:EXTENDS`, `:DEFINED_IN` deterministically from the AST. Most of the graph is built without any LLM extraction at all. LLM extraction is reserved for `:IMPLEMENTS_MODEL` (because deciding "this `sabrCalibrator` class implements the SABR-LMM model" requires reading docstrings + comments — pure structural code analysis can't do it).
- **Cross-domain bridge**: `:IMPLEMENTS_MODEL` relations connect into the `:Model` nodes managed by the `quant-wiki` domain plugin. This is the "code linked to model docs" use case directly. The alias resolver handles the fragmentation between code-side names (`SABRLiborMarketModel` class) and wiki-side canonical (`SABR-LMM` model).

### 4.3 `domains/quant_wiki/` — what spec/01–08 currently is

Just the existing files moved into `domains/quant_wiki/config.py` plus `exemplars/`, with the few-shots from [03-extraction-prompts.md](03-extraction-prompts.md) §4 lifted out as data.

## 5. Federated retrieval across domains

The interesting case: a quant asks "show me the implementation and documentation for the SABR-LMM model". This crosses the `quant-wiki` and `code-explorer` domains.

The clean way: each domain's `:Model` node is ultimately the *same* canonical, because the alias resolver sees `IMPLEMENTS_MODEL` extractions from the code domain and binds them to the wiki domain's existing `:Model {name: "SABR-LMM"}` node. **One graph, multiple domain plugins contributing nodes and edges into a shared label space.**

This requires:
- Shared canonical entity labels (`:Model`, `:Methodology`, …) live in a **shared root-domain config** that both `quant-wiki` and `code-explorer` import. They each only *contribute* (entities + edges) to the shared labels; they don't redefine them.
- Domain-private labels (`:Function`, `:Class`, `:Page`) stay in their own domain.
- The query router can be configured with a *list* of domains; classifier examples are unioned; seed labels are unioned.

The alethograph use case doesn't need federation (its graph is fully self-contained). But the bank's wiki + code-explorer scenario depends on it.

## 6. What this refactor *doesn't* fix

- **Domain expertise still needed** for few-shot exemplar curation. The prompt template is reusable; the exemplars are not. Each new domain takes ~1 SME-week to seed quality exemplars.
- **Schema design judgement** — choosing the right entity labels and predicate set for a new domain is a design exercise, not something the framework can automate. The plugin contract makes the work easier, not zero.
- **Evaluation effort scales with domain count**. Each domain needs its own labelled eval set (50–500 queries) to track quality.

## 7. Verdict

The current spec is **structurally abstract** — files 02, 04, 05, 06, 07 read essentially as core / domain-agnostic. Files 01 and 03 carry the domain coupling, but the coupling is concentrated in two places (label set, prompt vocabulary) which is exactly where a `DomainConfig` plugin should sit.

The refactor needed before reuse is small: lift labels and predicates from [01-schema.md](01-schema.md) into a `DomainConfig`, lift exemplars from [03-extraction-prompts.md](03-extraction-prompts.md) into a `domains/<name>/exemplars/` directory, parameterise the prompt builder. **The architecture is reusable as-is.**

Recommended next steps (in priority order):

1. Define the `DomainConfig` dataclass (or pydantic model) and the core/domain split — Phase 0 of any actual implementation.
2. Write the `quant-wiki` config first (lifts directly from spec 01–08) and use it as the reference implementation.
3. Port alethograph onto core: read its existing Neo4j schema, write the `research-vault` config, swap its retrieval layer for the new query router. Should be a 1-2 week refactor and gives the alethograph plugin all five retrieval patterns.
4. Build `code-explorer` for the cross-domain bridge case — the highest-leverage bank-internal extension after the wiki itself works.
