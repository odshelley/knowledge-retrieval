# 04 — Cross-Source Alias Resolution

The single hardest correctness problem. The same model is "SABR-LMM" in the wiki, "SABR–LMM v3.2" in a model doc, "SABRLiborMarketModel" in a code repo, and "the LMM-style SABR" in a meeting note. Without solving this, the graph fragments and traversal queries miss most of the answer.

## 1. The data model

```cypher
(:Alias {
  surface: "SABRLiborMarketModel",          // the raw form found in some source
  source: "code",                            // {wiki, sharepoint, code, ...}
  first_seen_ts: datetime(),
  last_seen_ts: datetime(),
  needs_review: false                        // see §4
})

(:Alias)-[:ALIAS_OF {confidence}]->(:Model {canonical:true})
```

Canonical entity nodes are unique by `name` (constraint in [01-schema.md](01-schema.md)). Aliases hang off them. Every chunk's `:MENTIONED_IN` edge targets the **canonical** node, never an alias — aliases are bookkeeping, not traversal targets.

## 2. Resolution pipeline

For each surface form `s` with proposed canonical `c` and label `L` emitted by extraction:

```
1. exact_match  := MATCH (n:L {name: c}) RETURN n
   if hit:     bind to n
2. alias_match  := MATCH (a:Alias {surface: s, source: src})-[:ALIAS_OF]->(n:L) RETURN n
   if hit:     bind to n; bump alias.last_seen_ts
3. fuzzy_match  := top-3 cosine NN over (:L {canonical:true}).embedding from embed(c||" "||description)
                  AND token-set similarity > 0.75 (ratio of common normalised tokens)
   if best score > 0.92 AND label matches:
     create alias edge (a:Alias)-[:ALIAS_OF {confidence: score}]->(best); bind
   if 0.85 < best score <= 0.92:
     enqueue for LLM disambiguation (§3)
   else:
     create new canonical (§5)
4. (LLM disambig) — adjudicate
5. (new canonical) — only if all above fail
```

### 2.1 Token-set similarity helper

To avoid pure-vector false positives ("SABR" matching "SABRE" because of close embeddings):

```python
def token_sim(a: str, b: str) -> float:
    ta = set(_normalise(a).split())
    tb = set(_normalise(b).split())
    if not ta or not tb: return 0.0
    return len(ta & tb) / len(ta | tb)

def _normalise(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)        # strip punctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s
```

Both vector AND token similarity must agree before auto-merging. This is the lesson from production entity-resolution systems: vectors alone overmerge, token alone undermerge.

## 3. LLM disambiguation (§2 step 4)

Triggered when the top-1 match is in the ambiguous band [0.85, 0.92] OR when the second-best is within 0.05 of the top.

```
You are reconciling a candidate canonical name against existing entities in a
quant-finance knowledge graph.

CANDIDATE:
  label: {label}
  surface: {surface}
  proposed_canonical: {proposed}
  description: {description}
  source_context: {one_paragraph_around_the_mention}

EXISTING TOP-3 MATCHES:
{for n in top3}
  {idx}. canonical: {n.name}
     description: {n.description}
     used_in: {n.mentioned_count} chunks across {n.source_count} sources
     vector_score: {n.score}
{end}

Are any of the existing matches THE SAME entity as the candidate? Reply with JSON:
{"decision": "match" | "new", "match_index": <1|2|3 or null>, "reasoning": "<one sentence>"}

Be conservative. Prefer "new" when in doubt — wrongly merging two distinct models
is worse than fragmentation, because the fragmentation can be cleaned up later
but the merge silently corrupts traversal results.
```

If `decision = match` → bind to that node, create alias edge.
If `decision = new` → create new canonical (§5).
Either way, **also write the LLM's reasoning to a `:ResolutionLog` node** for audit:

```cypher
CREATE (:ResolutionLog {
  surface: $surface,
  proposed: $proposed,
  decision: $decision,
  match_canonical: $match_canonical,
  reasoning: $reasoning,
  ts: datetime()
});
```

## 4. Manual-review queue

When LLM disambiguation itself returns low confidence (e.g. it cites contradictory evidence), flag for human review:

```cypher
MERGE (a:Alias {surface: $surface, source: $source})
SET a.needs_review = true,
    a.review_note = $reasoning,
    a.last_seen_ts = datetime();
```

Operators query `MATCH (a:Alias {needs_review:true}) RETURN a` to triage. Resolution UI lets a human pick an existing canonical or create a new one; in either case the `:ALIAS_OF` edge is written and `needs_review` is cleared.

## 5. Creating a new canonical

Only after all match attempts have failed:

```cypher
CREATE (n:{label} {
  name: $proposed_canonical,
  full_name: $description_first_clause,
  description: $description,
  embedding: $embedding,
  canonical: true,
  created_ts: datetime(),
  created_from: $source_chunk_id
})
WITH n
MERGE (a:Alias {surface: $surface, source: $source})
ON CREATE SET a.first_seen_ts = datetime()
SET a.last_seen_ts = datetime()
MERGE (a)-[:ALIAS_OF {confidence: 1.0}]->(n);
```

## 6. Across-source preference rules

For finance domain content, prefer canonical naming from sources in this order (most-trusted first), so when two sources disagree on the canonical for the same entity, the higher-trust source wins:

1. **Model docs** approved by Model Risk (these are the audited source of truth for model names).
2. **Wiki pages** in the canonical-models space.
3. Other wiki pages.
4. Code repos.
5. Meeting notes / random PDFs.

Implementation: when promoting an alias to canonical, the alias from the highest-trust source **renames the canonical** (atomic `SET n.name = ...` plus `:WAS_NAMED` history edge). Lower-trust source surface forms remain as aliases.

## 7. Audit and reversibility

All merges are auditable via `:ResolutionLog` and reversible:

```cypher
// "Show me everything that was merged into SABR-LMM in the last week"
MATCH (a:Alias)-[:ALIAS_OF]->(:Model {name: "SABR-LMM"})
MATCH (log:ResolutionLog {match_canonical: "SABR-LMM"})
WHERE log.ts > datetime() - duration('P7D')
RETURN a.surface, log.reasoning, log.ts;

// "Unmerge: split this alias back into a separate canonical"
MATCH (a:Alias {surface: $surface})-[r:ALIAS_OF]->(old:Model {name: $old})
DELETE r
CREATE (n:Model {name: $new_canonical, ..., canonical: true})
CREATE (a)-[:ALIAS_OF {confidence: 1.0}]->(n);
// Then: re-attach :MENTIONED_IN edges manually based on review.
```

Reversibility is the reason the LLM disambiguation prompt is biased toward "new": cleaning up over-fragmentation is mechanical; cleaning up over-merging requires reading every chunk that mentioned the merged entity.
