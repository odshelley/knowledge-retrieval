# 03 — LLM Extraction Prompts

Per Edge et al. 2024 §6.1, **domain-tailored few-shot exemplars** are the single most important quality lever. Generic GPT-4o extraction will mash quant terminology (e.g. confuse "Bachelier" the model with "Bachelier" the volatility convention).

## 1. Output format

JSON, single response. No prose, no markdown wrappers. Validate with a JSON schema before merging.

```json
{
  "entities": [
    {
      "label": "Model" | "Methodology" | "Product" | "Instrument" | "RiskFactor" | "Calibration" | "Regulation" | "Desk" | "Concept",
      "surface": "SABR-LMM",                 // exact span from text
      "canonical": "SABR-LMM",                // proposed canonical (may differ from surface)
      "description": "Stochastic alpha-beta-rho LIBOR market model variant used for IR vol products.",
      "span_start": 421, "span_end": 429,
      "confidence": 0.95
    }
  ],
  "relations": [
    {
      "predicate": "USES" | "DEPENDS_ON" | "CALIBRATES_WITH" | "APPLIES_TO" | "HEDGES_WITH" | "HAS_RISK_FACTOR" | "SUPERSEDES" | "APPROVED_BY" | "OWNED_BY" | "SUBJECT_TO" | "DOCUMENTS",
      "src_canonical": "SABR-LMM",
      "src_label": "Model",
      "dst_canonical": "SVI calibration",
      "dst_label": "Calibration",
      "description": "SABR-LMM is calibrated against the SVI volatility surface for swaptions.",
      "confidence": 0.85
    }
  ]
}
```

## 2. System prompt (extraction)

```
You are a specialist quantitative-finance information-extraction assistant for an
internal bank knowledge graph. Extract typed entities and typed relations from the
text. Use ONLY the labels and predicates listed in the schema. Be conservative:
emit a relation only when it is asserted by the text, not when you can guess it
from background knowledge.

LABELS:
- Model: pricing or risk model (e.g. Black-Scholes, SABR, Hull-White, SABR-LMM, LSV)
- Methodology: a calculation approach or technique (e.g. SVI parameterisation,
  finite-difference PDE solver, Monte Carlo with quasi-random sequences)
- Product: a tradable financial product (e.g. variance swap, autocallable, CVA hedge)
- Instrument: a vanilla market instrument used for pricing/calibration (e.g. swaption,
  caplet, EUR/USD spot)
- RiskFactor: a market or credit risk driver (e.g. EUR rates curve, equity vol surface,
  CDS spread)
- Calibration: a named calibration procedure or target (e.g. SVI calibration,
  Heston calibration, vol surface arbitrage check)
- Regulation: a regulatory framework or rule (e.g. FRTB, SR 11-7, IFRS 13)
- Desk: a trading or quant desk (e.g. Equity Derivatives, IR Exotics, XVA)
- Author: a person responsible for or approving a model/methodology (e.g. lead quant,
  model owner, regulator-named approver)
- Concept: any quant concept that does not fit the above (use sparingly)

PREDICATES (allowed types are noted as src->dst):
- USES (Model->Methodology, Methodology->Methodology)
- DEPENDS_ON (Model->Model, Methodology->Methodology)
- CALIBRATES_WITH (Model->Calibration, Methodology->Calibration)
- APPLIES_TO (Methodology->Product, Methodology->Instrument, Model->Product)
- HEDGES_WITH (Product->Instrument)
- HAS_RISK_FACTOR (Product->RiskFactor, Model->RiskFactor)
- SUPERSEDES (Model->Model, Methodology->Methodology)
- APPROVED_BY (Model->Author, Methodology->Author)
- OWNED_BY (Model->Desk, Methodology->Desk)
- SUBJECT_TO (Model->Regulation, Methodology->Regulation)

CANONICAL NAME RULES:
- For Models, prefer the most-specific established name including any variant suffix
  (e.g. "SABR-LMM" not "SABR" if the text discusses the LIBOR-market-model variant).
- Strip version numbers from canonical names ("SABR-LMM" not "SABR-LMM v3.2");
  versions live on the document, not the model.
- Use title case for multi-word names; preserve standard acronyms in upper case
  (e.g. "Hull-White" not "hull-white"; "FRTB" not "Frtb").

CONFIDENCE:
- 0.9-1.0: explicitly stated and unambiguous
- 0.7-0.9: clearly implied by the text
- 0.5-0.7: probable but the text is partial or wording is ambiguous
- <0.5: do not emit
```

## 3. User prompt (per chunk)

```
Source: {page.source} — {page.title}
Page URL: {page.url}
Chunk position: {chunk.position} of {chunk.total}

--- TEXT START ---
{chunk.text}
--- TEXT END ---

PREVIOUSLY EXTRACTED IN THIS PAGE (do not re-emit identical entities):
{json.dumps(prev_entities)}

Return JSON matching the schema. No prose.
```

## 4. Few-shot exemplars

Curate 3-5 in-domain exemplars from real wiki pages once the system is being seeded. Until then, here are seed exemplars covering the harder cases:

### Exemplar A — model dependency

```text
Input:
"The SABR-LMM model is used by IR Exotics for Bermudan swaption pricing. It depends
on the bootstrapped EUR swap curve and is calibrated daily against the SVI swaption
vol cube. It supersedes the legacy Hull-White 2-factor model retired in 2024."

Output:
{
  "entities": [
    {"label":"Model","surface":"SABR-LMM","canonical":"SABR-LMM","description":"...","span_start":4,"span_end":12,"confidence":0.99},
    {"label":"Desk","surface":"IR Exotics","canonical":"IR Exotics","description":"Interest-rate exotics trading desk.","span_start":29,"span_end":39,"confidence":0.95},
    {"label":"Product","surface":"Bermudan swaption","canonical":"Bermudan Swaption","description":"...","span_start":44,"span_end":61,"confidence":0.95},
    {"label":"RiskFactor","surface":"EUR swap curve","canonical":"EUR Swap Curve","description":"...","span_start":108,"span_end":122,"confidence":0.9},
    {"label":"Calibration","surface":"SVI swaption vol cube","canonical":"SVI Swaption Vol Cube","description":"...","span_start":150,"span_end":171,"confidence":0.9},
    {"label":"Model","surface":"Hull-White 2-factor","canonical":"Hull-White 2-factor","description":"...","span_start":211,"span_end":230,"confidence":0.95}
  ],
  "relations": [
    {"predicate":"OWNED_BY","src_canonical":"SABR-LMM","src_label":"Model","dst_canonical":"IR Exotics","dst_label":"Desk","description":"...","confidence":0.95},
    {"predicate":"APPLIES_TO","src_canonical":"SABR-LMM","src_label":"Model","dst_canonical":"Bermudan Swaption","dst_label":"Product","description":"...","confidence":0.9},
    {"predicate":"DEPENDS_ON","src_canonical":"SABR-LMM","src_label":"Model","dst_canonical":"EUR Swap Curve","dst_label":"RiskFactor","description":"...","confidence":0.85},
    {"predicate":"CALIBRATES_WITH","src_canonical":"SABR-LMM","src_label":"Model","dst_canonical":"SVI Swaption Vol Cube","dst_label":"Calibration","description":"...","confidence":0.95},
    {"predicate":"SUPERSEDES","src_canonical":"SABR-LMM","src_label":"Model","dst_canonical":"Hull-White 2-factor","dst_label":"Model","description":"Replaced legacy Hull-White 2-factor in 2024.","confidence":0.9}
  ]
}
```

### Exemplar B — pure methodology page (avoid spurious model entities)

```text
Input:
"The SVI parameterisation expresses total implied variance as
w(k) = a + b * (rho*(k - m) + sqrt((k - m)^2 + sigma^2)).
This form guarantees no calendar-spread arbitrage when fitted with the standard
no-arb constraints (Roper 2010)."

Output:
{
  "entities": [
    {"label":"Methodology","surface":"SVI parameterisation","canonical":"SVI Parameterisation","description":"Stochastic-volatility-inspired implied-variance functional form...","span_start":4,"span_end":24,"confidence":0.99},
    {"label":"Concept","surface":"calendar-spread arbitrage","canonical":"Calendar-Spread Arbitrage","description":"...","span_start":150,"span_end":175,"confidence":0.85}
  ],
  "relations": []
}
```

(Do NOT invent a `:Model` for "SVI" — SVI here is a parameterisation, not a model. The label discipline matters.)

### Exemplar C — refusal on weak signal

If a chunk only mentions an entity in passing without asserting any relation, emit the entity with low confidence and **no relations**. Do not invent edges.

## 5. Self-reflection (gleaning) prompt

Run after the first pass:

```
Below is a chunk of text and the entities/relations already extracted from it.

--- TEXT ---
{chunk.text}

--- ALREADY EXTRACTED ---
{json.dumps(first_pass_output)}

Did the first pass miss any entities or relations that satisfy the schema rules?
Reply with a single token: YES or NO.
```

If `YES`, re-prompt with the original system prompt and add the first-pass output as `PREVIOUSLY EXTRACTED`. Merge the second pass's output with the first.

## 6. Cost / model-choice notes

- **GPT-4o** or equivalent for extraction is the strong baseline. Cheaper open models (Llama-3-70B, Qwen) are workable for the wiki ingest if you spend the time on few-shot tuning, but accept ~10-15% recall drop.
- Self-reflection roughly **doubles** entity recall in MS GraphRAG's experiments but **doubles cost** too — only enable on chunks where coverage matters (e.g. methodology pages) or when chunk size > 1000 tokens.
- Cache extraction outputs by `chunk.content_hash`; only re-run when the chunk changes.
