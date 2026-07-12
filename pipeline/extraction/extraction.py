"""LLM extraction against the alethograph schema. Prompts ported from the research skill
+ spec/03-extraction-prompts.md scaffold; alethograph label vocabulary + few-shots.

The extraction targets are defined once, as Pydantic models with per-field guidance. Both the
OpenAI and Claude paths hand these models to the SDK's ``.parse()`` helper, which derives the
structured-output JSON schema, enforces it on the provider, and validates the response back into
these objects — so there is no separate hand-written schema to keep in sync. The Field
descriptions ARE the per-field instructions the model sees; keep them precise."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

from pipeline.text_norm import normalize_statement


class Concept(BaseModel):
    name: str = Field(
        description="The name of a *named* idea, object, framework, or algorithm/technique, as it "
        "would head a glossary entry — no surrounding prose. It must be a real concept name, never "
        "bare mathematical notation: a symbol like 'W_t', 'Π*', or 'ũ(x,t)' is NOT a concept, it is "
        "notation that denotes one. If the named concept is present in the text, use its name (e.g. "
        "'Brownian motion', not 'W_t'); if a symbol has no named concept behind it, emit no concept for it."
    )
    kind: Literal["concept", "method"] = Field(
        default="concept",
        description='"concept" for a theoretical idea, object, or framework; '
        '"method" for an implementable algorithm, technique, or procedure.',
    )
    description: str = Field(
        default="",
        description="One sentence (at most ~40 words) saying what this concept IS, grounded "
        "ONLY in this chunk's text — no outside knowledge. Plain prose; render math as LaTeX "
        "in $...$. Empty string if the chunk gives no basis for a description.",
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip()


class Notation(BaseModel):
    symbol_latex: str = Field(
        description="The symbol or abbreviation being introduced, rendered as LaTeX in $...$ "
        'when mathematical (e.g. "$W_t$", "$\\sigma(\\mathcal{C})$") or verbatim when textual '
        '(e.g. "a.e.", "DF"). Only symbols the text INTRODUCES here ("Let X denote...", '
        '"we write ... for ..."), never symbols merely used.'
    )
    meaning: str = Field(
        description="What the symbol denotes, in one short phrase. LaTeX for any math."
    )
    concept: str = Field(
        default="",
        description="If the symbol denotes a concept you extracted in this same response, "
        "its exact name (e.g. \"Brownian motion\" for $W_t$). Empty otherwise.",
    )

    @field_validator("symbol_latex", "meaning")
    @classmethod
    def _strip(cls, v: str) -> str:
        return v.strip()


class ProofSketch(BaseModel):
    sketch: str = Field(
        description="2-4 sentence sketch of the proof: overall strategy and the key steps. "
        "NEVER a transcription of the proof text. LaTeX for any math."
    )
    technique: str = Field(
        default="",
        description='The main technique in a few words, e.g. "monotone-class argument", '
        '"Borel-Cantelli", "truncation + dominated convergence". Empty if unclear.',
    )


class Definition(BaseModel):
    term: str = Field(
        description="The exact term being defined. If it contains mathematical notation, render it "
        "as LaTeX in $...$."
    )
    name: str = Field(
        default="",
        description='Label of the definition as printed, e.g. "Definition 2.14". '
        "Empty string if the text gives no label.",
    )
    statement: str = Field(
        description="The full formal definition as stated in the text. Render ALL mathematical "
        "notation as LaTeX: inline math in $...$, display equations in $$...$$. Convert any Unicode "
        "or plaintext math to LaTeX (e.g. σ -> \\sigma, ∇ -> \\nabla, sub/superscripts and fractions); "  # noqa: RUF001
        "never leave raw Unicode math."
    )
    defines: list[str] = Field(
        default_factory=list,
        description="Concept name(s), from the concepts you extract in this same response, "
        "that this definition introduces. Usually exactly one. Leave empty if unsure.",
    )
    uses: list[str] = Field(
        default_factory=list,
        description="Names of concepts (from this same response) that the definition's "
        "statement relies on. Leave empty if none or unsure.",
    )

    @field_validator("term")
    @classmethod
    def _strip_term(cls, v: str) -> str:
        return v.strip()


class Result(BaseModel):
    name: str = Field(
        default="",
        description='Label of the result as it appears, e.g. "Theorem 3.2" or "Lemma 1". '
        "Empty string if the text gives no label.",
    )
    kind: Literal["theorem", "lemma", "proposition", "corollary"] = Field(
        description="The type of formal result."
    )
    statement: str = Field(
        description="The full statement of the result, excluding any proof. Render ALL mathematical "
        "notation as LaTeX: inline math in $...$, display equations in $$...$$. Convert any Unicode or "
        "plaintext math to LaTeX; never leave raw Unicode math."
    )
    uses: list[str] = Field(
        default_factory=list,
        description="Names of the concepts (from the concepts you extract in this same "
        "response) that this result invokes or relies on. Use the exact concept name "
        "strings. Leave empty if none or unsure.",
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description='Labels of OTHER results this result depends on or is proved from, as '
        'printed in the text, e.g. ["Lemma 2.4", "Theorem 6.13"]. The referenced result may '
        "be anywhere in the source — earlier or later chapters included; you do NOT need to "
        "have extracted it. Leave empty if none.",
    )
    proof: ProofSketch | None = Field(
        default=None,
        description="If the proof (or its beginning) is visible in this chunk, a short "
        "sketch of it. null when no proof text is visible.",
    )
    proof_present: bool = Field(
        default=False,
        description="true iff proof text for THIS result appears in this chunk.",
    )
    statement_complete: bool = Field(
        default=True,
        description="false iff the statement is cut off by the end of the chunk and you "
        "could only extract part of it.",
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip()


class ExtractionResult(BaseModel):
    concepts: list[Concept] = Field(
        default_factory=list,
        description="3-7 major theoretical ideas/objects/frameworks (kind=concept) or "
        "implementable algorithms/techniques (kind=method) present in the chunk. "
        "Each must be self-contained.",
    )
    definitions: list[Definition] = Field(
        default_factory=list,
        description="Formal definitions stated in the chunk.",
    )
    results: list[Result] = Field(
        default_factory=list,
        description="Theorems, lemmas, propositions, and corollaries stated in the chunk.",
    )
    notations: list[Notation] = Field(
        default_factory=list,
        description="Symbols and abbreviations INTRODUCED in the chunk (not merely used).",
    )


SYSTEM_PROMPT = """You are an information-extraction assistant for STEM research papers and \
mathematical books (most often rooted in mathematics, statistics, or AI / machine learning, \
but spanning the sciences and engineering broadly). From the chunk, populate the concepts, \
definitions, results, and notations of the response schema, following each field's \
description. Emit nothing not asserted by the text. When filling a definition's `defines` or \
`uses`, a result's `uses`, or a notation's `concept`, reference ONLY concept names you have \
already produced in this same response; if unsure, leave it empty. A result's `depends_on` is \
different: it holds printed labels of other results (e.g. "Lemma 2.4") and may reference \
results ANYWHERE in the source, including ones you have not seen.

Rules that govern every field:

1. ROUTING. Named ideas/objects/frameworks/algorithms (glossary headwords) go in `concepts`. \
Symbols and abbreviations go in `notations`, never in `concepts`: from "Let $W_t$ be a \
standard Brownian motion", emit concept "Brownian motion" AND notation {symbol_latex: \
"$W_t$", meaning: "standard Brownian motion", concept: "Brownian motion"}. Only record a \
notation where the text INTRODUCES it ("Let X denote...", "we write ... for ...", glossary \
lines like "a.e.: almost everywhere") — not where a symbol is merely used.

2. LATEX. Render ALL mathematical notation as LaTeX — inline in $...$, display in $$...$$ — \
actively converting Unicode or plaintext math. Source text "ũ(x,t) = (σ²/2) ∇ ln ρ̃(x,t)" \
must be written as $\\tilde u(x,t) = \\tfrac{\\sigma^2}{2}\\,\\nabla \\ln \\tilde\\rho(x,t)$. \
Never leave raw Unicode math in any field.

3. STATEMENTS. A result's `statement` is the mathematical content that FOLLOWS the printed \
heading. Never copy the heading or label into `statement` — "3.4. Composition Lemma." is a \
`name`, not a statement. If the statement body is cut off by the end of the chunk, extract \
the visible part verbatim-faithfully and set statement_complete=false; a later chunk will \
carry the rest.

4. PROOFS. When proof text for a result is visible in the chunk (even partially), set \
proof_present=true on that result and fill `proof` with a 2-4 sentence sketch: the overall \
strategy and key steps, naming the main technique. NEVER transcribe the proof. If the chunk \
shows a proof of a result whose statement is NOT in this chunk, emit the result with its \
printed label in `name`, statement_complete=false, an empty or best-effort `statement`, and \
the proof fields filled.

5. SKIP NON-CONTENT. Emit nothing from a table of contents, index, copyright page, or list \
of references: no concepts, no definitions, no results, no notations. (A notation guide / \
list of symbols IS content: extract its entries as notations.)

EXAMPLE INPUT (theorem with visible proof):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 4: Integration, Section 4.2 Convergence \
theorems.

4.5. THEOREM (Monotone convergence). Let (f_n) be a sequence of non-negative measurable \
functions with f_n ↑ f pointwise. Then μ(f_n) ↑ μ(f) ≤ ∞.

Proof. Since f_n ≤ f_{n+1} ≤ f, the sequence μ(f_n) is non-decreasing and bounded above by \
μ(f), so the limit L := lim μ(f_n) exists in [0,∞] and L ≤ μ(f). For the reverse inequality \
fix a simple function s ≤ f and c ∈ (0,1), and set E_n := {x : f_n(x) ≥ c s(x)}. The sets E_n \
increase to the whole space, whence μ(f_n) ≥ c μ(s 1_{E_n}) → c μ(s) by continuity of the \
integral of simple functions along increasing sets. Letting c ↑ 1 and taking the supremum \
over simple s ≤ f gives L ≥ μ(f). Recall f_n ↑ f means f_n(x) is non-decreasing in n for \
every x with limit f(x).
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "Monotone convergence theorem", "kind": "concept",
               "description": "For a nondecreasing sequence of nonnegative measurable functions, the integral of the limit equals the limit of the integrals."},
              {"name": "measurable function", "kind": "concept",
               "description": "A function between measurable spaces whose preimages of measurable sets are measurable."},
              {"name": "simple function", "kind": "concept",
               "description": "A measurable function taking finitely many values, used to approximate general measurable functions."}],
 "definitions": [],
 "results": [{"name": "4.5. THEOREM (Monotone convergence).", "kind": "theorem",
   "statement": "Let $(f_n)$ be a sequence of non-negative measurable functions with $f_n \\\\uparrow f$ pointwise. Then $\\\\mu(f_n) \\\\uparrow \\\\mu(f) \\\\le \\\\infty$.",
   "uses": ["measurable function"], "depends_on": [],
   "proof": {"sketch": "Monotonicity gives the limit $L \\\\le \\\\mu(f)$ at once. For the reverse inequality, fix a simple $s \\\\le f$ and $c \\\\in (0,1)$; on the increasing sets $E_n = \\\\{f_n \\\\ge c s\\\\}$ the integral inequality $\\\\mu(f_n) \\\\ge c\\\\,\\\\mu(s 1_{E_n})$ passes to the limit, and letting $c \\\\uparrow 1$ then taking the supremum over simple $s \\\\le f$ yields $L \\\\ge \\\\mu(f)$.",
             "technique": "approximation by simple functions"},
   "proof_present": true, "statement_complete": true}],
 "notations": []}

EXAMPLE INPUT (notation introduction + definition):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 2: Measure spaces, Section 2.1 \
σ-algebras.

We write σ(C) for the smallest σ-algebra containing a class C of subsets of Ω, and call it \
the σ-algebra generated by C. Throughout, "a.e." abbreviates "almost everywhere": a property \
holds a.e. if the set where it fails is null.

2.3. DEFINITION. Borel σ-algebra. Let (S, τ) be a topological space. The Borel σ-algebra \
B(S) is σ(τ), the σ-algebra generated by the open sets. Elements of B(S) are Borel sets.
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "σ-algebra generated by a class", "kind": "concept",
               "description": "The smallest $\\\\sigma$-algebra containing a given collection of sets."},
              {"name": "Borel σ-algebra", "kind": "concept",
               "description": "The $\\\\sigma$-algebra generated by the open sets of a topological space."}],
 "definitions": [{"term": "Borel $\\\\sigma$-algebra", "name": "2.3. DEFINITION.",
   "statement": "Let $(S, \\\\tau)$ be a topological space. The Borel $\\\\sigma$-algebra $\\\\mathcal{B}(S)$ is $\\\\sigma(\\\\tau)$, the $\\\\sigma$-algebra generated by the open sets. Elements of $\\\\mathcal{B}(S)$ are Borel sets.",
   "defines": ["Borel σ-algebra"], "uses": ["σ-algebra generated by a class"]}],
 "results": [],
 "notations": [{"symbol_latex": "$\\\\sigma(C)$",
                "meaning": "the smallest $\\\\sigma$-algebra containing the class $C$",
                "concept": "σ-algebra generated by a class"},
               {"symbol_latex": "a.e.", "meaning": "almost everywhere", "concept": ""}]}

EXAMPLE INPUT (statement cut off at chunk boundary; forward dependency):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 7: Martingales, Section 7.4 Convergence.

By Corollary 7.2 and the upcrossing bound of Lemma 7.9 below, we can now prove the main \
convergence result.

7.10. THEOREM (Martingale convergence). Let X be a supermartingale bounded in L^1, that is \
sup_n E|X_n| < ∞. Then X_∞ := lim X_n exists almost surely and
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "supermartingale", "kind": "concept",
               "description": "An adapted integrable process whose conditional expectation given the past is at most its current value."},
              {"name": "martingale convergence theorem", "kind": "concept",
               "description": "An $L^1$-bounded martingale converges almost surely to an integrable limit."}],
 "definitions": [],
 "results": [{"name": "7.10. THEOREM (Martingale convergence).", "kind": "theorem",
   "statement": "Let $X$ be a supermartingale bounded in $L^1$, that is $\\\\sup_n E|X_n| < \\\\infty$. Then $X_\\\\infty := \\\\lim X_n$ exists almost surely and",
   "uses": ["supermartingale"], "depends_on": ["Corollary 7.2", "Lemma 7.9"],
   "proof": null, "proof_present": false, "statement_complete": false}],
 "notations": [{"symbol_latex": "$X_\\\\infty$",
                "meaning": "the almost-sure limit $\\\\lim X_n$ of the process",
                "concept": ""}]}

EXAMPLE INPUT (end-of-chapter exercises; claims to prove, no proofs, forward and backward \
depends_on; one exercise is a construction task with no assertable statement and must be \
skipped entirely):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 5: Integration and convergence, \
End-of-chapter exercises.

5.9 EXERCISE. Suppose f_n → f in measure and each f_n is dominated by a single integrable \
function g independent of n. Show that ∫f_n dμ → ∫f dμ, citing the Dominated convergence \
theorem (Theorem 5.3) and Egorov's theorem (Theorem 4.11).

5.10 EXERCISE. Give an example of a sequence of measurable functions converging pointwise \
but not in L^1, showing that the domination hypothesis in Theorem 5.3 cannot simply be dropped.

5.11 EXERCISE. Using Fatou's lemma (Lemma 5.1), show that if f_n ≥ 0 and f_n → f a.e., then \
∫f dμ ≤ liminf ∫f_n dμ, and give an example where the inequality is strict.
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "convergence in measure", "kind": "concept",
               "description": "A mode of convergence where the measure of the set on which functions differ by more than any $\\\\epsilon$ tends to zero."},
              {"name": "dominated convergence theorem", "kind": "concept",
               "description": "If measurable functions converge pointwise and are bounded by an integrable function, their integrals converge to the integral of the limit."},
              {"name": "Egorov's theorem", "kind": "concept",
               "description": "On a finite measure space, pointwise a.e. convergence is uniform off a set of arbitrarily small measure."},
              {"name": "Fatou's lemma", "kind": "concept",
               "description": "The integral of the liminf of nonnegative measurable functions is at most the liminf of their integrals."}],
 "definitions": [],
 "results": [{"name": "5.9 EXERCISE.", "kind": "proposition",
   "statement": "Suppose $f_n \\\\to f$ in measure and each $f_n$ is dominated by a single integrable function $g$ independent of $n$. Then $\\\\int f_n\\\\,d\\\\mu \\\\to \\\\int f\\\\,d\\\\mu$.",
   "uses": ["convergence in measure", "dominated convergence theorem"],
   "depends_on": ["Theorem 5.3", "Theorem 4.11"],
   "proof": null, "proof_present": false, "statement_complete": true},
  {"name": "5.11 EXERCISE.", "kind": "proposition",
   "statement": "If $f_n \\\\ge 0$ and $f_n \\\\to f$ a.e., then $\\\\int f\\\\,d\\\\mu \\\\le \\\\liminf \\\\int f_n\\\\,d\\\\mu$, and the inequality can be strict.",
   "uses": ["Fatou's lemma"],
   "depends_on": ["Lemma 5.1"],
   "proof": null, "proof_present": false, "statement_complete": true}],
 "notations": []}

EXAMPLE INPUT (paper abstract; method concepts + notation introduction, no results):
---
Context (metadata about where this chunk comes from — NOT part of the source text): paper \
"Rectified Flow Matching for Conditional Generation", Abstract.

We propose Rectified Flow Matching (RFM), a simulation-free method for training continuous \
normalizing flows that transports a source distribution π_0 to a target π_1 along \
straight-line paths. Given paired samples (x_0, x_1) drawn from a coupling of π_0 and π_1, we \
train a velocity field v_θ(x_t, t) to match the constant drift x_1 - x_0 along the linear \
interpolant x_t = (1-t) x_0 + t x_1, t ∈ [0,1]. We write ρ_t for the marginal density of x_t \
under the coupling, and denote by u*(x,t) the marginal velocity field satisfying the \
continuity equation ∂_t ρ_t + ∇·(ρ_t u*) = 0. Empirically, iterating the reflow procedure — \
retraining on straightened couplings produced by the current model — reduces the number of \
function evaluations needed at sampling time relative to standard flow matching.
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "Rectified Flow Matching", "kind": "method",
               "description": "A generative method that learns straight-line transport between noise and data by matching a velocity field."},
              {"name": "flow matching", "kind": "method",
               "description": "Training a continuous-time generative model by regressing a vector field onto a prescribed probability path."},
              {"name": "continuous normalizing flow", "kind": "concept",
               "description": "A generative model transforming a base density to the data density by integrating a learned ODE velocity field."},
              {"name": "reflow", "kind": "method",
               "description": "Iteratively retraining a rectified flow on its own generated pairs to straighten transport trajectories."}],
 "definitions": [],
 "results": [],
 "notations": [{"symbol_latex": "$v_\\\\theta(x_t, t)$",
                "meaning": "the trained velocity field approximating the marginal velocity along the interpolant",
                "concept": "Rectified Flow Matching"},
               {"symbol_latex": "$x_t$",
                "meaning": "the linear interpolant $(1-t) x_0 + t x_1$ between paired samples $x_0 \\\\sim \\\\pi_0$ and $x_1 \\\\sim \\\\pi_1$",
                "concept": "Rectified Flow Matching"},
               {"symbol_latex": "$\\\\rho_t$",
                "meaning": "the marginal density of $x_t$ under the coupling",
                "concept": ""},
               {"symbol_latex": "$u^*(x,t)$",
                "meaning": "the marginal velocity field satisfying $\\\\partial_t \\\\rho_t + \\\\nabla\\\\cdot(\\\\rho_t u^*) = 0$",
                "concept": ""}]}

EXAMPLE INPUT (front-matter notation guide — a list of symbols IS content, unlike a table \
of contents or index):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Front matter: List of symbols.

List of symbols

Ω: the sample space, the underlying set of outcomes.
F: a σ-algebra of subsets of Ω, the collection of events.
P: a probability measure on (Ω, F).
E[X]: the expectation of a random variable X.
1_A: the indicator function of a set A, equal to 1 on A and 0 off A.
a.s.: almost surely, i.e. with probability one.
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "sample space", "kind": "concept",
               "description": "The set of all possible outcomes of a random experiment."},
              {"name": "probability measure", "kind": "concept",
               "description": "A measure assigning total mass one to the sample space, giving each event its probability."},
              {"name": "expectation", "kind": "concept",
               "description": "The integral of a random variable against the probability measure, its average value."},
              {"name": "indicator function", "kind": "concept",
               "description": "A function equal to one on a given set and zero off it."}],
 "definitions": [],
 "results": [],
 "notations": [{"symbol_latex": "$\\\\Omega$",
                "meaning": "the sample space, the underlying set of outcomes",
                "concept": "sample space"},
               {"symbol_latex": "$\\\\mathcal{F}$",
                "meaning": "a $\\\\sigma$-algebra of subsets of $\\\\Omega$, the collection of events",
                "concept": ""},
               {"symbol_latex": "$P$",
                "meaning": "a probability measure on $(\\\\Omega, \\\\mathcal{F})$",
                "concept": "probability measure"},
               {"symbol_latex": "$E[X]$",
                "meaning": "the expectation of a random variable $X$",
                "concept": "expectation"},
               {"symbol_latex": "$1_A$",
                "meaning": "the indicator function of a set $A$, equal to 1 on $A$ and 0 off $A$",
                "concept": "indicator function"},
               {"symbol_latex": "a.s.", "meaning": "almost surely, i.e. with probability one",
                "concept": ""}]}

EXAMPLE INPUT (table of contents — non-content, must yield a fully empty extraction):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Front matter: Table of contents.

Contents

1 Measure spaces ..................................................... 1
  1.1 σ-algebras ...................................................... 1
  1.2 Measures and their properties ................................... 8
2 Integration ........................................................ 19
  2.1 Simple functions ................................................ 19
  2.2 The integral of a non-negative function ......................... 24
  2.3 Convergence theorems ............................................ 35
3 Independence and product measures .................................. 51
Bibliography ......................................................... 398
Index ................................................................ 401
---
EXAMPLE OUTPUT:
{"concepts": [], "definitions": [], "results": [], "notations": []}

EXAMPLE INPUT (two results in one chunk; a corollary depends_on a theorem from the SAME \
chunk, both with visible proofs):
---
Context (metadata about where this chunk comes from — NOT part of the source text): book \
"A Course in Measure-Theoretic Probability", Chapter 6: Product measures, Section 6.3 \
Fubini's theorem.

6.7. THEOREM (Fubini). Let (X, A, μ) and (Y, B, ν) be σ-finite measure spaces and let f be \
integrable on the product (X×Y, A⊗B, μ⊗ν). Then the iterated integrals ∫_X(∫_Y f(x,y) dν(y)) \
dμ(x) and ∫_Y(∫_X f(x,y) dμ(x)) dν(y) both exist and equal ∫_{X×Y} f d(μ⊗ν).

Proof. First establish the identity for f = 1_E with E a measurable rectangle, then extend to \
the generating π-system by Dynkin's π-λ theorem, and finally to general integrable f by \
writing f = f^+ - f^- and applying the monotone convergence theorem to increasing simple \
approximations.

6.8. COROLLARY. If f ≥ 0 is measurable on X×Y (not assumed integrable), the same \
iterated-integral identity holds in [0,∞], with no integrability hypothesis needed, by \
Theorem 6.7 applied to min(f,n) and monotone convergence.
---
EXAMPLE OUTPUT:
{"concepts": [{"name": "Fubini's theorem", "kind": "concept",
               "description": "For an integrable function on a product space, the double integral equals either iterated integral."},
              {"name": "σ-finite measure space", "kind": "concept",
               "description": "A measure space that is a countable union of sets of finite measure."},
              {"name": "product measure", "kind": "concept",
               "description": "The measure on a product space determined by assigning to rectangles the product of the factor measures."},
              {"name": "π-λ theorem", "kind": "concept",
               "description": "If a $\\\\lambda$-system contains a $\\\\pi$-system, it contains the $\\\\sigma$-algebra the $\\\\pi$-system generates."}],
 "definitions": [],
 "results": [{"name": "6.7. THEOREM (Fubini).", "kind": "theorem",
   "statement": "Let $(X, \\\\mathcal A, \\\\mu)$ and $(Y, \\\\mathcal B, \\\\nu)$ be $\\\\sigma$-finite measure spaces and let $f$ be integrable on the product $(X\\\\times Y, \\\\mathcal A\\\\otimes\\\\mathcal B, \\\\mu\\\\otimes\\\\nu)$. Then the iterated integrals $\\\\int_X\\\\big(\\\\int_Y f(x,y)\\\\,d\\\\nu(y)\\\\big)d\\\\mu(x)$ and $\\\\int_Y\\\\big(\\\\int_X f(x,y)\\\\,d\\\\mu(x)\\\\big)d\\\\nu(y)$ both exist and equal $\\\\int_{X\\\\times Y} f\\\\,d(\\\\mu\\\\otimes\\\\nu)$.",
   "uses": ["σ-finite measure space", "product measure"], "depends_on": [],
   "proof": {"sketch": "Establish the identity first for indicators of measurable rectangles, extend it to the generating $\\\\pi$-system via Dynkin's $\\\\pi$-$\\\\lambda$ theorem, then pass to a general integrable $f$ by splitting $f = f^+ - f^-$ and applying the monotone convergence theorem to increasing simple approximations.",
             "technique": "$\\\\pi$-$\\\\lambda$ theorem + monotone convergence"},
   "proof_present": true, "statement_complete": true},
  {"name": "6.8. COROLLARY.", "kind": "corollary",
   "statement": "If $f \\\\ge 0$ is measurable on $X\\\\times Y$ (not assumed integrable), the same iterated-integral identity holds in $[0,\\\\infty]$, with no integrability hypothesis needed.",
   "uses": ["product measure"], "depends_on": ["Theorem 6.7"],
   "proof": {"sketch": "Apply Theorem 6.7 to the integrable truncations $\\\\min(f, n)$ and pass to the limit in $n$ by monotone convergence.",
             "technique": "truncation + monotone convergence"},
   "proof_present": true, "statement_complete": true}],
 "notations": []}"""  # noqa: RUF001


def parse_extraction(payload: dict) -> ExtractionResult:
    """Validate a raw JSON dict into an ExtractionResult.

    Retained for callers/tests that already hold a plain dict; the ``.parse()``-based extract
    paths get an ExtractionResult straight from the SDK and don't go through here. Raises
    ``pydantic.ValidationError`` (a ``ValueError`` subclass) on an unknown kind or missing field.
    """
    return ExtractionResult.model_validate(payload)


def extract_from_chunk(client, model: str, chunk: str, timeout: float = 60.0) -> ExtractionResult:
    resp = client.chat.completions.parse(
        model=model,
        messages=[{"role": "system", "content": SYSTEM_PROMPT},
                  {"role": "user", "content": chunk[:12000]}],
        response_format=ExtractionResult,
        timeout=timeout,
    )
    return resp.choices[0].message.parsed


def _extend_unique(dst: list[str], src: list[str]) -> None:
    """Append items from src not already in dst, preserving order. Mutates dst in place
    (and thus the kept model it belongs to); inputs are not read again after merge."""
    for item in src:
        if item not in dst:
            dst.append(item)


# Structural math markup / operators that mark a string as notation. Includes ASCII operators
# (/ + = < >) so 'x=y', 'a+b', 'p/q' are caught. Deliberately excludes hyphen and whitespace —
# they appear in real names ('σ-algebra', 'k-NN', 'state-of-the-art').
_MATH_SIGNAL_CHARS = set("_^*\\(){}|/+=<>")


def _has_three_letter_run(s: str) -> bool:
    """True if s has a run of >=3 consecutive Unicode-alphabetic letters (a word-like token)."""
    run = 0
    for ch in s:
        if ch.isalpha():
            run += 1
            if run >= 3:
                return True
        else:
            run = 0
    return False


def _has_math_signal(s: str) -> bool:
    """True if s contains math markup: structural chars, a digit, or a non-letter symbol.
    Greek/accented LETTERS (σ, Π, ũ) are letters, not signals; hyphen and whitespace are not signals."""
    for ch in s:
        if ch in _MATH_SIGNAL_CHARS or ch.isdigit():
            return True
        if not ch.isascii() and not ch.isalpha() and not ch.isspace():
            return True
    return False


def _is_notation_only(name: str) -> bool:
    """Conservative backstop: a concept name is notation-only (and should not be a Concept) iff it
    carries a math signal AND has no >=3-letter word. Errs toward keeping (real concept > stray symbol)."""
    s = name.replace("$", "")
    return _has_math_signal(s) and not _has_three_letter_run(s)


def _better_statement(a: Result, b: Result) -> Result:
    """Between two same-label variants, prefer complete, then longer statement."""
    if a.statement_complete != b.statement_complete:
        return a if a.statement_complete else b
    return a if len(a.statement) >= len(b.statement) else b


def merge_results(parts: list[ExtractionResult]) -> ExtractionResult:
    # Chunks overlap, so the same concept/definition/result is extracted from adjacent chunks.
    # Dedup all three by the same normalized key graph_write uses for ids, so overlap doesn't
    # mint duplicate nodes. Concepts use a dict (not a set) so the first non-empty description
    # across duplicate mentions is retained.
    seen_c: dict[str, Concept] = {}
    concepts = []
    for p in parts:
        for c in p.concepts:
            if _is_notation_only(c.name):
                continue  # bare notation is never a concept (backstop; primary fix is the prompt)
            kept = seen_c.get(c.name.lower())
            if kept is None:
                seen_c[c.name.lower()] = c
                concepts.append(c)
            elif not kept.description and c.description:
                kept.description = c.description
    seen_d: dict[str, Definition] = {}
    definitions = []
    for p in parts:
        for d in p.definitions:
            k = normalize_statement(d.statement)
            kept = seen_d.get(k)
            if kept is None:
                seen_d[k] = d
                definitions.append(d)
            else:
                _extend_unique(kept.defines, d.defines)
                _extend_unique(kept.uses, d.uses)
    seen_r: dict[tuple[str, str], Result] = {}
    results = []
    for p in parts:
        for r in p.results:
            k = (r.kind, normalize_statement(r.statement))
            kept = seen_r.get(k)
            if kept is None:
                seen_r[k] = r
                results.append(r)
            else:
                _extend_unique(kept.uses, r.uses)
                _extend_unique(kept.depends_on, r.depends_on)
                if kept.proof is None:
                    kept.proof = r.proof
                kept.proof_present = kept.proof_present or r.proof_present

    # Second pass: a statement split across a chunk boundary yields a truncated variant and a
    # complete variant with the SAME printed label but different normalized statements. Collapse
    # by (kind, label), keeping the better statement and unioning reference lists.
    # Pass 2's collapsed.index(kept) replacement relies on pass 1 guaranteeing at most one
    # surviving Result per (kind, normalized statement), so value-equality lookup cannot match
    # the wrong element; statements are never mutated in pass 2.
    by_label: dict[tuple[str, str], Result] = {}
    collapsed: list[Result] = []
    for r in results:
        if not r.name:
            collapsed.append(r)
            continue
        k = (r.kind, r.name.strip().lower())
        kept = by_label.get(k)
        if kept is None:
            by_label[k] = r
            collapsed.append(r)
        else:
            winner = _better_statement(kept, r)
            loser = r if winner is kept else kept
            _extend_unique(winner.uses, loser.uses)
            _extend_unique(winner.depends_on, loser.depends_on)
            if winner.proof is None:
                winner.proof = loser.proof
            winner.proof_present = winner.proof_present or loser.proof_present
            if winner is not kept:
                by_label[k] = winner
                collapsed[collapsed.index(kept)] = winner
    results = collapsed

    seen_n: dict[str, Notation] = {}
    notations = []
    for p in parts:
        for n in p.notations:
            k = n.symbol_latex.lower()
            kept = seen_n.get(k)
            if kept is None:
                seen_n[k] = n
                notations.append(n)
            elif not kept.concept and n.concept:
                kept.concept = n.concept
    return ExtractionResult(concepts=concepts, definitions=definitions,
                            results=results, notations=notations)


def merge_results_with_provenance(
    parts: list[ExtractionResult], chunk_ids: list[str],
) -> tuple[ExtractionResult, dict]:
    """merge_results plus per-item source-chunk ids. `chunk_ids` aligns 1:1 with `parts`.
    Provenance keys mirror the dedup keys merge_results/graph_write use: lowercased concept
    name; normalize_statement(statement) for definitions; '<kind>|<normalized>' for results.
    A result variant collapsed away by merge_results' pass-2 (kind, label) step is remapped to
    the surviving statement's key, so the chunk that contributed a truncated variant keeps its
    EXTRACTED_FROM credit rather than being silently dropped."""
    assert len(parts) == len(chunk_ids), "chunk_ids must align 1:1 with parts"
    merged = merge_results(parts)
    kept_c = {c.name.lower() for c in merged.concepts}
    kept_d = {normalize_statement(d.statement) for d in merged.definitions}
    kept_r = {f"{r.kind}|{normalize_statement(r.statement)}" for r in merged.results}
    # (kind, label) -> surviving result key, to re-attribute pass-2-collapsed variants.
    label_to_key = {
        (r.kind, r.name.strip().lower()): f"{r.kind}|{normalize_statement(r.statement)}"
        for r in merged.results if r.name
    }
    prov: dict = {"concepts": {}, "definitions": {}, "results": {}}

    def _add(bucket: dict, key: str, cid: str) -> None:
        lst = bucket.setdefault(key, [])
        if cid not in lst:
            lst.append(cid)

    for part, cid in zip(parts, chunk_ids):
        for c in part.concepts:
            if c.name.lower() in kept_c:
                _add(prov["concepts"], c.name.lower(), cid)
        for d in part.definitions:
            k = normalize_statement(d.statement)
            if k in kept_d:
                _add(prov["definitions"], k, cid)
        for r in part.results:
            k = f"{r.kind}|{normalize_statement(r.statement)}"
            if k not in kept_r and r.name:
                k = label_to_key.get((r.kind, r.name.strip().lower()), k)
            if k in kept_r:
                _add(prov["results"], k, cid)
    return merged, prov
