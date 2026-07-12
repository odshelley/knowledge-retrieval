"""Chapter extraction plumbing: context headers (user-message side, so provider prompt
caches stay warm), page attribution, chapter payload assembly. Reuses the paper models."""
from __future__ import annotations

from pipeline.extraction.extraction import ExtractionResult
from pipeline.text_norm import normalize_statement

_CONTEXT_TEMPLATE = (
    "Context (metadata about where this chunk comes from — NOT part of the source text): "
    'book "{book}", Chapter {ch_no}: {ch_title}, Section {sec_no} {sec_title}. '
    "Capture each definition/result label exactly as printed in the book "
    '(e.g. "Theorem 3.1.2"), in the `name` field.\n\n---\n\n'
)


def chunk_with_context(book_title: str, chapter: dict, section: dict, text: str) -> str:
    header = _CONTEXT_TEMPLATE.format(
        book=book_title, ch_no=chapter["number"], ch_title=chapter["title"],
        sec_no=section["number"], sec_title=section["title"])
    return header + text


def attach_pages(merged: ExtractionResult,
                 chunk_extractions: list[tuple[ExtractionResult, int, int]],
                 ) -> tuple[list[dict], list[dict], list[dict]]:
    """Attach first-seen page to each merged definition/result; collect per-chunk proof
    locations. chunk_extractions tuples are (extraction, page_start, chunk_position)."""
    def_pages: dict[str, int] = {}
    res_pages: dict[tuple[str, str], int] = {}
    proof_rows: list[dict] = []
    for er, page, position in chunk_extractions:
        for d in er.definitions:
            def_pages.setdefault(normalize_statement(d.statement), page)
        for r in er.results:
            key = (r.kind, normalize_statement(r.statement))
            res_pages.setdefault(key, page)
            if r.proof_present:
                proof_rows.append({"result_key": list(key), "label": r.name,
                                   "position": position})
    defs = [{**d.model_dump(), "page": def_pages.get(normalize_statement(d.statement))}
            for d in merged.definitions]
    results = [{**r.model_dump(),
                "page": res_pages.get((r.kind, normalize_statement(r.statement)))}
               for r in merged.results]
    return defs, results, proof_rows


def flatten_concepts(section_merges: list[ExtractionResult]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for m in section_merges:
        for c in m.concepts:
            if c.name.lower() not in seen:
                seen.add(c.name.lower())
                out.append(c.model_dump())
    return out


def chapter_payload(book_id: str, chapter: dict, section_outputs: list[dict],
                    concepts: list[dict]) -> dict:
    return {"book_id": book_id, "chapter_id": chapter["id"],
            "concepts": concepts, "sections": section_outputs}
