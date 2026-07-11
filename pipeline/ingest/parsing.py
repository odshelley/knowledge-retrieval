"""PDF → markdown parsing.

NOTE: switched from Docling to pypdfium2 for speed. Docling's ML pipeline
(layout + OCR + VLM) is impractically slow on CPU, and Docker on Apple Silicon
cannot access the Mac GPU (no Metal/MPS passthrough), so every parse was CPU-only
and hung on first-run HuggingFace model downloads. pypdfium2 extracts the text
layer in well under a second with no models. Tradeoff: it does NOT reconstruct
LaTeX from rendered equations (no fast parser can) — equation glyphs come through
approximately. The original Docling implementation is preserved in
parsing.py.docling-bak for when a GPU host / VLM path is available.
"""
from __future__ import annotations

from dataclasses import dataclass

# Threshold: avg chars/page below this ⇒ assume scanned/image ⇒ would need OCR.
MIN_CHARS_PER_PAGE = 100


def sanitize_text(s: str) -> str:
    """PDF text layers occasionally contain NUL bytes; Postgres text params reject them,
    so they must never enter parsed artifacts (they'd resurface in extracted concept names)."""
    return s.replace("\x00", "")


def needs_ocr(extractable_chars: int, page_count: int) -> bool:
    if page_count <= 0:
        return True
    return (extractable_chars / page_count) < MIN_CHARS_PER_PAGE


@dataclass
class ParseResult:
    markdown: str
    mode: str  # "text" | "vlm"

    @property
    def is_empty(self) -> bool:
        return len(self.markdown.strip()) < 10


def parse_pdf(path: str) -> ParseResult:
    """Convert a digital PDF to text/markdown using pypdfium2 (fast, no ML)."""
    import pypdfium2 as pdfium

    pdf = pdfium.PdfDocument(path)
    try:
        pages = len(pdf)
        parts = []
        for i in range(pages):
            page = pdf[i]
            textpage = page.get_textpage()
            parts.append(textpage.get_text_range())
            textpage.close()
            page.close()
    finally:
        pdf.close()

    md = sanitize_text("\n\n".join(parts)).strip()
    if needs_ocr(extractable_chars=len(md), page_count=max(pages, 1)):
        # Scanned/image PDF — no text layer. A real OCR/VLM path is needed here;
        # pypdfium2 alone can't help. Surface as empty so the asset can flag it.
        return ParseResult(markdown=md, mode="vlm")
    return ParseResult(markdown=md, mode="text")
