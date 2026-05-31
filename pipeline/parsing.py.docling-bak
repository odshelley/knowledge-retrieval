"""Docling parsing with text/OCR mode routing. Output: markdown with LaTeX equations."""
from __future__ import annotations

from dataclasses import dataclass

# Threshold: avg chars/page below this ⇒ assume scanned/image ⇒ OCR.
MIN_CHARS_PER_PAGE = 100


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
    """Convert a PDF to markdown+LaTeX. Tries text mode; falls back to OCR/VLM mode."""
    from docling.document_converter import DocumentConverter

    conv = DocumentConverter()
    doc = conv.convert(path).document
    md = doc.export_to_markdown()
    pages = getattr(doc, "num_pages", lambda: 1)() if callable(getattr(doc, "num_pages", None)) else 1
    if needs_ocr(extractable_chars=len(md), page_count=max(pages, 1)):
        # SCANNED/IMAGE PATH — must emit LaTeX, so use the Granite-Docling VLM pipeline,
        # NOT plain `do_ocr=True` (that runs Tesseract/EasyOCR → prose, no LaTeX; see Gate A4).
        from docling.datamodel.base_models import InputFormat
        from docling.document_converter import PdfFormatOption
        from docling.pipeline.vlm_pipeline import VlmPipeline

        vlm_conv = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_cls=VlmPipeline)}
        )
        md = vlm_conv.convert(path).document.export_to_markdown()
        return ParseResult(markdown=md, mode="vlm")
    return ParseResult(markdown=md, mode="text")
