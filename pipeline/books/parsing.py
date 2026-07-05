"""Book PDF → per-page text + outline (TOC). pypdfium2, same engine as paper parsing,
but pages are kept separate (page provenance) and bookmarks are read for structure."""
from __future__ import annotations

from dataclasses import dataclass, field

from pipeline.ingest.parsing import needs_ocr


@dataclass
class TocEntry:
    level: int        # 0 = chapter-level bookmark
    title: str
    page_index: int   # 0-based


@dataclass
class BookParse:
    pages: list[str]
    toc: list[TocEntry] = field(default_factory=list)
    mode: str = "text"

    @property
    def is_empty(self) -> bool:
        return sum(len(p.strip()) for p in self.pages) < 10


def parse_book_pdf(path: str) -> BookParse:
    import pypdfium2 as pdfium

    pdf = pdfium.PdfDocument(path)
    try:
        pages = []
        for i in range(len(pdf)):
            page = pdf[i]
            textpage = page.get_textpage()
            pages.append(textpage.get_text_range())
            textpage.close()
            page.close()
        toc = []
        for bm in pdf.get_toc():
            dest = bm.get_dest()
            if dest is None:
                continue  # bookmark without a destination — cannot be placed, skip
            toc.append(TocEntry(level=bm.level, title=bm.get_title().strip(),
                                page_index=dest.get_index()))
    finally:
        pdf.close()

    total = sum(len(p) for p in pages)
    mode = "vlm" if needs_ocr(extractable_chars=total, page_count=max(len(pages), 1)) else "text"
    return BookParse(pages=pages, toc=toc, mode=mode)
