"""Generate the deterministic fixture book PDF (5 pages, 2 chapters, outline bookmarks).

Used by unit tests (via the book_pdf conftest fixture) and by the integration suite
(scripts drop the output into BOOKS_SOURCE_DIR). reportlab is a dev-only dependency.
"""
from __future__ import annotations

from pathlib import Path

FILLER = "This line pads the page so the parser does not classify it as scanned. " * 2

PAGES: list[tuple[list[tuple[str, str, int]], list[str]]] = [
    # (bookmarks on this page: [(title, key, level)], lines)
    ([], [
        "Stochastic Processes: A Tiny Book",
        "First Edition",
        "Osian Fixture",
        "ISBN 978-3-16-148410-0",
        "Tiny Press, 2026",
        FILLER, FILLER,
    ]),
    ([("Chapter 1 Levy Processes", "ch1", 0), ("1.1 Definitions", "s11", 1)], [
        "Chapter 1 Levy Processes",
        "1.1 Definitions",
        "Definition 1.1 (Levy process). A Levy process is a stochastic process",
        "with stationary and independent increments and cadlag paths, started at zero.",
        FILLER, FILLER,
    ]),
    ([("1.2 First Results", "s12", 1)], [
        "1.2 First Results",
        "Theorem 1.2. Every Levy process has an infinitely divisible marginal",
        "distribution at each fixed time. The proof uses Definition 1.1.",
        FILLER, FILLER,
    ]),
    ([("Chapter 2 Poisson Processes", "ch2", 0), ("2.1 Counting Processes", "s21", 1)], [
        "Chapter 2 Poisson Processes",
        "2.1 Counting Processes",
        "Definition 2.1 (Poisson process). A Poisson process is a Levy process",
        "whose increments follow a Poisson distribution.",
        FILLER, FILLER,
    ]),
    ([("2.2 Compound Sums", "s22", 1)], [
        "2.2 Compound Sums",
        "Theorem 2.2. A compound Poisson process is a Levy process.",
        "This depends on Theorem 1.2 and Definition 2.1.",
        FILLER, FILLER,
    ]),
]


def make_book_pdf(path: Path) -> Path:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    c = canvas.Canvas(str(path), pagesize=A4)
    for bookmarks, lines in PAGES:
        for title, key, level in bookmarks:
            c.bookmarkPage(key)
            c.addOutlineEntry(title, key, level=level)
        text = c.beginText(72, 780)
        for line in lines:
            text.textLine(line)
        c.drawText(text)
        c.showPage()
    c.save()
    return path


if __name__ == "__main__":
    import sys
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("tiny-book.pdf")
    print(make_book_pdf(out))
