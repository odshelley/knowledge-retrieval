from pipeline.definitions import defs


def test_book_jobs_registered():
    assert defs.get_job_def("ingest_book") is not None
    assert defs.get_job_def("extract_book_chapter") is not None


def test_book_sensors_registered():
    assert defs.get_sensor_def("books_sensor") is not None
    assert defs.get_sensor_def("book_chapters_sensor") is not None


def test_paper_job_untouched():
    job = defs.get_job_def("ingest_document")
    names = {ak.path[-1] for ak in job.asset_layer.executable_asset_keys}
    assert "graph_write" in names and not any(n.startswith("book_") for n in names)


def test_book_assets_registered():
    expected = {"book_raw_blob", "book_parsed", "book_metadata", "book_structure",
                "book_chunks", "book_structure_write", "book_chapter_extraction",
                "book_chapter_resolved", "book_chapter_graph_write"}
    have = {ak.path[-1] for ak in defs.get_asset_graph().get_all_asset_keys()}
    assert expected <= have
