from pipeline.chunking import split_markdown, _segments

def test_segments_keep_display_math_intact():
    md = "para one\n\n$$\na = b\n+ c\n$$\n\npara two"
    segs = _segments(md)
    assert "$$\na = b\n+ c\n$$" in segs

def test_split_never_breaks_a_math_block():
    md = "x" * 100 + "\n\n$$" + ("y" * 50) + "$$\n\n" + "z" * 100
    chunks = split_markdown(md, target=120, overlap=20)
    for c in chunks:
        # a chunk that contains an opening $$ must also contain its closing $$
        assert c.count("$$") % 2 == 0

def test_split_respects_target_size_roughly():
    md = "\n\n".join(["para %d %s" % (i, "w" * 40) for i in range(20)])
    chunks = split_markdown(md, target=200, overlap=20)
    assert len(chunks) > 1
    assert all(len(c) <= 400 for c in chunks)  # never wildly over target
