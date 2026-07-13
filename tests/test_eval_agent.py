"""Loop mechanics for the eval agent mode — no network, no judges. A scripted fake OpenAI
client plays the answer model; a fake graph plays Neo4j."""
import json
from types import SimpleNamespace

from scripts.eval_agent import MAX_TOOL_CALLS, answer_with_tools, execute_tool


class FakeGraph:
    def __init__(self):
        self.cypher_seen = []

    def read_limited(self, cypher, **kw):
        self.cypher_seen.append(cypher)
        return [{"answer": 42}], False


def _tool_call(call_id, name, args):
    return SimpleNamespace(
        id=call_id,
        function=SimpleNamespace(name=name, arguments=json.dumps(args)),
        model_dump=lambda: {"id": call_id, "type": "function",
                            "function": {"name": name, "arguments": json.dumps(args)}},
    )


class FakeOAI:
    """Yields scripted responses; records the tool_choice of each request."""

    def __init__(self, script):
        self.script = list(script)
        self.tool_choices = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kw):
        self.tool_choices.append(kw.get("tool_choice"))
        msg = self.script.pop(0)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)])


def _msg(content=None, tool_calls=None):
    return SimpleNamespace(content=content, tool_calls=tool_calls)


def test_tool_dispatch_and_trace():
    graph = FakeGraph()
    oai = FakeOAI([
        _msg(tool_calls=[_tool_call("c1", "run_cypher",
                                    {"query": "MATCH (p:Paper) RETURN count(p) AS answer"})]),
        _msg(content="There are 42 papers."),
    ])
    answer, trace, evidence = answer_with_tools(oai, "m", graph, "How many papers?")
    assert answer == "There are 42 papers."
    assert [t["tool"] for t in trace] == ["run_cypher"]
    assert trace[0]["error"] is None
    assert "42" in evidence
    assert graph.cypher_seen == ["MATCH (p:Paper) RETURN count(p) AS answer"]


def test_guard_violation_surfaces_as_tool_error_not_crash():
    graph = FakeGraph()
    oai = FakeOAI([
        _msg(tool_calls=[_tool_call("c1", "run_cypher", {"query": "CREATE (n) RETURN n"})]),
        _msg(content="I cannot modify the graph."),
    ])
    answer, trace, evidence = answer_with_tools(oai, "m", graph, "Add a node")
    assert answer == "I cannot modify the graph."
    assert trace[0]["error"] is not None and "error:" in trace[0]["error"]
    assert graph.cypher_seen == []  # guard rejected before the driver


def test_cap_forces_final_answer():
    graph = FakeGraph()
    def one_call(i):
        return _msg(tool_calls=[_tool_call(f"c{i}", "run_cypher",
                                           {"query": "MATCH (n) RETURN n"})])

    oai = FakeOAI([one_call(i) for i in range(MAX_TOOL_CALLS)] + [_msg(content="done")])
    answer, trace, _ = answer_with_tools(oai, "m", graph, "loop forever")
    assert answer == "done"
    assert len(trace) == MAX_TOOL_CALLS
    # the final request must forbid further tool calls
    assert oai.tool_choices[-1] == "none"
    assert all(tc == "auto" for tc in oai.tool_choices[:-1])


def test_unknown_tool_reported():
    out = execute_tool(FakeGraph(), "made_up_tool", {})
    assert out.startswith("error:") or "unknown" in out


def test_get_schema_needs_no_graph_call():
    out = execute_tool(FakeGraph(), "get_schema", {})
    assert isinstance(out, str) and len(out) > 50


def test_unknown_label_rejected_before_driver():
    graph = FakeGraph()
    out = execute_tool(graph, "run_cypher", {"query": "MATCH (t:Theorem) RETURN count(t)"})
    assert out.startswith("error:") and "Theorem" in out
    assert graph.cypher_seen == []  # rejected before execution


def test_known_labels_pass_validation():
    from scripts.eval_agent import validate_cypher_labels
    assert validate_cypher_labels(
        "MATCH (p:Paper)-[:CITES]->(q:Paper) RETURN count(q)") is None
    # label-looking tokens inside string literals are ignored
    assert validate_cypher_labels(
        "MATCH (c:Concept) WHERE c.name = 'foo:Bar' RETURN c") is None


def test_empty_rows_get_retry_hint():
    class EmptyGraph(FakeGraph):
        def read_limited(self, cypher, **kw):
            self.cypher_seen.append(cypher)
            return [], False

    out = execute_tool(EmptyGraph(), "run_cypher", {"query": "MATCH (p:Paper) RETURN p"})
    payload = json.loads(out)
    assert payload["rows"] == [] and "hint" in payload


def test_anthropic_loop_dispatch_and_cap():
    from scripts.eval_agent import answer_with_tools_anthropic

    class Block(SimpleNamespace):
        pass

    def tool_use(call_id):
        return Block(type="tool_use", id=call_id, name="run_cypher",
                     input={"query": "MATCH (p:Paper) RETURN count(p) AS answer"})

    class FakeAnthropic:
        def __init__(self, script):
            self.script = list(script)
            self.tool_choices = []
            self.messages = SimpleNamespace(create=self._create)

        def _create(self, **kw):
            self.tool_choices.append(kw.get("tool_choice"))
            return self.script.pop(0)

    graph = FakeGraph()
    client = FakeAnthropic([
        SimpleNamespace(stop_reason="tool_use", content=[tool_use("t1")]),
        SimpleNamespace(stop_reason="end_turn",
                        content=[Block(type="text", text="42 papers.")]),
    ])
    answer, trace, evidence = answer_with_tools_anthropic(client, "claude-x", graph, "How many?")
    assert answer == "42 papers."
    assert [t["tool"] for t in trace] == ["run_cypher"]
    assert "42" in evidence
    assert client.tool_choices == [{"type": "auto"}, {"type": "auto"}]
