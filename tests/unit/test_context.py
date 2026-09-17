from datetime import UTC, datetime
from types import SimpleNamespace

from a2a.helpers import new_text_message
from a2a.types.a2a_pb2 import Role
from google.protobuf.json_format import ParseDict

from choirworks.a2a.room import A2A_ROOM_URI
from choirworks.a2a.state import NodeState
from choirworks.core.context import (
    ContextBriefBuilder,
    build_assist_input,
    build_followup_input,
    build_peer_choice_user,
    build_peer_fallback_input,
    build_planner_capabilities,
    build_planner_user_message,
    build_replan_context,
    build_replan_reason,
)
from choirworks.core.fencing import (
    QUOTED_CONTENT_BEGIN,
    QUOTED_CONTENT_END,
    QUOTED_CONTENT_PREAMBLE,
)
from choirworks.models.domain import AgentRecord
from tests.support.fakes import FakeLLM


def make_agent(name: str, description: str = "", skills: list[str] | None = None):
    return AgentRecord(
        id=name,
        name=name,
        card_url=f"http://{name}",
        card={
            "name": name,
            "description": description or f"{name} agent",
            "skills": [
                {"id": s, "name": s, "description": s} for s in (skills or [])
            ],
        },
        created_at=datetime.now(UTC),
    )


def node(node_id: str, **kwargs) -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name=kwargs.pop("agent_name", node_id),
        agent_url=f"http://{node_id}",
        **kwargs,
    )


def room_msg(text: str, sender: str | None = None, role: int = Role.ROLE_USER):
    msg = new_text_message(text, role=role)
    if sender:
        ParseDict({A2A_ROOM_URI: {"sender": sender}}, msg.metadata)
    return msg


# ------------------------------------------------------------- builders


def test_build_planner_capabilities_caps_description():
    agents = [make_agent("a", description="x" * 1100, skills=["s1"])]
    text = build_planner_capabilities(agents)
    assert text.startswith("- a: ")
    assert "... [truncated]" in text
    assert "s1 (s1)" in text


def test_build_planner_user_message_fences_untrusted_blocks():
    user = build_planner_user_message(
        "帮我写报告",
        "- a: agent",
        reason="nodes failed: boom",
        context="- @a: done",
    )
    assert "User request:\n帮我写报告" in user
    assert QUOTED_CONTENT_PREAMBLE in user
    assert user.count(QUOTED_CONTENT_BEGIN) == 4
    assert user.count(QUOTED_CONTENT_END) == 4
    assert "Available agents:\n- a: agent" in user
    assert "Reason for replanning:\nnodes failed: boom" in user
    assert "Completed work so far:\n- @a: done" in user


def test_build_planner_user_message_without_reason_and_context():
    user = build_planner_user_message("hi", "- a: agent")
    assert user.count(QUOTED_CONTENT_BEGIN) == 2
    assert "Reason for replanning" not in user


def test_build_peer_choice_user():
    candidates = [make_agent("a"), make_agent("b")]
    user = build_peer_choice_user("c", "需要确认", candidates)
    assert "Requester: c" in user
    assert "Question / blocked work:\n需要确认" in user
    assert QUOTED_CONTENT_PREAMBLE in user
    assert "- a: a agent" in user
    assert "- b: b agent" in user


def test_build_replan_reason_and_context():
    nodes = [
        node("n1", status="completed", output="ok" * 150),
        node("n2", status="failed", error="boom"),
        node("n3", status="failed"),
    ]
    assert build_replan_reason(nodes) == "nodes failed: boom, n3"
    context = build_replan_context(nodes)
    assert context == "- @n1: " + "ok" * 150


def test_build_followup_input():
    assert build_followup_input("a", None, "新要求") == "新要求"
    fenced = build_followup_input("a", "产出", "新要求")
    assert "引用 @a 此前产出：" in fenced
    assert QUOTED_CONTENT_BEGIN in fenced and QUOTED_CONTENT_END in fenced
    assert "新要求：新要求" in fenced


def test_build_assist_input_and_peer_fallback():
    assist = build_assist_input("a", "产出")
    assert "@a 在协作中请求你的协助。" in assist
    assert QUOTED_CONTENT_BEGIN in assist
    assert build_peer_fallback_input("问题") == "请协助回答以下问题：\n问题"


# --------------------------------------------------- ContextBriefBuilder


class FakeTaskStore:
    def __init__(self, tasks):
        self._tasks = tasks

    async def list(self, params, ctx):
        return SimpleNamespace(tasks=self._tasks)


def brief_builder(llm, tasks, **kwargs) -> ContextBriefBuilder:
    builder = ContextBriefBuilder(llm, **kwargs)
    builder.set_task_store(FakeTaskStore(tasks))
    return builder


async def test_brief_without_task_store_returns_empty():
    builder = ContextBriefBuilder(FakeLLM())
    assert await builder.build("ctx-1", "t1") == ""


async def test_brief_small_timeline_returns_full_text():
    tasks = [
        SimpleNamespace(
            id="t1",
            history=[room_msg("你好", role=Role.ROLE_USER), room_msg("回复", "researcher")],
        ),
        SimpleNamespace(id="t2", history=[room_msg("别的会话")]),
    ]
    builder = brief_builder(FakeLLM(), tasks)
    brief = await builder.build("ctx-1", "t0")
    assert brief == "[user] 你好\n[researcher] 回复\n[user] 别的会话"


async def test_brief_skips_empty_messages_and_excluded_task():
    empty = new_text_message("x")
    empty.ClearField("parts")
    tasks = [
        SimpleNamespace(id="t1", history=[room_msg("当前")]),
        SimpleNamespace(id="t2", history=[empty]),
    ]
    builder = brief_builder(FakeLLM(), tasks)
    assert await builder.build("ctx-1", "t1") == ""


async def test_brief_compacts_when_over_threshold():
    llm = FakeLLM(text_results=["早期摘要"])
    tasks = [
        SimpleNamespace(
            id="t2",
            history=[
                room_msg(f"旧消息{i}" + "内容" * 5, f"agent{i}") for i in range(5)
            ],
        )
    ]
    builder = brief_builder(
        llm, tasks, compaction_threshold=0.0001, compaction_retention=2
    )
    brief = await builder.build("ctx-1", "t1")
    assert brief.startswith("## 群聊历史摘要\n早期摘要\n\n## 最近消息\n")
    assert "旧消息4" in brief
    assert "旧消息0" not in brief
    assert len(llm.text_calls) == 1


async def test_brief_cache_hit_on_same_split():
    llm = FakeLLM(text_results=["早期摘要"])
    tasks = [
        SimpleNamespace(
            id="t2",
            history=[
                room_msg(f"旧消息{i}" + "内容" * 5, f"agent{i}") for i in range(5)
            ],
        )
    ]
    builder = brief_builder(
        llm, tasks, compaction_threshold=0.0001, compaction_retention=2
    )
    await builder.build("ctx-1", "t1")
    await builder.build("ctx-1", "t1")
    assert len(llm.text_calls) == 1


async def test_brief_updates_summary_incrementally():
    llm = FakeLLM(text_results=["早期摘要", "更新摘要"])
    history = [room_msg(f"旧消息{i}" + "内容" * 5, f"agent{i}") for i in range(5)]
    builder = brief_builder(
        llm,
        [SimpleNamespace(id="t2", history=history)],
        compaction_threshold=0.0001,
        compaction_retention=2,
    )
    await builder.build("ctx-1", "t1")
    history.append(room_msg("新消息A" + "内容" * 5, "a6"))
    history.append(room_msg("新消息B" + "内容" * 5, "a7"))
    brief = await builder.build("ctx-1", "t1")
    assert len(llm.text_calls) == 2
    assert "Previous summary:\n早期摘要" in llm.text_calls[1]["user"]
    assert "旧消息3" in llm.text_calls[1]["user"]
    assert "旧消息4" in llm.text_calls[1]["user"]
    assert "新消息A" in brief and "新消息B" in brief
    assert "更新摘要" in brief
