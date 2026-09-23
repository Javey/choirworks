from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.models.domain import AgentRecord
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.patch import PatchResult, PlanPatch
from choirworks.orchestration.state import (
    NodeState,
    NodeStatus,
    OrchestrationState,
    add_member,
    pending_interventions,
)
from choirworks.tools import (
    AskUserArgs,
    CallSubagentArgs,
    JoinMembersArgs,
    RevisePlanArgs,
    ask_user_func,
    call_subagent_func,
    create_plan_func,
    join_members_func,
    revise_plan_func,
)
from choirworks.tools.call_subagent import CallSubagentData
from choirworks.tools.capabilities import ToolEffects
from choirworks.tools.create_plan import CreatePlanData
from choirworks.tools.join_members import JoinMembersData
from choirworks.tools.revise_plan import RevisePlanData
from tests.support.fakes import FakeRegistry


def agent(name: str, url: str) -> AgentRecord:
    return AgentRecord(
        id=name,
        name=name,
        card_url=url,
        card={},
        created_at=datetime.now(UTC),
    )


AGENTS = [agent("research", "http://research"), agent("writer", "http://writer")]


class RecordingEffects:
    def __init__(self, *, max_derived_nodes: int = 5):
        self.max_derived_nodes = max_derived_nodes
        self.joined: list[tuple[list[str], str]] = []
        self.persist_count = 0
        self.patches: list[PlanPatch] = []
        self.patch_result = PatchResult()

    async def join_members(self, names: list[str], reason: str) -> None:
        self.joined.append((names, reason))

    async def persist(self) -> None:
        self.persist_count += 1

    async def apply_patch_locked(self, patch: PlanPatch) -> PatchResult:
        self.patches.append(patch)
        return self.patch_result

    def as_tool_effects(self) -> ToolEffects:
        return ToolEffects(
            max_derived_nodes=self.max_derived_nodes,
            join_members=self.join_members,
            persist=self.persist,
            apply_patch_locked=self.apply_patch_locked,
        )


def make_ctx(
    state: OrchestrationState,
    effects: RecordingEffects | None = None,
) -> OrchestrationContext:
    effects = effects or RecordingEffects()
    runtime = SimpleNamespace(
        state=state,
        task_id="t1",
        context_id="c1",
        queue=None,
        lock=None,
        runner_start_requested=False,
    )
    return OrchestrationContext(
        runtime=runtime,  # type: ignore[arg-type]
        registry=FakeRegistry(AGENTS),  # type: ignore[arg-type]
        remote=SimpleNamespace(),  # type: ignore[arg-type]
        llm=SimpleNamespace(),  # type: ignore[arg-type]
        sessions=SimpleNamespace(),  # type: ignore[arg-type]
        config=SimpleNamespace(),  # type: ignore[arg-type]
        brief_builder=SimpleNamespace(),  # type: ignore[arg-type]
        effects=effects.as_tool_effects(),
    )


def make_node(state: OrchestrationState, node_id: str, **kwargs) -> NodeState:
    kwargs.setdefault("name", node_id)
    kwargs.setdefault("agent_name", "research")
    kwargs.setdefault("agent_url", "http://research")
    node = NodeState(id=node_id, **kwargs)
    state.nodes[node_id] = node
    return node


async def test_create_plan_builds_nodes_and_joins_members():
    state = OrchestrationState(plan_id="p1", plan_version=1)
    effects = RecordingEffects()
    draft = PlanDraft(
        nodes=[
            PlanNodeDraft(
                id="n1",
                name="调研",
                agent_name="research",
                input={"text": "research it"},
            ),
            PlanNodeDraft(id="n2", name="写作", agent_name="writer", deps=["n1"]),
        ]
    )

    result = await create_plan_func.execute(make_ctx(state, effects), draft)

    assert result.success is True
    assert set(state.nodes) == {"n1", "n2"}
    assert state.nodes["n1"].agent_url == "http://research"
    assert state.nodes["n1"].input_text == "research it"
    assert state.nodes["n2"].deps == ["n1"]
    assert effects.joined == [(["research", "writer"], "plan")]
    assert isinstance(result.data, CreatePlanData)
    assert [node.id for node in result.data.nodes] == ["n1", "n2"]


async def test_join_members_adds_registered_agents_to_room():
    state = OrchestrationState()

    result = await join_members_func.execute(
        make_ctx(state),
        JoinMembersArgs(names=["writer", "writer", "ghost"], reason="human_mention"),
    )

    assert result.success is True
    assert isinstance(result.data, JoinMembersData)
    assert result.data.joined == ["writer"]
    assert state.members["writer"].url == "http://writer"
    assert state.members["writer"].reason == "human_mention"
    assert "ghost" not in state.members


async def test_join_members_skips_existing_member():
    state = OrchestrationState()
    add_member(state, "writer", "http://writer", "plan")

    result = await join_members_func.execute(
        make_ctx(state), JoinMembersArgs(names=["writer"], reason="plan_revision")
    )

    assert result.success is True
    assert isinstance(result.data, JoinMembersData)
    assert result.data.joined == []
    assert state.members["writer"].reason == "plan"


async def test_ask_user_marks_node_input_required():
    state = OrchestrationState()
    make_node(state, "n1", status=NodeStatus.WORKING, a2a_task_id="remote-1")

    result = await ask_user_func.execute(
        make_ctx(state), AskUserArgs(node_id="n1", question="请问？")
    )

    assert result.success is True
    node = state.nodes["n1"]
    assert node.status == NodeStatus.INPUT_REQUIRED
    assert node.question == "请问？"
    # The remote task stays open awaiting input, so a human answer can
    # resume it instead of spawning a brand-new remote task.
    assert node.a2a_task_id == "remote-1"
    pending = pending_interventions(state)
    assert [iv.question for iv in pending] == ["请问？"]


async def test_ask_user_rejects_unknown_node():
    result = await ask_user_func.execute(
        make_ctx(OrchestrationState()), AskUserArgs(node_id="ghost", question="?")
    )

    assert result.success is False
    assert result.error == "unknown node: ghost"


async def test_call_subagent_spawns_derived_helper():
    state = OrchestrationState()
    make_node(state, "n1", status=NodeStatus.INPUT_REQUIRED, question="需要数据")
    effects = RecordingEffects()

    result = await call_subagent_func.execute(
        make_ctx(state, effects),
        CallSubagentArgs(requested_by="n1", target_agent="writer", instruction="帮忙写"),
    )

    assert result.success is True
    assert isinstance(result.data, CallSubagentData)
    helper = state.nodes[result.data.helper_node_id]
    assert helper.derived is True
    assert helper.assist_requested_by == "n1"
    assert helper.agent_name == "writer"
    assert helper.input_text == "帮忙写"
    assert effects.joined == [(["writer"], "peer_assist")]
    assert effects.persist_count == 1


async def test_call_subagent_falls_back_to_requester_question():
    state = OrchestrationState()
    make_node(state, "n1", status=NodeStatus.INPUT_REQUIRED, question="缺少接口文档")

    result = await call_subagent_func.execute(
        make_ctx(state), CallSubagentArgs(requested_by="n1", target_agent="writer")
    )

    assert result.success is True
    assert isinstance(result.data, CallSubagentData)
    helper = state.nodes[result.data.helper_node_id]
    assert "缺少接口文档" in helper.input_text


async def test_call_subagent_rejects_orchestrator_and_unknown_agent():
    state = OrchestrationState()
    ctx = make_ctx(state)

    orchestrator = await call_subagent_func.execute(
        ctx, CallSubagentArgs(requested_by="orchestrator", target_agent="writer")
    )
    unknown = await call_subagent_func.execute(
        ctx, CallSubagentArgs(requested_by="n1", target_agent="ghost")
    )

    assert orchestrator.success is False
    assert orchestrator.error == "orchestrator dispatch does not create helper nodes"
    assert unknown.success is False
    assert unknown.error == "unknown agent: ghost"


async def test_call_subagent_enforces_derived_limit():
    state = OrchestrationState(derived_count=5)
    effects = RecordingEffects(max_derived_nodes=5)

    result = await call_subagent_func.execute(
        make_ctx(state, effects),
        CallSubagentArgs(requested_by="n1", target_agent="writer"),
    )

    assert result.success is False
    assert result.error == "max derived nodes reached"


async def test_revise_plan_reports_patch_effect():
    state = OrchestrationState()
    make_node(state, "x1", name="补充")
    effects = RecordingEffects()
    effects.patch_result = PatchResult(added=["x1"], invalidated=["n9"], skipped_in_flight=["n2"])
    patch = PlanPatch(reason="需要补充")

    result = await revise_plan_func.execute(make_ctx(state, effects), RevisePlanArgs(patch=patch))

    assert result.success is True
    assert effects.patches == [patch]
    assert isinstance(result.data, RevisePlanData)
    assert result.data.added == ["x1"]
    assert result.data.invalidated == ["n9"]
    assert result.data.skipped_in_flight == ["n2"]
    assert result.data.added_nodes[0].id == "x1"
    assert result.data.added_nodes[0].name == "补充"
