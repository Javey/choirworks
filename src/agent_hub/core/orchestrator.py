from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.config import PolicyConfig
from agent_hub.core.conversations import build_conversation_context
from agent_hub.core.dispatcher import InvalidNodeState, NodeDispatcher
from agent_hub.core.llm import LLMClient
from agent_hub.core.planner import Planner, PlanNodeDraft
from agent_hub.core.policy import PolicyEngine
from agent_hub.core.room import post_agent_messages, post_message
from agent_hub.core.tasks import TaskService
from agent_hub.models.domain import Node, OrchestrationTask, RoomMessage
from agent_hub.models.enums import (
    TERMINAL_TASK_STATUSES,
    EventType,
    InterventionStatus,
    NodeStatus,
    TaskStatus,
)
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore

ASSIST_SYSTEM = (
    "You answer clarifying questions asked by worker agents on behalf of the user. "
    "Use the original request and available context. Reply with a short, direct answer."
)
PEER_SYSTEM = (
    "Choose the best registered agent to answer a worker agent's question. "
    "Return the agent name and a precise instruction for that agent."
)


class PeerChoice(BaseModel):
    agent_name: str
    instruction: str


def _join_text(parts: Any) -> str:
    return "\n".join(part.text for part in parts if part.HasField("text"))


class Orchestrator:
    def __init__(
        self,
        db: Database,
        events: EventStore,
        planner: Planner,
        dispatcher: NodeDispatcher,
        task_service: TaskService,
        *,
        registry: AgentRegistry | None = None,
        remote: RemoteAgentClient | None = None,
        llm: LLMClient | None = None,
        policy_engine: PolicyEngine | None = None,
        max_parallel: int = 4,
        max_node_attempts: int = 2,
        retry_backoff_seconds: float = 1.0,
        replan_on_failure: bool = True,
    ):
        self._coordinator: Any | None = None
        self._db = db
        self._events = events
        self._planner = planner
        self._dispatcher = dispatcher
        self._task_service = task_service
        self._registry = registry
        self._remote = remote
        self._llm = llm
        self._policy = policy_engine or PolicyEngine(PolicyConfig())
        self._max_parallel = max_parallel
        self._max_node_attempts = max_node_attempts
        self._retry_backoff = retry_backoff_seconds
        self._replan_on_failure = replan_on_failure
        self._runs: dict[str, asyncio.Task] = {}
        self._restart_requested: set[str] = set()
        self._checkpoint_counts: dict[str, int] = {}
        self._inflight: set[asyncio.Task] = set()
        self._continuing: dict[str, asyncio.Task] = {}
        self._timeout_tasks: dict[str, asyncio.Task] = {}

    def set_coordinator(self, coordinator: Any) -> None:
        self._coordinator = coordinator

    def start(self, task_id: str) -> None:
        existing = self._runs.get(task_id)
        if existing is not None and not existing.done():
            self._restart_requested.add(task_id)
            return
        self._launch(task_id)

    def _launch(self, task_id: str) -> None:
        run = asyncio.create_task(self.run(task_id), name=f"orchestrator:{task_id}")
        self._runs[task_id] = run

        def _on_done(_: asyncio.Task) -> None:
            self._runs.pop(task_id, None)
            if task_id in self._restart_requested:
                self._restart_requested.discard(task_id)
                self._launch(task_id)

        run.add_done_callback(_on_done)

    async def stop(self) -> None:
        pending = (
            list(self._runs.values())
            + list(self._inflight)
            + list(self._continuing.values())
            + list(self._timeout_tasks.values())
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._runs.clear()
        self._inflight.clear()
        self._continuing.clear()
        self._timeout_tasks.clear()

    async def stop_task(self, task_id: str) -> None:
        run = self._runs.pop(task_id, None)
        self._restart_requested.discard(task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        if plan is not None:
            for node in await projections.fetch_nodes(self._db, task_id, plan.id):
                continuing = self._continuing.pop(node.id, None)
                if continuing is not None:
                    continuing.cancel()
        for intervention in await projections.fetch_interventions(self._db, task_id):
            timer = self._timeout_tasks.pop(intervention.id, None)
            if timer is not None:
                timer.cancel()
        if run is not None:
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)

    async def wait(
        self,
        task_id: str,
        timeout_seconds: float = 30.0,
        *,
        until_terminal: bool = False,
    ) -> None:
        async def _poll() -> None:
            while True:
                task = await projections.fetch_task(self._db, task_id)
                if task is not None and (
                    task.status in TERMINAL_TASK_STATUSES
                    or (not until_terminal and task.status is TaskStatus.AWAITING_INPUT)
                ):
                    return
                await asyncio.sleep(0.02)

        async with asyncio.timeout(timeout_seconds):
            await _poll()

    async def run(self, task_id: str) -> None:
        task = await projections.fetch_task(self._db, task_id)
        if task is None or task.status in TERMINAL_TASK_STATUSES:
            return
        if task.plan_version is None:
            await self._initial_plan(task)

        while True:
            task = await projections.fetch_task(self._db, task_id)
            if task is None or task.status in TERMINAL_TASK_STATUSES:
                return
            plan = await projections.fetch_current_plan(self._db, task_id)
            if plan is None:
                return
            nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
            self._inflight = {item for item in self._inflight if not item.done()}
            new_messages = await post_agent_messages(
                self._db, self._events, task, nodes
            )
            arbitrated = 0
            if self._coordinator is not None:
                for message in new_messages:
                    arbitrated += await self._coordinator.arbitrate_message(
                        task, message
                    )
            if arbitrated:
                continue
            if self._coordinator is not None:
                await self._coordinator.deliver_queued_for_terminal(task, nodes)

            completed_count = sum(
                1 for node in nodes if node.status is NodeStatus.COMPLETED
            )
            if completed_count and completed_count > self._checkpoint_counts.get(
                task_id, 0
            ):
                await self._task_service.create_checkpoint(task_id)
                self._checkpoint_counts[task_id] = completed_count

            failed = [node for node in nodes if node.status is NodeStatus.FAILED]
            if failed:
                await self._handle_failure(task, failed[0])
                continue

            parked = [node for node in nodes if node.status is NodeStatus.INPUT_REQUIRED]
            if parked:
                progressed = await self._process_interventions(task, parked)
                if progressed:
                    continue
                if not await self._has_pending_assist(parked):
                    if task.status is not TaskStatus.AWAITING_INPUT:
                        await self._events.append(
                            task_id,
                            EventType.TASK_STATE_CHANGED,
                            {
                                "from": TaskStatus.RUNNING.value,
                                "to": TaskStatus.AWAITING_INPUT.value,
                            },
                        )
                    return
                # 协助节点在途：继续走 ready/inflight 的派发与等待逻辑
            if task.status is TaskStatus.AWAITING_INPUT:
                await self._events.append(
                    task_id,
                    EventType.TASK_STATE_CHANGED,
                    {
                        "from": TaskStatus.AWAITING_INPUT.value,
                        "to": TaskStatus.RUNNING.value,
                    },
                )
                continue

            ready = [
                node
                for node in nodes
                if node.status is NodeStatus.PENDING and self._deps_completed(node, nodes)
            ] + [node for node in nodes if node.status is NodeStatus.READY]

            if ready:
                slots = max(0, self._max_parallel - len(self._inflight))
                for node in ready[:slots]:
                    if self._coordinator is not None:
                        await self._coordinator.announce_dispatch(task, node)
                        await self._coordinator.mark_delivered_for_node(node.id)
                    self._inflight.add(
                        asyncio.create_task(
                            self._dispatcher.dispatch_node(task_id, node.id)
                        )
                    )
                if self._inflight:
                    await asyncio.wait(
                        self._inflight, return_when=asyncio.FIRST_COMPLETED
                    )
                continue

            if self._inflight:
                await asyncio.wait(self._inflight, return_when=asyncio.FIRST_COMPLETED)
                continue

            active_nodes = [
                node for node in nodes if node.status is not NodeStatus.INVALIDATED
            ]
            if active_nodes and all(
                node.status is NodeStatus.COMPLETED for node in active_nodes
            ):
                if self._coordinator is not None:
                    await self._coordinator.announce_completion(task, active_nodes)
                await self._task_service.finalize_if_complete(task_id)
                return
            await self._events.append(
                task_id,
                EventType.ERROR,
                {"message": "scheduler stalled: no ready nodes and no in-flight work"},
            )
            await self._events.append(task_id, EventType.TASK_FAILED, {})
            return

    async def _initial_plan(self, task: OrchestrationTask) -> None:
        try:
            context = None
            if task.conversation_id:
                context = await build_conversation_context(
                    self._db, task.conversation_id, task.id
                )
            drafted = await self._planner.plan(task.request, context=context)
            await self._task_service.create_plan_from_draft(task.id, drafted, version=1)
            await self._task_service.mark_running(task.id)
            if self._coordinator is not None:
                await self._coordinator.announce_plan(task, drafted)
        except Exception as exc:  # noqa: BLE001 - 规划失败统一标记任务失败
            await self._events.append(task.id, EventType.ERROR, {"message": str(exc)})
            await self._events.append(task.id, EventType.TASK_FAILED, {})

    async def _handle_failure(self, task: OrchestrationTask, node: Node) -> None:
        if node.attempt < self._max_node_attempts:
            delay = self._retry_backoff * node.attempt
            await self._events.append(
                task.id,
                EventType.NODE_RETRY_SCHEDULED,
                {
                    "node_id": node.id,
                    "attempt": node.attempt + 1,
                    "delay_seconds": delay,
                },
            )
            if delay > 0:
                await asyncio.sleep(delay)
            await self._events.append(
                task.id,
                EventType.NODE_STATE_CHANGED,
                {
                    "node_id": node.id,
                    "from": node.status.value,
                    "to": NodeStatus.READY.value,
                },
            )
            return
        if self._replan_on_failure:
            await self._fail_assigned_interventions(node)
            await self._replan(task, node)
            return
        await self._fail_assigned_interventions(node)
        await self._events.append(task.id, EventType.TASK_FAILED, {})

    async def _fail_assigned_interventions(self, node: Node) -> None:
        for intervention in await projections.fetch_interventions_by_assigned_node(
            self._db, node.id
        ):
            if intervention.status is InterventionStatus.PENDING:
                await self._events.append(
                    node.task_id,
                    EventType.INTERVENTION_FAILED,
                    {"intervention_id": intervention.id},
                )

    async def _replan(self, task: OrchestrationTask, failed_node: Node) -> None:
        plan = await projections.fetch_current_plan(self._db, task.id)
        assert plan is not None
        nodes = await projections.fetch_nodes(self._db, task.id, plan.id)
        completed = [node for node in nodes if node.status is NodeStatus.COMPLETED]
        context = "\n".join(
            f"- {node.name}: {json.dumps(node.output, ensure_ascii=False)}"
            for node in completed
        )
        reason = f"node '{failed_node.name}' failed: {failed_node.error or 'unknown error'}"
        try:
            drafted = await self._planner.plan(
                task.request, reason=reason, context=context
            )
            await self._task_service.create_plan_from_draft(
                task.id, drafted, version=plan.version + 1
            )
        except Exception as exc:  # noqa: BLE001 - 重规划失败统一标记任务失败
            await self._events.append(task.id, EventType.ERROR, {"message": str(exc)})
            await self._events.append(task.id, EventType.TASK_FAILED, {})

    async def _process_interventions(
        self, task: OrchestrationTask, parked: list[Node]
    ) -> bool:
        progressed = False
        for node in parked:
            continuing = self._continuing.get(node.id)
            if continuing is not None and not continuing.done():
                await asyncio.wait({continuing}, return_when=asyncio.FIRST_COMPLETED)
                progressed = True
                continue
            if continuing is not None:
                exc = continuing.exception()
                self._continuing.pop(node.id, None)
                if exc is not None:
                    await self._events.append(
                        task.id,
                        EventType.ERROR,
                        {"message": f"continuation failed: {exc}", "node_id": node.id},
                    )
                    await self._events.append(
                        task.id,
                        EventType.NODE_STATE_CHANGED,
                        {
                            "node_id": node.id,
                            "from": node.status.value,
                            "to": NodeStatus.FAILED.value,
                        },
                    )
                    progressed = True
                    continue

            interventions = await projections.fetch_interventions_for_node(
                self._db, node.id
            )
            resolved = next(
                (
                    item
                    for item in interventions
                    if item.status is InterventionStatus.RESOLVED
                    and item.answer is not None
                ),
                None,
            )
            if resolved is not None:
                text = str((resolved.answer or {}).get("text", ""))
                if self._coordinator is not None:
                    queued = await projections.fetch_queued_messages(
                        self._db, node.id
                    )
                    if queued:
                        supplement = "；".join(message.text for message in queued)
                        text = f"{text}\n（CEO 补充：{supplement}）"
                        await self._coordinator.mark_delivered_for_node(node.id)
                run = asyncio.create_task(
                    self._dispatcher.continue_node(task.id, node.id, text)
                )
                self._continuing[node.id] = run
                self._inflight.add(run)
                progressed = True
                continue

            open_pending = next(
                (
                    item
                    for item in interventions
                    if item.status is InterventionStatus.PENDING
                ),
                None,
            )
            if open_pending is not None:
                if open_pending.assigned_node_id:
                    helper = await projections.fetch_node(
                        self._db, open_pending.assigned_node_id
                    )
                    if helper is None or helper.status is NodeStatus.INVALIDATED:
                        await self._events.append(
                            task.id,
                            EventType.INTERVENTION_FAILED,
                            {"intervention_id": open_pending.id},
                        )
                        progressed = True
                    elif helper.status is NodeStatus.COMPLETED:
                        await self._events.append(
                            task.id,
                            EventType.INTERVENTION_RESOLVED,
                            {
                                "intervention_id": open_pending.id,
                                "answer": {"text": self._artifact_text(helper.output)},
                                "responder": helper.agent_name or "peer_agent",
                            },
                        )
                        progressed = True
                continue

            await self._create_intervention(task, node)
            progressed = True
        return progressed

    async def _create_intervention(
        self, task: OrchestrationTask, node: Node
    ) -> None:
        policy = self._policy.resolve(
            node.policy_override, node.agent_name, node.skill_id, task.policy
        )
        if node.requires_approval:
            policy = "human"
        if policy == "auto_llm" and self._llm is None:
            policy = "human"
        if policy == "peer_agent" and (
            self._llm is None or self._registry is None or self._remote is None
        ):
            policy = "human"

        question = await self._question_text(task.id, node.id)
        intervention_id = uuid4().hex
        deadline_at = None
        if policy == "human":
            deadline = datetime.now(UTC) + timedelta(
                seconds=self._policy.config.timeout_seconds
            )
            deadline_at = deadline.isoformat()

        assigned_node_id: str | None = None
        assigned_to: str | None = None
        if policy == "peer_agent":
            plan = await projections.fetch_current_plan(self._db, task.id)
            if plan is None:
                return
            try:
                choice = await self._peer_choice(task, node, question)
                reusable = await self._reusable_helper(plan, node, choice.agent_name)
            except Exception as exc:  # noqa: BLE001 - 协助决策失败按节点失败处理
                await self._fail_intervention(task, node, intervention_id, exc)
                return
            if reusable is not None and reusable.status is NodeStatus.COMPLETED:
                await self._append_intervention_requested(
                    task,
                    node,
                    intervention_id,
                    policy,
                    question,
                    deadline_at,
                    reusable.id,
                    choice.agent_name,
                )
                await self._events.append(
                    task.id,
                    EventType.INTERVENTION_RESOLVED,
                    {
                        "intervention_id": intervention_id,
                        "answer": {"text": self._artifact_text(reusable.output)},
                        "responder": choice.agent_name,
                    },
                )
                return
            if reusable is not None:
                assigned_node_id = reusable.id
            else:
                helper_key = f"a{uuid4().hex[:8]}"
                assigned_node_id = f"{plan.id}:{helper_key}"
                await self._task_service.extend_plan(
                    task.id,
                    added_nodes=[
                        PlanNodeDraft(
                            id=helper_key,
                            name=f"协助 · {choice.agent_name}",
                            agent_name=choice.agent_name,
                            input={"text": choice.instruction},
                        )
                    ],
                    edges=[(helper_key, self._dag_id(plan.id, node.id))],
                    rationale=f"peer assistance for node {node.name}",
                )
            assigned_to = choice.agent_name

        await self._append_intervention_requested(
            task,
            node,
            intervention_id,
            policy,
            question,
            deadline_at,
            assigned_node_id,
            assigned_to,
        )

        if policy == "human":
            if self._coordinator is not None:
                await self._coordinator.announce_intervention(
                    task, node, intervention_id, question
                )
            self._timeout_tasks[intervention_id] = asyncio.create_task(
                self._timeout_watcher(
                    task.id, intervention_id, self._policy.config.timeout_seconds
                )
            )
            return

        if policy == "auto_llm":
            try:
                answer = await self._assist_text(task, node, question)
            except Exception as exc:  # noqa: BLE001 - 协助失败按节点失败处理
                await self._fail_intervention(task, node, intervention_id, exc)
                return
            await self._events.append(
                task.id,
                EventType.INTERVENTION_RESOLVED,
                {
                    "intervention_id": intervention_id,
                    "answer": {"text": answer},
                    "responder": "auto_llm",
                },
            )
        # peer_agent：扩展已写入，协助节点由调度器正常派发

    async def _append_intervention_requested(
        self,
        task: OrchestrationTask,
        node: Node,
        intervention_id: str,
        policy: str,
        question: str,
        deadline_at: str | None,
        assigned_node_id: str | None,
        assigned_to: str | None,
    ) -> None:
        await self._events.append(
            task.id,
            EventType.INTERVENTION_REQUESTED,
            {
                "intervention_id": intervention_id,
                "node_id": node.id,
                "source": "remote_input_required",
                "policy": policy,
                "question": {"text": question},
                "responder": None,
                "deadline_at": deadline_at,
                "assigned_node_id": assigned_node_id,
                "assigned_to": assigned_to,
            },
        )

    async def _fail_intervention(
        self,
        task: OrchestrationTask,
        node: Node,
        intervention_id: str,
        exc: Exception,
    ) -> None:
        await self._events.append(
            task.id,
            EventType.ERROR,
            {"message": f"assist failed: {exc}", "node_id": node.id},
        )
        await self._events.append(
            task.id,
            EventType.INTERVENTION_FAILED,
            {"intervention_id": intervention_id},
        )
        await self._events.append(
            task.id,
            EventType.NODE_STATE_CHANGED,
            {
                "node_id": node.id,
                "from": node.status.value,
                "to": NodeStatus.FAILED.value,
            },
        )

    @staticmethod
    def _dag_id(plan_id: str, node_id: str) -> str:
        return node_id[len(plan_id) + 1 :]

    @staticmethod
    def _artifact_text(output: dict[str, Any] | None) -> str:
        artifacts = (output or {}).get("artifacts", [])
        return "\n".join(
            artifact.get("text", "")
            for artifact in artifacts
            if isinstance(artifact, dict) and artifact.get("text")
        )

    async def _reusable_helper(
        self, plan: Any, parent: Node, agent_name: str
    ) -> Node | None:
        derived = {
            f"{plan.id}:{node['id']}"
            for node in plan.dag.get("nodes", [])
            if node.get("derived")
        }
        if not derived:
            return None
        for candidate in await projections.fetch_nodes(
            self._db, parent.task_id, plan.id
        ):
            if candidate.id not in derived:
                continue
            if candidate.agent_name != agent_name:
                continue
            if candidate.status is NodeStatus.INVALIDATED:
                continue
            if candidate.id in parent.deps:
                return candidate
        return None

    async def _has_pending_assist(self, parked: list[Node]) -> bool:
        for node in parked:
            for intervention in await projections.fetch_interventions_for_node(
                self._db, node.id
            ):
                if (
                    intervention.status is not InterventionStatus.PENDING
                    or not intervention.assigned_node_id
                ):
                    continue
                helper = await projections.fetch_node(
                    self._db, intervention.assigned_node_id
                )
                if helper is not None and helper.status is not NodeStatus.INVALIDATED:
                    return True
        return False

    async def _timeout_watcher(
        self, task_id: str, intervention_id: str, delay: float
    ) -> None:
        try:
            await asyncio.sleep(max(delay, 0))
            intervention = await projections.fetch_intervention(
                self._db, intervention_id
            )
            if (
                intervention is None
                or intervention.status is not InterventionStatus.PENDING
            ):
                return
            mode = self._policy.config.on_timeout
            if mode == "auto":
                task = await projections.fetch_task(self._db, task_id)
                node = await projections.fetch_node(
                    self._db, intervention.node_id or ""
                )
                if task is None or node is None:
                    return
                question = str((intervention.question or {}).get("text", ""))
                answer = await self._assist_text(task, node, question)
                await self._events.append(
                    task_id,
                    EventType.INTERVENTION_RESOLVED,
                    {
                        "intervention_id": intervention_id,
                        "answer": {"text": answer},
                        "responder": "auto_llm",
                    },
                )
            elif mode == "fail":
                node = await projections.fetch_node(
                    self._db, intervention.node_id or ""
                )
                if node is not None:
                    await self._events.append(
                        task_id,
                        EventType.NODE_STATE_CHANGED,
                        {
                            "node_id": node.id,
                            "from": node.status.value,
                            "to": NodeStatus.FAILED.value,
                        },
                    )
                await self._events.append(task_id, EventType.TASK_FAILED, {})
            else:  # escalate：审计提醒并重新计时
                await self._events.append(
                    task_id,
                    EventType.ERROR,
                    {
                        "message": f"intervention {intervention_id} is overdue; "
                        "still waiting for human input",
                        "node_id": intervention.node_id,
                    },
                )
                self._timeout_tasks[intervention_id] = asyncio.create_task(
                    self._timeout_watcher(task_id, intervention_id, delay)
                )
                return
            self.start(task_id)
        except asyncio.CancelledError:
            raise
        finally:
            current = self._timeout_tasks.get(intervention_id)
            if current is asyncio.current_task():
                self._timeout_tasks.pop(intervention_id, None)

    async def answer_intervention(
        self,
        intervention_id: str,
        text: str,
        responder: str = "user",
        quote_id: str | None = None,
    ) -> RoomMessage | None:
        intervention = await projections.fetch_intervention(
            self._db, intervention_id
        )
        if intervention is None:
            raise KeyError(f"intervention not found: {intervention_id}")
        if intervention.status is not InterventionStatus.PENDING:
            raise InvalidNodeState(
                f"intervention {intervention_id} is {intervention.status.value}"
            )
        await self._events.append(
            intervention.task_id,
            EventType.INTERVENTION_RESOLVED,
            {
                "intervention_id": intervention_id,
                "answer": {"text": text},
                "responder": responder,
            },
        )
        task = await projections.fetch_task(self._db, intervention.task_id)
        posted: RoomMessage | None = None
        if (
            task is not None
            and task.conversation_id is not None
            and self._coordinator is not None
        ):
            posted = await post_message(
                self._db,
                self._events,
                conversation_id=task.conversation_id,
                role="user",
                sender="CEO",
                text=text,
                quote_id=quote_id,
                task_id=task.id,
                node_id=intervention.node_id,
                intervention_id=intervention_id,
            )
        timer = self._timeout_tasks.pop(intervention_id, None)
        if timer is not None:
            timer.cancel()
        self.start(intervention.task_id)
        return posted

    async def _question_text(self, task_id: str, node_id: str) -> str:
        events = await self._events.replay(task_id)
        for event in reversed(events):
            if (
                event.type is EventType.NODE_STATE_CHANGED
                and event.payload.get("node_id") == node_id
                and event.payload.get("to") == NodeStatus.INPUT_REQUIRED.value
                and event.payload.get("message")
            ):
                return str(event.payload["message"])
        return "The worker agent needs more information to continue."

    async def _assist_text(
        self, task: OrchestrationTask, node: Node, question: str
    ) -> str:
        if self._llm is None:
            raise RuntimeError("assist LLM is not configured")
        user = (
            f"Original request:\n{task.request}\n\n"
            f"Node '{node.name}' asks:\n{question}"
        )
        return await self._llm.text(system=ASSIST_SYSTEM, user=user)

    async def _peer_choice(
        self, task: OrchestrationTask, node: Node, question: str
    ) -> PeerChoice:
        if self._llm is None or self._registry is None:
            raise RuntimeError("peer_agent policy requires llm/registry")
        agents = await self._registry.list()
        options = "\n".join(
            f"- {record.name}: {record.card.get('description', '')}" for record in agents
        )
        choice = await self._llm.structured(
            system=PEER_SYSTEM,
            user=(
                f"Original request:\n{task.request}\n\n"
                f"Worker node '{node.name}' asks:\n{question}\n\n"
                f"Registered agents:\n{options}"
            ),
            schema=PeerChoice,
        )
        record = await self._registry.get_by_name(choice.agent_name)
        if record is None:
            raise ValueError(f"peer agent not registered: {choice.agent_name}")
        return choice

    @staticmethod
    def _deps_completed(node: Node, nodes: list[Node]) -> bool:
        by_id = {item.id: item for item in nodes}
        return all(
            dep in by_id and by_id[dep].status is NodeStatus.COMPLETED
            for dep in node.deps
        )
