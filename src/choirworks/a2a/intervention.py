from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.state import ACTIVE_NODE_STATUSES, NodeState
from choirworks.core.context import build_assistance_decision_user
from choirworks.subagents.assistance import AssistanceSubagent
from choirworks.tools import AskUserFunction
from choirworks.tools.ask_user import AskUserArgs
from choirworks.tools.base import FunctionContext

if TYPE_CHECKING:
    from choirworks.a2a.assist import AssistArbiter
    from choirworks.a2a.events import EventEmitter
    from choirworks.a2a.session import SessionManager

logger = logging.getLogger(__name__)

_AFFIRMATIVE_ANSWERS = {"确认", "确定", "打断", "是", "yes", "y", "ok"}


def _is_affirmative(text: str) -> bool:
    return text.strip().lower() in _AFFIRMATIVE_ANSWERS


class InterventionManager:
    """Human-in-the-loop: settle input, request human, answer intervention."""

    def __init__(
        self,
        emitter: EventEmitter,
        session_mgr: SessionManager,
        assistance_subagent: AssistanceSubagent,
        ask_user_func: AskUserFunction,
        assist_arbiter: AssistArbiter,
        remote_caller: object,
        config: object,
    ):
        self._emitter = emitter
        self._session_mgr = session_mgr
        self._assistance_subagent = assistance_subagent
        self._ask_user_func = ask_user_func
        self._assist_arbiter = assist_arbiter
        self._remote_caller = remote_caller
        self._config = config

    async def settle_input(self, ctx: OrchestrationContext) -> bool:
        state = ctx.state
        progress = False
        for node in list(state.input_required_nodes()):
            intervention = state.pending_intervention_for(node.id)
            if intervention is not None:
                continue
            helpers = [
                n
                for n in state.nodes.values()
                if n.derived
                and n.assist_requested_by == node.id
                and n.status == "completed"
            ]
            if helpers:
                helper = helpers[0]
                intervention = state.pending_intervention_for(node.id)
                if intervention is None:
                    intervention = state.add_intervention(node.id, node.question or "")
                intervention.status = "resolved"
                intervention.answer = helper.output
                intervention.responder = helper.id
                node.answer_text = helper.output
                node.status = "ready"
                await self._emitter.emit_state_delta(ctx,
                    nodes={node.id: {"status": "ready"}},
                    interventions={
                        intervention.id: {
                            "status": "resolved",
                            "node_id": node.id,
                            "kind": intervention.kind,
                        },
                    },
                )
                progress = True
                continue
            active_helpers = [
                n
                for n in state.nodes.values()
                if n.derived
                and n.assist_requested_by == node.id
                and n.status in ACTIVE_NODE_STATUSES | {"pending", "ready"}
            ]
            if active_helpers:
                continue

            decision = await self._decide_assistance(ctx, node)
            if decision is not None and decision.target_agent:
                if await self._assist_arbiter.spawn_assist(ctx, node, decision):
                    progress = True
                else:
                    await self._request_human(ctx, node)
            else:
                await self._request_human(ctx, node)
        return progress

    async def _decide_assistance(
        self, ctx: OrchestrationContext, node: NodeState
    ) -> object | None:
        if node.question is None:
            return None
        agents = await ctx.registry.list()
        candidates = [agent for agent in agents if agent.name != node.agent_name]
        if not candidates:
            from choirworks.tools.outcome_decision import OutcomeDecision
            return OutcomeDecision(intent="need_info")
        user = build_assistance_decision_user(
            node.agent_name,
            node.question or node.input_text,
            candidates,
        )
        try:
            return await self._assistance_subagent.run(
                ctx, user, exclude_agent=node.agent_name
            )
        except Exception:
            logger.exception("assistance decision failed for %s", node.id)
            from choirworks.tools.outcome_decision import OutcomeDecision
            return OutcomeDecision(intent="need_info")

    async def request_human(
        self, ctx: OrchestrationContext, node: NodeState
    ) -> None:
        args = AskUserArgs(node_id=node.id, question=node.question or node.output or "")
        func_ctx = FunctionContext(executor=ctx.executor, runtime=ctx.runtime)  # type: ignore[arg-type]
        result = await self._ask_user_func.execute(func_ctx, args)
        await self._emitter.emit_function_call(
            ctx, self._ask_user_func, args, result,
            state_name=4,  # TASK_STATE_INPUT_REQUIRED
        )

    async def answer_intervention(
        self,
        ctx: OrchestrationContext,
        text: str,
    ) -> None:
        state = ctx.state
        pending = state.pending_interventions()
        if not pending or not text:
            return
        intervention = pending[0]
        intervention.status = "resolved"
        intervention.answer = text
        intervention.responder = "human"
        if intervention.kind == "confirm_cancel":
            target = state.nodes.get(intervention.target_node_id or "")
            if target is not None and _is_affirmative(text):
                await self.cancel_node(ctx, target)
            await self._emitter.emit_state_delta(ctx, interventions={
                intervention.id: {
                    "status": "resolved",
                    "node_id": intervention.node_id,
                    "kind": "confirm_cancel",
                },
            })
            await self._session_mgr.persist(ctx)
            ctx.runtime.runner_start_requested = True
            return
        node = state.nodes.get(intervention.node_id)
        if node is not None:
            node.answer_text = text
            node.status = "ready"
        await self._emitter.emit_state_delta(ctx,
            nodes={node.id: {"status": "ready"}} if node else None,
            interventions={
                intervention.id: {
                    "status": "resolved",
                    "node_id": intervention.node_id,
                    "kind": intervention.kind,
                },
            },
        )
        ctx.runtime.runner_start_requested = True

    async def cancel_node(
        self,
        ctx: OrchestrationContext,
        node: NodeState,
    ) -> None:
        state = ctx.state
        if node.a2a_task_id and node.agent_url:
            await self._remote_caller.cancel_remote_task(
                node.agent_url, node.a2a_task_id
            )
        node.status = "canceled"
        node.a2a_task_id = None
        invalidated = state.blocked_nodes()
        for blocked in invalidated:
            blocked.status = "invalidated"
        await self._emitter.emit_state_delta(ctx, nodes={
            node.id: {"status": "canceled", "agent_name": node.agent_name},
            **{b.id: {"status": "invalidated"} for b in invalidated},
        })
        await self._session_mgr.persist(ctx)
